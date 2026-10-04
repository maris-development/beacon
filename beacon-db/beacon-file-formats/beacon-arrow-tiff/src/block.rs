//! Block access for one TIFF image.
//!
//! A block is a tile of a tiled TIFF or a strip of a stripped TIFF. A strip is
//! a block that is as wide as the image. [`BlockLayout`] holds the geometry and
//! the decode settings, and [`TiffImage`] fetches, decodes and caches blocks.

use std::{ops::Range, sync::Arc};

use async_tiff::ImageFileDirectory;
use async_tiff::decoder::{Decoder, DecoderRegistry};
use async_tiff::reader::{AsyncFileReader, Endianness};
use async_tiff::tags::{
    Compression, PhotometricInterpretation, PlanarConfiguration, Predictor, SampleFormat,
};
use beacon_nd_array::datatypes::NdArrayType;
use bytes::Bytes;

use crate::reader::ObjectStoreAsyncReader;

/// The approximate pixel count of one read chunk of a stripped image.
const TARGET_STRIP_CHUNK_PIXELS: usize = 256 * 256;

/// The minimum memory budget for the decoded blocks of one image.
const MIN_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// The decoders that Beacon uses: the `async-tiff` defaults plus PackBits.
fn decoder_registry() -> DecoderRegistry {
    let mut registry = DecoderRegistry::default();
    registry
        .as_mut()
        .insert(Compression::PackBits, Box::new(PackBitsDecoder));
    registry
}

/// Decoder for PackBits (compression 32773), the run-length code of baseline TIFF.
#[derive(Debug)]
struct PackBitsDecoder;

impl Decoder for PackBitsDecoder {
    fn decode_tile(
        &self,
        buffer: Bytes,
        _photometric_interpretation: PhotometricInterpretation,
        _jpeg_tables: Option<&[u8]>,
        _samples_per_pixel: u16,
        _bits_per_sample: u16,
        _lerc_parameters: Option<&[u32]>,
    ) -> async_tiff::error::AsyncTiffResult<Vec<u8>> {
        unpack_bits(&buffer).ok_or_else(|| {
            async_tiff::error::AsyncTiffError::General("truncated PackBits data".to_string())
        })
    }
}

/// Expand PackBits data. Returns `None` when a run goes past the end of `data`.
fn unpack_bits(data: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() * 2);
    let mut i = 0;
    while i < data.len() {
        let header = data[i] as i8;
        i += 1;
        match header {
            // A literal run of `header + 1` bytes.
            0.. => {
                let end = i + header as usize + 1;
                out.extend_from_slice(data.get(i..end)?);
                i = end;
            }
            // -128 is a no-op.
            -128 => {}
            // One byte, repeated `1 - header` times.
            _ => {
                let byte = *data.get(i)?;
                out.extend(std::iter::repeat_n(byte, (1 - header as isize) as usize));
                i += 1;
            }
        }
    }
    Some(out)
}

/// A pixel sample type that a TIFF block can hold.
pub(crate) trait TiffSample: NdArrayType + Copy + Default {
    /// The size of one sample in bytes.
    const BYTES: usize;

    /// Read one sample from native-endian bytes.
    fn from_ne_slice(bytes: &[u8]) -> Self;

    /// Parse a `GDAL_NODATA` value. Returns `None` when the type cannot hold it.
    fn parse_nodata(text: &str) -> Option<Self>;
}

macro_rules! impl_int_sample {
    ($($T:ty),*) => {$(
        impl TiffSample for $T {
            const BYTES: usize = std::mem::size_of::<$T>();

            fn from_ne_slice(bytes: &[u8]) -> Self {
                <$T>::from_ne_bytes(bytes.try_into().expect("slice has the sample size"))
            }

            fn parse_nodata(text: &str) -> Option<Self> {
                if let Ok(value) = text.parse::<$T>() {
                    return Some(value);
                }
                // GDAL writes some integer nodata values as floats, such as "255.0".
                let value = text.parse::<f64>().ok()?;
                let exact = value.fract() == 0.0
                    && value >= <$T>::MIN as f64
                    && value <= <$T>::MAX as f64;
                exact.then_some(value as $T)
            }
        }
    )*};
}

macro_rules! impl_float_sample {
    ($($T:ty),*) => {$(
        impl TiffSample for $T {
            const BYTES: usize = std::mem::size_of::<$T>();

            fn from_ne_slice(bytes: &[u8]) -> Self {
                <$T>::from_ne_bytes(bytes.try_into().expect("slice has the sample size"))
            }

            fn parse_nodata(text: &str) -> Option<Self> {
                text.parse::<f64>().ok().map(|value| value as $T)
            }
        }
    )*};
}

impl_int_sample!(i8, i16, i32, i64, u8, u16, u32, u64);
impl_float_sample!(f32, f64);

/// The geometry and the decode settings of the blocks of one TIFF image.
#[derive(Debug)]
pub(crate) struct BlockLayout {
    pub(crate) image_width: usize,
    pub(crate) image_height: usize,
    pub(crate) block_width: usize,
    pub(crate) block_height: usize,
    pub(crate) blocks_across: usize,
    pub(crate) blocks_down: usize,
    pub(crate) tiled: bool,
    pub(crate) n_bands: usize,
    pub(crate) planar: bool,
    pub(crate) sample_format: SampleFormat,
    pub(crate) bits_per_sample: u16,
    offsets: Vec<u64>,
    byte_counts: Vec<u64>,
    compression: Compression,
    photometric: PhotometricInterpretation,
    jpeg_tables: Option<Vec<u8>>,
    lerc_parameters: Option<Vec<u32>>,
    predictor: Predictor,
    endianness: Endianness,
}

impl BlockLayout {
    /// Read and check the block layout of `ifd`.
    pub(crate) fn from_ifd(
        ifd: &ImageFileDirectory,
        endianness: Endianness,
    ) -> anyhow::Result<Self> {
        let image_width = ifd.image_width() as usize;
        let image_height = ifd.image_height() as usize;
        anyhow::ensure!(
            image_width > 0 && image_height > 0,
            "TIFF image has an empty size ({image_width}x{image_height})"
        );

        let n_bands = ifd.samples_per_pixel() as usize;
        anyhow::ensure!(n_bands > 0, "TIFF image has 0 samples per pixel");
        let planar = n_bands > 1 && ifd.planar_configuration() == PlanarConfiguration::Planar;

        let bits_per_sample = single_value(ifd.bits_per_sample(), "BitsPerSample")?.unwrap_or(1);
        anyhow::ensure!(
            matches!(bits_per_sample, 8 | 16 | 32 | 64),
            "Unsupported TIFF sample size: {bits_per_sample} bits per sample"
        );
        let sample_format =
            single_value(ifd.sample_format(), "SampleFormat")?.unwrap_or(SampleFormat::Uint);

        let (block_width, block_height, offsets, byte_counts, tiled) =
            match (ifd.tile_width(), ifd.tile_height()) {
                (Some(width), Some(height)) => (
                    width as usize,
                    height as usize,
                    ifd.tile_offsets(),
                    ifd.tile_byte_counts(),
                    true,
                ),
                (None, None) => {
                    // RowsPerStrip defaults to 2^32 - 1, so one strip holds the whole image.
                    let rows = ifd.rows_per_strip().unwrap_or(u32::MAX) as usize;
                    (
                        image_width,
                        rows.min(image_height),
                        ifd.strip_offsets(),
                        ifd.strip_byte_counts(),
                        false,
                    )
                }
                _ => anyhow::bail!("TIFF image has only one of TileWidth and TileLength"),
            };
        let kind = if tiled { "tile" } else { "strip" };
        anyhow::ensure!(
            block_width > 0 && block_height > 0,
            "TIFF {kind} size is empty ({block_width}x{block_height})"
        );

        let offsets = offsets
            .ok_or_else(|| anyhow::anyhow!("TIFF image has no {kind} offsets"))?
            .to_vec();
        let byte_counts = byte_counts
            .ok_or_else(|| anyhow::anyhow!("TIFF image has no {kind} byte counts"))?
            .to_vec();

        let blocks_across = image_width.div_ceil(block_width);
        let blocks_down = image_height.div_ceil(block_height);
        let planes = if planar { n_bands } else { 1 };
        let expected = blocks_across * blocks_down * planes;
        anyhow::ensure!(
            offsets.len() >= expected && byte_counts.len() >= expected,
            "TIFF image needs {expected} {kind} entries, but has {} offsets and {} byte counts",
            offsets.len(),
            byte_counts.len(),
        );

        let predictor = ifd.predictor().unwrap_or(Predictor::None);
        match predictor {
            Predictor::None | Predictor::Horizontal => {}
            Predictor::FloatingPoint => anyhow::ensure!(
                sample_format == SampleFormat::Float && matches!(bits_per_sample, 32 | 64),
                "TIFF floating point predictor needs 32 or 64 bit float samples"
            ),
            other => anyhow::bail!("Unsupported TIFF predictor {other:?}"),
        }

        let compression = ifd.compression();
        anyhow::ensure!(
            decoder_registry().as_ref().contains_key(&compression),
            "Unsupported TIFF compression {compression:?}"
        );

        Ok(Self {
            image_width,
            image_height,
            block_width,
            block_height,
            blocks_across,
            blocks_down,
            tiled,
            n_bands,
            planar,
            sample_format,
            bits_per_sample,
            offsets,
            byte_counts,
            compression,
            photometric: ifd.photometric_interpretation(),
            jpeg_tables: ifd.jpeg_tables().map(<[u8]>::to_vec),
            lerc_parameters: ifd.lerc_parameters().map(<[u32]>::to_vec),
            predictor,
            endianness,
        })
    }

    /// The word for one block in messages: "tile" or "strip".
    pub(crate) fn kind(&self) -> &'static str {
        if self.tiled { "tile" } else { "strip" }
    }

    /// The chunk shape `[rows, columns]` that a scan reads in one step.
    pub(crate) fn chunk_shape(&self) -> Vec<usize> {
        if self.tiled {
            return vec![self.block_height, self.block_width];
        }
        // Strips are often a few rows high. Group them to get chunks of a useful size.
        let strip_pixels = self.block_height * self.image_width;
        let strips = (TARGET_STRIP_CHUNK_PIXELS / strip_pixels).max(1);
        let rows = (strips * self.block_height).min(self.image_height);
        vec![rows, self.image_width]
    }

    /// The number of samples per pixel in one decoded block.
    fn block_samples(&self) -> usize {
        if self.planar { 1 } else { self.n_bands }
    }

    /// The byte count of one decoded block.
    pub(crate) fn decoded_block_bytes(&self) -> u64 {
        let bytes_per_sample = self.bits_per_sample as u64 / 8;
        (self.block_width * self.block_height * self.block_samples()) as u64 * bytes_per_sample
    }

    /// The offset table index of the block of `band` at column `bx` and row `by`.
    fn block_index(&self, band: usize, bx: usize, by: usize) -> usize {
        let index = by * self.blocks_across + bx;
        if self.planar {
            band * self.blocks_across * self.blocks_down + index
        } else {
            index
        }
    }

    /// The number of image rows in block row `by`. The last strip can be short.
    fn block_rows(&self, by: usize) -> usize {
        if self.tiled {
            self.block_height
        } else {
            self.block_height
                .min(self.image_height - by * self.block_height)
        }
    }

    /// The file byte range of block `index`, or `None` for a sparse block.
    ///
    /// GDAL writes an offset and a byte count of 0 for a block that has no data.
    fn byte_range(&self, index: usize) -> anyhow::Result<Option<Range<u64>>> {
        let offset = self.offsets[index];
        let count = self.byte_counts[index];
        if offset == 0 || count == 0 {
            return Ok(None);
        }
        let end = offset.checked_add(count).ok_or_else(|| {
            anyhow::anyhow!("TIFF {} {index} has an invalid byte range", self.kind())
        })?;
        Ok(Some(offset..end))
    }

    /// Decompress one block and undo the predictor.
    ///
    /// Returns the samples of the first `rows` rows as native-endian bytes.
    fn decode_block(
        &self,
        decoder: &dyn Decoder,
        raw: Bytes,
        rows: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let samples = self.block_samples();
        let mut data = decoder
            .decode_tile(
                raw,
                self.photometric,
                self.jpeg_tables.as_deref(),
                samples as u16,
                self.bits_per_sample,
                self.lerc_parameters.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("{:?} decode failed: {e}", self.compression))?;

        let bytes_per_sample = self.bits_per_sample as usize / 8;
        let row_bytes = self.block_width * samples * bytes_per_sample;
        let needed = rows * row_bytes;
        anyhow::ensure!(
            data.len() >= needed,
            "decoded {} bytes, but {rows} rows need {needed} bytes",
            data.len()
        );
        data.truncate(needed);

        match self.predictor {
            Predictor::FloatingPoint => {
                for row in data.chunks_exact_mut(row_bytes) {
                    undo_float_predictor(row, samples, bytes_per_sample);
                }
            }
            Predictor::Horizontal => {
                to_native_endian(&mut data, self.endianness, bytes_per_sample);
                for row in data.chunks_exact_mut(row_bytes) {
                    undo_horizontal_predictor(row, samples, bytes_per_sample);
                }
            }
            _ => to_native_endian(&mut data, self.endianness, bytes_per_sample),
        }
        Ok(data)
    }
}

/// Return the one value that `values` holds for every sample.
fn single_value<V: Copy + PartialEq + std::fmt::Debug>(
    values: &[V],
    tag: &str,
) -> anyhow::Result<Option<V>> {
    let Some(&first) = values.first() else {
        return Ok(None);
    };
    anyhow::ensure!(
        values.iter().all(|v| *v == first),
        "TIFF bands with different {tag} values are not supported: {values:?}"
    );
    Ok(Some(first))
}

fn to_native_endian(data: &mut [u8], endianness: Endianness, bytes_per_sample: usize) {
    if endianness.is_native() || bytes_per_sample == 1 {
        return;
    }
    for sample in data.chunks_exact_mut(bytes_per_sample) {
        sample.reverse();
    }
}

/// Undo horizontal differencing (Predictor 2) on one row of native-endian samples.
fn undo_horizontal_predictor(row: &mut [u8], samples: usize, bytes_per_sample: usize) {
    macro_rules! accumulate {
        ($T:ty) => {{
            const N: usize = std::mem::size_of::<$T>();
            let stride = samples * N;
            for i in (stride..row.len()).step_by(N) {
                let prev = <$T>::from_ne_bytes(row[i - stride..i - stride + N].try_into().unwrap());
                let value = <$T>::from_ne_bytes(row[i..i + N].try_into().unwrap());
                row[i..i + N].copy_from_slice(&value.wrapping_add(prev).to_ne_bytes());
            }
        }};
    }
    match bytes_per_sample {
        1 => accumulate!(u8),
        2 => accumulate!(u16),
        4 => accumulate!(u32),
        _ => accumulate!(u64),
    }
}

/// Undo the floating point predictor (Predictor 3) on one row.
///
/// The encoder splits each value into its bytes, most significant byte first, and
/// stores all first bytes of the row, then all second bytes, and so on. It then
/// differences that byte sequence. The result is native-endian samples.
fn undo_float_predictor(row: &mut [u8], samples: usize, bytes_per_sample: usize) {
    for i in samples..row.len() {
        row[i] = row[i].wrapping_add(row[i - samples]);
    }
    let values = row.len() / bytes_per_sample;
    let shuffled = row.to_vec();
    for (value, out) in row.chunks_exact_mut(bytes_per_sample).enumerate() {
        for (byte, slot) in out.iter_mut().enumerate() {
            *slot = shuffled[byte * values + value];
        }
        if cfg!(target_endian = "little") {
            out.reverse();
        }
    }
}

/// One TIFF image that all its band arrays share.
///
/// The bands of a pixel-interleaved image share each block, so the cache lets
/// the bands fetch and decode a block one time only.
pub(crate) struct TiffImage<T: TiffSample> {
    reader: ObjectStoreAsyncReader,
    pub(crate) layout: BlockLayout,
    decoders: DecoderRegistry,
    cache: moka::future::Cache<usize, Arc<Vec<T>>>,
}

impl<T: TiffSample> std::fmt::Debug for TiffImage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffImage")
            .field("reader", &self.reader)
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl<T: TiffSample> TiffImage<T> {
    pub(crate) fn new(reader: ObjectStoreAsyncReader, layout: BlockLayout) -> anyhow::Result<Self> {
        anyhow::ensure!(
            T::BYTES * 8 == layout.bits_per_sample as usize,
            "TIFF sample size {} does not match the array type",
            layout.bits_per_sample
        );
        // Keep room for at least two blocks, so that a scan over one large strip
        // does not decode that strip again for each chunk.
        let capacity = MIN_CACHE_BYTES.max(layout.decoded_block_bytes().saturating_mul(2));
        let cache = moka::future::Cache::builder()
            .max_capacity(capacity)
            .weigher(|_, block: &Arc<Vec<T>>| {
                u32::try_from(block.len() * T::BYTES).unwrap_or(u32::MAX)
            })
            .build();
        Ok(Self {
            reader,
            layout,
            decoders: decoder_registry(),
            cache,
        })
    }

    /// The samples of the block of `band` at column `bx` and row `by`.
    ///
    /// The block holds `block_rows(by)` rows of `block_width` pixels. A pixel has
    /// one sample for a planar image, and `n_bands` samples in other cases.
    /// Returns `None` for a sparse block.
    pub(crate) async fn block(
        &self,
        band: usize,
        bx: usize,
        by: usize,
    ) -> anyhow::Result<Option<Arc<Vec<T>>>> {
        let layout = &self.layout;
        let context = || format!("TIFF {} ({bx},{by})", layout.kind());
        let index = layout.block_index(band, bx, by);
        let Some(range) = layout.byte_range(index)? else {
            return Ok(None);
        };
        let rows = layout.block_rows(by);

        let load = async {
            let raw = self
                .reader
                .get_bytes(range.clone())
                .await
                .map_err(|e| anyhow::anyhow!("fetch of bytes {range:?} failed: {e}"))?;
            let decoder = self
                .decoders
                .as_ref()
                .get(&layout.compression)
                .ok_or_else(|| anyhow::anyhow!("no decoder for {:?}", layout.compression))?;
            let bytes = layout.decode_block(decoder.as_ref(), raw, rows)?;
            let samples = bytes.chunks_exact(T::BYTES).map(T::from_ne_slice).collect();
            Ok::<_, anyhow::Error>(Arc::new(samples))
        };
        self.cache
            .try_get_with(index, load)
            .await
            .map(Some)
            .map_err(|e| anyhow::anyhow!("Failed to read {}: {e:#}", context()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpack_bits_expands_the_specification_example() {
        // The example of the TIFF 6.0 specification, section 9.
        let packed = [
            0xFE, 0xAA, 0x02, 0x80, 0x00, 0x2A, 0xFD, 0xAA, 0x03, 0x80, 0x00, 0x2A, 0x22, 0xF7,
            0xAA,
        ];
        let unpacked = [
            0xAA, 0xAA, 0xAA, 0x80, 0x00, 0x2A, 0xAA, 0xAA, 0xAA, 0xAA, 0x80, 0x00, 0x2A, 0x22,
            0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        ];
        assert_eq!(unpack_bits(&packed).unwrap(), unpacked);
        // A -128 header is a no-op.
        assert_eq!(unpack_bits(&[0x80, 0x00, 0x07]).unwrap(), [0x07]);
    }

    #[test]
    fn unpack_bits_rejects_a_truncated_run() {
        assert_eq!(unpack_bits(&[0x03, 0x01, 0x02]), None);
        assert_eq!(unpack_bits(&[0xFE]), None);
    }

    #[test]
    fn integer_nodata_must_fit_the_type() {
        assert_eq!(u8::parse_nodata("255"), Some(255));
        assert_eq!(u8::parse_nodata("255.0"), Some(255));
        assert_eq!(u8::parse_nodata("-9999"), None);
        assert_eq!(u8::parse_nodata("1.5"), None);
        assert_eq!(i16::parse_nodata("-32767"), Some(-32767));
        assert_eq!(u64::parse_nodata("18446744073709551615"), Some(u64::MAX));
        assert_eq!(i32::parse_nodata("nan"), None);
    }

    #[test]
    fn float_nodata_accepts_nan() {
        assert!(f32::parse_nodata("nan").unwrap().is_nan());
        assert_eq!(f64::parse_nodata("-3.4e38"), Some(-3.4e38));
    }

    #[test]
    fn horizontal_predictor_accumulates_per_sample() {
        // Two samples per pixel; each sample channel is summed on its own.
        let mut row: Vec<u8> = [10u16, 100, 1, 5, 2, 5]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        undo_horizontal_predictor(&mut row, 2, 2);
        let values: Vec<u16> = row
            .chunks_exact(2)
            .map(|c| u16::from_ne_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(values, vec![10, 100, 11, 105, 13, 110]);
    }

    #[test]
    fn float_predictor_restores_values() {
        // Encode two f32 values with the predictor, then decode them.
        let values = [1.5f32, -2.25];
        let be: Vec<[u8; 4]> = values.iter().map(|v| v.to_be_bytes()).collect();
        let mut row: Vec<u8> = (0..4).flat_map(|b| be.iter().map(move |v| v[b])).collect();
        for i in (1..row.len()).rev() {
            row[i] = row[i].wrapping_sub(row[i - 1]);
        }
        undo_float_predictor(&mut row, 1, 4);
        let decoded: Vec<f32> = row
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(decoded, values);
    }

    #[test]
    fn big_endian_samples_become_native() {
        let mut data = 0x0102u16.to_be_bytes().to_vec();
        to_native_endian(&mut data, Endianness::BigEndian, 2);
        assert_eq!(u16::from_ne_bytes(data.try_into().unwrap()), 0x0102);
    }
}
