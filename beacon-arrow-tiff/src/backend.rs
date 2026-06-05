use std::sync::Arc;

use async_tiff::reader::AsyncFileReader;
use async_tiff::tags::Predictor;
use async_tiff::{ImageFileDirectory, TypedArray, decoder::DecoderRegistry};
use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::NdArrayType,
};

use crate::reader::ObjectStoreAsyncReader;

/// Which band to extract from a decoded tile, and how samples are laid out.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BandConfig {
    pub(crate) band_index: usize,
    /// Total number of bands (samples per pixel) in the tile.
    pub(crate) n_bands: usize,
    /// True when the TIFF uses planar (separated) sample layout.
    pub(crate) planar: bool,
}

/// Extract typed band data from a decoded tile array.
pub(crate) trait BandExtract: Sized {
    fn extract_band(
        data: &TypedArray,
        cfg: BandConfig,
        tile_height: usize,
        tile_width: usize,
    ) -> anyhow::Result<Vec<Self>>;
}

macro_rules! impl_band_extract {
    ($T:ty, $variant:ident) => {
        impl BandExtract for $T {
            fn extract_band(
                data: &TypedArray,
                cfg: BandConfig,
                tile_height: usize,
                tile_width: usize,
            ) -> anyhow::Result<Vec<$T>> {
                let data = match data {
                    TypedArray::$variant(d) => d.as_slice(),
                    _ => anyhow::bail!(
                        "unexpected tile data type; expected {}",
                        stringify!($variant)
                    ),
                };
                let mut out = vec![<$T>::default(); tile_height * tile_width];
                if !cfg.planar {
                    for row in 0..tile_height {
                        for col in 0..tile_width {
                            out[row * tile_width + col] = data[row * tile_width * cfg.n_bands
                                + col * cfg.n_bands
                                + cfg.band_index]
                                .clone();
                        }
                    }
                } else {
                    let start = cfg.band_index * tile_height * tile_width;
                    for i in 0..tile_height * tile_width {
                        out[i] = data[start + i].clone();
                    }
                }
                Ok(out)
            }
        }
    };
}

impl_band_extract!(f32, Float32);
impl_band_extract!(f64, Float64);
impl_band_extract!(i8, Int8);
impl_band_extract!(i16, Int16);
impl_band_extract!(i32, Int32);
impl_band_extract!(i64, Int64);
impl_band_extract!(u8, UInt8);
impl_band_extract!(u16, UInt16);
impl_band_extract!(u32, UInt32);
impl_band_extract!(u64, UInt64);

/// Lazy tile-reading backend for a single band of a tiled GeoTIFF.
///
/// No data is fetched until [`read_subset`] is called. Each call fetches
/// only the tiles that intersect the requested region.
pub(crate) struct TiffTileBackend<T: NdArrayType> {
    pub(crate) reader: ObjectStoreAsyncReader,
    pub(crate) ifd: Arc<ImageFileDirectory>,
    pub(crate) image_width: usize,
    pub(crate) image_height: usize,
    pub(crate) tile_width: usize,
    pub(crate) tile_height: usize,
    pub(crate) band_config: BandConfig,
    pub(crate) fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for TiffTileBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffTileBackend")
            .field("image_width", &self.image_width)
            .field("image_height", &self.image_height)
            .field("tile_width", &self.tile_width)
            .field("tile_height", &self.tile_height)
            .field("band_config", &self.band_config)
            .finish()
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + Default + BandExtract> ArrayBackend<T> for TiffTileBackend<T> {
    fn len(&self) -> usize {
        self.image_width * self.image_height
    }

    fn shape(&self) -> Vec<usize> {
        vec![self.image_height, self.image_width]
    }

    fn chunk_shape(&self) -> Vec<usize> {
        vec![self.tile_height, self.tile_width]
    }

    fn dimensions(&self) -> Vec<String> {
        vec!["y".to_string(), "x".to_string()]
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        self.validate_subset(&subset)?;

        let row_start = subset.start[0];
        let col_start = subset.start[1];
        let n_rows = subset.shape[0];
        let n_cols = subset.shape[1];

        // Fast path: the subset is exactly one complete tile at a tile boundary.
        // Skip coordinate calculation and the copy loop entirely.
        if n_rows == self.tile_height
            && n_cols == self.tile_width
            && row_start.is_multiple_of(self.tile_height)
            && col_start.is_multiple_of(self.tile_width)
        {
            let ty = row_start / self.tile_height;
            let tx = col_start / self.tile_width;
            let registry = DecoderRegistry::default();
            let mut tiles = self
                .ifd
                .fetch_tiles(&[(tx, ty)], &self.reader)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to fetch TIFF tile ({tx},{ty}): {e}"))?;
            let decoded = tiles
                .remove(0)
                .decode(&registry)
                .map_err(|e| anyhow::anyhow!("Failed to decode tile ({tx},{ty}): {e}"))?;
            let result = T::extract_band(
                decoded.data(),
                self.band_config,
                self.tile_height,
                self.tile_width,
            )?;
            return ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[n_rows, n_cols]), result)
                .map_err(|e| anyhow::anyhow!("Failed to build result array: {e}"));
        }

        // General path: collect all tiles that overlap the subset.
        let ty_min = row_start / self.tile_height;
        let ty_max = row_start.saturating_add(n_rows).saturating_sub(1) / self.tile_height;
        let tx_min = col_start / self.tile_width;
        let tx_max = col_start.saturating_add(n_cols).saturating_sub(1) / self.tile_width;

        let coords: Vec<(usize, usize)> = (ty_min..=ty_max)
            .flat_map(|ty| (tx_min..=tx_max).map(move |tx| (tx, ty)))
            .collect();

        let tiles = self
            .ifd
            .fetch_tiles(&coords, &self.reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch TIFF tiles: {e}"))?;

        let registry = DecoderRegistry::default();
        let mut result = vec![T::default(); n_rows * n_cols];

        for (tile, (tx, ty)) in tiles.into_iter().zip(coords.into_iter()) {
            let decoded = tile
                .decode(&registry)
                .map_err(|e| anyhow::anyhow!("Failed to decode tile ({tx},{ty}): {e}"))?;

            let tile_data = T::extract_band(
                decoded.data(),
                self.band_config,
                self.tile_height,
                self.tile_width,
            )?;

            // Copy the intersection of the tile footprint and the requested subset.
            let img_row_start = row_start.max(ty * self.tile_height);
            let img_row_end = (row_start + n_rows)
                .min((ty + 1) * self.tile_height)
                .min(self.image_height);
            let img_col_start = col_start.max(tx * self.tile_width);
            let img_col_end = (col_start + n_cols)
                .min((tx + 1) * self.tile_width)
                .min(self.image_width);

            for img_row in img_row_start..img_row_end {
                for img_col in img_col_start..img_col_end {
                    let src = (img_row - ty * self.tile_height) * self.tile_width
                        + (img_col - tx * self.tile_width);
                    let dst = (img_row - row_start) * n_cols + (img_col - col_start);
                    result[dst] = tile_data[src].clone();
                }
            }
        }

        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[n_rows, n_cols]), result)
            .map_err(|e| anyhow::anyhow!("Failed to build result array: {e}"))
    }
}

/// Decode a little-endian byte buffer into a typed sample vector.
///
/// The tiled path goes through `async-tiff`'s `DecodedTile`/`TypedArray`, but the
/// stripped path decompresses each strip to raw bytes (`decode_tile` returns
/// `Vec<u8>`), so it must reinterpret those bytes itself. Keeping this as a trait
/// lets `TiffStripBackend<T>` stay generic over the sample type.
pub(crate) trait StripDecode: Sized {
    fn decode_le(bytes: &[u8]) -> Vec<Self>;
}

macro_rules! impl_strip_decode {
    ($T:ty, $N:expr) => {
        impl StripDecode for $T {
            fn decode_le(bytes: &[u8]) -> Vec<$T> {
                bytes
                    .chunks_exact($N)
                    .map(|chunk| {
                        let arr: [u8; $N] = chunk.try_into().unwrap();
                        <$T>::from_le_bytes(arr)
                    })
                    .collect()
            }
        }
    };
}

impl_strip_decode!(u8, 1);
impl_strip_decode!(u16, 2);
impl_strip_decode!(u32, 4);
impl_strip_decode!(u64, 8);
impl_strip_decode!(i8, 1);
impl_strip_decode!(i16, 2);
impl_strip_decode!(i32, 4);
impl_strip_decode!(i64, 8);
impl_strip_decode!(f32, 4);
impl_strip_decode!(f64, 8);

/// Lazy strip-reading backend for a single band of a stripped TIFF.
///
/// A stripped TIFF stores the image as full-width horizontal strips stacked along
/// the row axis, so chunking is 1-D in the row dimension. No data is fetched until
/// [`read_subset`] is called; each call fetches only the strips that intersect the
/// requested rows and decompresses them on demand.
///
/// Only `Predictor::None` is supported (matching the eager reader); predictors are
/// rejected at read time rather than emitting silently wrong pixels.
pub(crate) struct TiffStripBackend<T: NdArrayType> {
    pub(crate) reader: ObjectStoreAsyncReader,
    pub(crate) ifd: Arc<ImageFileDirectory>,
    pub(crate) image_width: usize,
    pub(crate) image_height: usize,
    pub(crate) rows_per_strip: usize,
    pub(crate) band_config: BandConfig,
    pub(crate) fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for TiffStripBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffStripBackend")
            .field("image_width", &self.image_width)
            .field("image_height", &self.image_height)
            .field("rows_per_strip", &self.rows_per_strip)
            .field("band_config", &self.band_config)
            .finish()
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + Default + StripDecode> ArrayBackend<T> for TiffStripBackend<T> {
    fn len(&self) -> usize {
        self.image_width * self.image_height
    }

    fn shape(&self) -> Vec<usize> {
        vec![self.image_height, self.image_width]
    }

    fn chunk_shape(&self) -> Vec<usize> {
        vec![self.rows_per_strip, self.image_width]
    }

    fn dimensions(&self) -> Vec<String> {
        vec!["y".to_string(), "x".to_string()]
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        self.validate_subset(&subset)?;

        let row_start = subset.start[0];
        let col_start = subset.start[1];
        let n_rows = subset.shape[0];
        let n_cols = subset.shape[1];

        let mut result = vec![T::default(); n_rows * n_cols];
        if n_rows == 0 || n_cols == 0 {
            return ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[n_rows, n_cols]), result)
                .map_err(|e| anyhow::anyhow!("Failed to build result array: {e}"));
        }

        // A predictor is a reversible transform applied before compression. We don't
        // reverse it, so reject anything other than "no predictor" rather than emit
        // silently wrong pixels (matches the eager reader).
        let predictor = self.ifd.predictor().unwrap_or(Predictor::None);
        anyhow::ensure!(
            predictor == Predictor::None,
            "Unsupported predictor {predictor:?} for stripped TIFF (only Predictor::None is supported)"
        );

        let strip_offsets = self
            .ifd
            .strip_offsets()
            .ok_or_else(|| anyhow::anyhow!("IFD has no strip offsets"))?;
        let strip_byte_counts = self
            .ifd
            .strip_byte_counts()
            .ok_or_else(|| anyhow::anyhow!("IFD has no strip byte counts"))?;

        let n_bands = self.band_config.n_bands;
        let bits_per_sample = self.ifd.bits_per_sample().first().copied().unwrap_or(8);
        let compression = self.ifd.compression();
        let registry = DecoderRegistry::default();
        let decoder = registry.as_ref().get(&compression).ok_or_else(|| {
            anyhow::anyhow!("Unsupported TIFF compression {compression:?} for stripped layout")
        })?;
        let photometric = self.ifd.photometric_interpretation();
        let jpeg_tables = self.ifd.jpeg_tables();

        let strips_per_plane = self.image_height.div_ceil(self.rows_per_strip);
        let row_end = row_start + n_rows;
        let col_end = col_start + n_cols;

        // Strips (by row position) overlapping the requested rows.
        let k_min = row_start / self.rows_per_strip;
        let k_max = (row_end - 1) / self.rows_per_strip;

        for k in k_min..=k_max {
            let strip_first_row = k * self.rows_per_strip;
            let strip_rows = self.rows_per_strip.min(self.image_height - strip_first_row);

            // Planar strips are grouped per plane (band); chunky strips interleave the
            // bands within each strip and share one strip index across bands.
            let strip_index = if self.band_config.planar {
                self.band_config.band_index * strips_per_plane + k
            } else {
                k
            };

            let offset = *strip_offsets
                .get(strip_index)
                .ok_or_else(|| anyhow::anyhow!("strip index {strip_index} out of range"))?;
            let count = strip_byte_counts[strip_index];

            let bytes = self
                .reader
                .get_bytes(offset..offset + count)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read strip {strip_index}: {e}"))?;
            let decoded = decoder
                .decode_tile(
                    bytes,
                    photometric,
                    jpeg_tables,
                    n_bands as u16,
                    bits_per_sample,
                    None,
                )
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to decompress strip {strip_index} ({compression:?}): {e}"
                    )
                })?;
            let values: Vec<T> = T::decode_le(&decoded);

            // Copy the intersection of this strip's rows with the requested window.
            let img_row_lo = strip_first_row.max(row_start);
            let img_row_hi = (strip_first_row + strip_rows).min(row_end);

            for img_row in img_row_lo..img_row_hi {
                let within = img_row - strip_first_row;
                for img_col in col_start..col_end {
                    // Chunky: samples interleaved as px0_b0 px0_b1 … (per strip-local pixel).
                    // Planar: the strip holds a single band, row-major.
                    let src = if self.band_config.planar {
                        within * self.image_width + img_col
                    } else {
                        (within * self.image_width + img_col) * n_bands
                            + self.band_config.band_index
                    };
                    let dst = (img_row - row_start) * n_cols + (img_col - col_start);
                    result[dst] = values[src].clone();
                }
            }
        }

        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[n_rows, n_cols]), result)
            .map_err(|e| anyhow::anyhow!("Failed to build result array: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunky_f32(rows: usize, cols: usize, n_bands: usize, values: Vec<f32>) -> TypedArray {
        assert_eq!(values.len(), rows * cols * n_bands);
        TypedArray::Float32(values)
    }

    fn planar_i16(rows: usize, cols: usize, n_bands: usize, values: Vec<i16>) -> TypedArray {
        assert_eq!(values.len(), rows * cols * n_bands);
        TypedArray::Int16(values)
    }

    #[test]
    fn extract_chunky_single_band() {
        // 2×2 tile, 1 band — data is already the band itself.
        let data = TypedArray::Float32(vec![1.0, 2.0, 3.0, 4.0]);
        let cfg = BandConfig {
            band_index: 0,
            n_bands: 1,
            planar: false,
        };
        let out = f32::extract_band(&data, cfg, 2, 2).unwrap();
        assert_eq!(out, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn extract_chunky_multi_band_band0() {
        // 2×2 tile, 2 bands, chunky: [b0,b1] per pixel, row-major.
        // Pixels: (r0,c0)=[1,10], (r0,c1)=[2,20], (r1,c0)=[3,30], (r1,c1)=[4,40]
        let data = chunky_f32(2, 2, 2, vec![1.0, 10.0, 2.0, 20.0, 3.0, 30.0, 4.0, 40.0]);
        let cfg = BandConfig {
            band_index: 0,
            n_bands: 2,
            planar: false,
        };
        let out = f32::extract_band(&data, cfg, 2, 2).unwrap();
        assert_eq!(out, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn extract_chunky_multi_band_band1() {
        let data = chunky_f32(2, 2, 2, vec![1.0, 10.0, 2.0, 20.0, 3.0, 30.0, 4.0, 40.0]);
        let cfg = BandConfig {
            band_index: 1,
            n_bands: 2,
            planar: false,
        };
        let out = f32::extract_band(&data, cfg, 2, 2).unwrap();
        assert_eq!(out, vec![10.0, 20.0, 30.0, 40.0]);
    }

    #[test]
    fn extract_planar_band0() {
        // 2×2 tile, 2 bands, planar: all of band 0 first, then band 1.
        let data = chunky_f32(2, 2, 2, vec![1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0]);
        let cfg = BandConfig {
            band_index: 0,
            n_bands: 2,
            planar: true,
        };
        let out = f32::extract_band(&data, cfg, 2, 2).unwrap();
        assert_eq!(out, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn extract_planar_band1() {
        let data = chunky_f32(2, 2, 2, vec![1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0]);
        let cfg = BandConfig {
            band_index: 1,
            n_bands: 2,
            planar: true,
        };
        let out = f32::extract_band(&data, cfg, 2, 2).unwrap();
        assert_eq!(out, vec![10.0, 20.0, 30.0, 40.0]);
    }

    #[test]
    fn extract_i16_chunky() {
        let data = planar_i16(1, 3, 1, vec![100, 200, 300]);
        let cfg = BandConfig {
            band_index: 0,
            n_bands: 1,
            planar: false,
        };
        let out = i16::extract_band(&data, cfg, 1, 3).unwrap();
        assert_eq!(out, vec![100i16, 200, 300]);
    }

    #[test]
    fn extract_wrong_type_returns_error() {
        // f64 extraction from a Float32 array must fail.
        let data = TypedArray::Float32(vec![1.0, 2.0]);
        let cfg = BandConfig {
            band_index: 0,
            n_bands: 1,
            planar: false,
        };
        assert!(f64::extract_band(&data, cfg, 1, 2).is_err());
    }

    #[test]
    fn band_config_is_copy() {
        let cfg = BandConfig {
            band_index: 2,
            n_bands: 4,
            planar: true,
        };
        let cfg2 = cfg; // Copy
        assert_eq!(cfg.band_index, cfg2.band_index);
    }
}
