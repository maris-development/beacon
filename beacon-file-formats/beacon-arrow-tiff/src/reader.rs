use std::{ops::Range, sync::Arc};

use async_tiff::ImageFileDirectory;
use async_tiff::decoder::DecoderRegistry;
use async_tiff::metadata::{TiffMetadataReader, cache::ReadaheadMetadataCache};
use async_tiff::reader::AsyncFileReader;
use async_tiff::tags::{PlanarConfiguration, Predictor, SampleFormat};
use beacon_nd_array::{
    NdArray, NdArrayD,
    dataset::{AnyDataset, Dataset},
};

use crate::backend::{BandConfig, TiffTileBackend};
use indexmap::IndexMap;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

#[derive(Clone)]
pub(crate) struct ObjectStoreAsyncReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl std::fmt::Debug for ObjectStoreAsyncReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreAsyncReader")
            .field("path", &self.path)
            .finish()
    }
}

#[async_trait::async_trait]
impl AsyncFileReader for ObjectStoreAsyncReader {
    async fn get_bytes(
        &self,
        range: Range<u64>,
    ) -> async_tiff::error::AsyncTiffResult<bytes::Bytes> {
        self.store.get_range(&self.path, range).await.map_err(|e| {
            async_tiff::error::AsyncTiffError::General(format!(
                "Object store read failed for '{}': {}",
                self.path, e
            ))
        })
    }
}

/// Open a TIFF/GeoTIFF file and return its contents as an AnyDataset.
///
/// Current implementation:
/// - Reads TIFF metadata through `async-tiff`.
/// - Exposes deterministic metadata arrays.
/// - Fails fast for non-tiled TIFF files (v1 policy).
pub async fn open_dataset(
    object_store: Arc<dyn ObjectStore>,
    path: Path,
) -> anyhow::Result<AnyDataset> {
    let dataset_name = path.to_string();
    tracing::debug!(path = %dataset_name, "opening TIFF dataset");
    let reader = ObjectStoreAsyncReader {
        store: object_store,
        path,
    };
    let cached_reader = ReadaheadMetadataCache::new(reader.clone());

    let mut metadata_reader = TiffMetadataReader::try_open(&cached_reader)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open TIFF metadata: {e}"))?;

    let ifds = metadata_reader
        .read_all_ifds(&cached_reader)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read TIFF metadata IFDs: {e}"))?;

    let first_ifd = Arc::new(
        ifds.into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("TIFF contains no image file directories (IFDs)."))?,
    );

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    insert_scalar(&mut arrays, "image.width", first_ifd.image_width())?;
    insert_scalar(&mut arrays, "image.height", first_ifd.image_height())?;
    insert_scalar(
        &mut arrays,
        "image.samples_per_pixel",
        first_ifd.samples_per_pixel(),
    )?;

    if let Some(bits_per_sample) = first_ifd.bits_per_sample().first() {
        insert_scalar(&mut arrays, "image.bits_per_sample", *bits_per_sample)?;
    }

    if let Some((tiles_x, tiles_y)) = first_ifd.tile_count() {
        let tile_width = first_ifd.tile_width().ok_or_else(|| {
            anyhow::anyhow!("tiled TIFF reports a tile count but is missing TileWidth")
        })?;
        let tile_height = first_ifd.tile_height().ok_or_else(|| {
            anyhow::anyhow!("tiled TIFF reports a tile count but is missing TileLength")
        })?;
        insert_scalar(&mut arrays, "image.tile_width", tile_width)?;
        insert_scalar(&mut arrays, "image.tile_height", tile_height)?;
        insert_scalar(&mut arrays, "image.tile_count_x", tiles_x as u64)?;
        insert_scalar(&mut arrays, "image.tile_count_y", tiles_y as u64)?;
    }

    if let Some(geo_keys) = first_ifd.geo_key_directory()
        && let Some(epsg) = geo_keys.epsg_code()
    {
        insert_scalar(&mut arrays, "geo.epsg", epsg)?;
        insert_scalar(&mut arrays, "geo.crs", format!("EPSG:{epsg}"))?;
    }

    if let Some(model_pixel_scale) = first_ifd.model_pixel_scale() {
        insert_scalar(
            &mut arrays,
            "geo.model_pixel_scale",
            format_f64_list(model_pixel_scale),
        )?;
    }

    if let Some(model_tiepoint) = first_ifd.model_tiepoint() {
        insert_scalar(
            &mut arrays,
            "geo.model_tiepoint",
            format_f64_list(model_tiepoint),
        )?;
    }

    if let Some(model_transformation) = first_ifd.model_transformation() {
        insert_scalar(
            &mut arrays,
            "geo.model_transformation",
            format_f64_list(model_transformation),
        )?;
    }

    let nodata_value: Option<f64> = first_ifd
        .gdal_nodata()
        .and_then(|s| match s.parse::<f64>() {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::warn!(value = %s, error = %e, "ignoring unparseable GDAL_NODATA tag");
                None
            }
        });

    if let Some(nodata) = first_ifd.gdal_nodata() {
        insert_scalar(&mut arrays, "geo.nodata", nodata.to_string())?;
    }

    if let Some(gdal_metadata) = first_ifd.gdal_metadata() {
        insert_scalar(&mut arrays, "geo.gdal_metadata", gdal_metadata.to_string())?;
    }

    let bands = if first_ifd.tile_count().is_some() {
        read_pixel_bands(Arc::clone(&first_ifd), &reader, nodata_value)
            .map_err(|e| anyhow::anyhow!("Failed to build TIFF tile backends: {e}"))?
    } else {
        read_pixel_bands_stripped(&first_ifd, &reader, nodata_value)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read TIFF strip data: {e}"))?
    };
    for (band_idx, band_array) in bands.into_iter().enumerate() {
        arrays.insert(format!("band.{band_idx}"), band_array);
    }

    if let Some((lon_array, lat_array)) = build_coordinate_arrays(&first_ifd)? {
        arrays.insert(
            "geo.lat".to_string(),
            Arc::new(lat_array) as Arc<dyn NdArrayD>,
        );
        arrays.insert(
            "geo.lon".to_string(),
            Arc::new(lon_array) as Arc<dyn NdArrayD>,
        );
    }

    arrays.sort_keys();

    let dataset = Dataset::new(dataset_name, arrays).await;
    AnyDataset::try_from_dataset(dataset).await
}

fn insert_scalar<T: beacon_nd_array::datatypes::NdArrayType>(
    arrays: &mut IndexMap<String, Arc<dyn NdArrayD>>,
    name: &str,
    value: T,
) -> anyhow::Result<()> {
    let array = NdArray::try_new_from_vec_in_mem(vec![value], vec![], vec![], None)?;
    arrays.insert(name.to_string(), Arc::new(array));
    Ok(())
}

fn format_f64_list(values: &[f64]) -> String {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Derive 1-D coordinate arrays from GeoTIFF geolocation tags.
///
/// Returns `(lon_array, lat_array)` when sufficient geo metadata is present:
/// - `lon_array`: dim `["x"]`, shape `[image_width]`
/// - `lat_array`: dim `["y"]`, shape `[image_height]`
///
/// Returns `None` when no supported geolocation tags are found, or when the
/// transformation involves a rotation that cannot be collapsed to 1-D axes.
fn build_coordinate_arrays(
    ifd: &ImageFileDirectory,
) -> anyhow::Result<Option<(NdArray<f64>, NdArray<f64>)>> {
    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;

    // Tiepoint + pixel scale (most common GeoTIFF encoding).
    // Formula: lon[x] = tie_wx + (x - tie_px) * scale_x
    //          lat[y] = tie_wy - (y - tie_py) * scale_y
    if let (Some(tiepoints), Some(pixel_scale)) = (ifd.model_tiepoint(), ifd.model_pixel_scale()) {
        if tiepoints.len() >= 6 && pixel_scale.len() >= 2 {
            let tie_px = tiepoints[0];
            let tie_py = tiepoints[1];
            let tie_wx = tiepoints[3];
            let tie_wy = tiepoints[4];
            let scale_x = pixel_scale[0];
            let scale_y = pixel_scale[1];

            let lons: Vec<f64> = (0..image_width)
                .map(|x| tie_wx + (x as f64 - tie_px) * scale_x)
                .collect();
            let lats: Vec<f64> = (0..image_height)
                .map(|y| tie_wy - (y as f64 - tie_py) * scale_y)
                .collect();

            let lon_array = NdArray::try_new_from_vec_in_mem(
                lons,
                vec![image_width],
                vec!["x".to_string()],
                None,
            )?;
            let lat_array = NdArray::try_new_from_vec_in_mem(
                lats,
                vec![image_height],
                vec!["y".to_string()],
                None,
            )?;
            return Ok(Some((lon_array, lat_array)));
        }
    }

    // Model transformation matrix (4×4 affine, row-major).
    // Only supported for rectilinear (non-rotated) grids where the off-diagonal
    // terms b (transform[1]) and e (transform[4]) are zero.
    // lon[x] = a * x + d
    // lat[y] = f * y + h
    if let Some(transform) = ifd.model_transformation() {
        if transform.len() >= 16 {
            let a = transform[0];
            let b = transform[1];
            let d = transform[3];
            let e = transform[4];
            let f_coeff = transform[5];
            let h = transform[7];

            if b.abs() < 1e-10 && e.abs() < 1e-10 {
                let lons: Vec<f64> = (0..image_width).map(|x| a * x as f64 + d).collect();
                let lats: Vec<f64> = (0..image_height).map(|y| f_coeff * y as f64 + h).collect();

                let lon_array = NdArray::try_new_from_vec_in_mem(
                    lons,
                    vec![image_width],
                    vec!["x".to_string()],
                    None,
                )?;
                let lat_array = NdArray::try_new_from_vec_in_mem(
                    lats,
                    vec![image_height],
                    vec!["y".to_string()],
                    None,
                )?;
                return Ok(Some((lon_array, lat_array)));
            }
        }
    }

    Ok(None)
}

/// Build one lazy [`TiffTileBackend`] per band for a tiled GeoTIFF.
///
/// No tile data is fetched here. Each backend records the tile geometry and
/// reports [`chunk_shape`] = `[tile_height, tile_width]` so that callers
/// stream data one tile row at a time.
fn read_pixel_bands(
    ifd: Arc<ImageFileDirectory>,
    reader: &ObjectStoreAsyncReader,
    nodata_value: Option<f64>,
) -> anyhow::Result<Vec<Arc<dyn NdArrayD>>> {
    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;
    let tile_width = ifd
        .tile_width()
        .ok_or_else(|| anyhow::anyhow!("tiled TIFF IFD is missing TileWidth"))?
        as usize;
    let tile_height = ifd
        .tile_height()
        .ok_or_else(|| anyhow::anyhow!("tiled TIFF IFD is missing TileLength"))?
        as usize;

    let n_bands = ifd.samples_per_pixel() as usize;
    let is_planar = ifd.planar_configuration() == PlanarConfiguration::Planar;

    let bits_per_sample = ifd.bits_per_sample().first().copied().unwrap_or(8) as usize;
    let sample_format = ifd
        .sample_format()
        .first()
        .copied()
        .unwrap_or(SampleFormat::Uint);

    macro_rules! make_backends {
        ($T:ty, $nodata:expr) => {{
            (0..n_bands)
                .map(|band_idx| -> anyhow::Result<Arc<dyn NdArrayD>> {
                    let backend = TiffTileBackend::<$T> {
                        reader: reader.clone(),
                        ifd: Arc::clone(&ifd),
                        image_width,
                        image_height,
                        tile_width,
                        tile_height,
                        band_config: BandConfig {
                            band_index: band_idx,
                            n_bands,
                            planar: is_planar,
                        },
                        fill_value: $nodata,
                    };
                    let nd = NdArray::new_with_backend(backend)?;
                    Ok(Arc::new(nd) as Arc<dyn NdArrayD>)
                })
                .collect::<anyhow::Result<Vec<Arc<dyn NdArrayD>>>>()
        }};
    }

    match (sample_format, bits_per_sample) {
        (SampleFormat::Float, 32) => make_backends!(f32, nodata_value.map(|v| v as f32)),
        (SampleFormat::Float, 64) => make_backends!(f64, nodata_value),
        (SampleFormat::Uint, 8) => make_backends!(u8, nodata_value.map(|v| v as u8)),
        (SampleFormat::Uint, 16) => make_backends!(u16, nodata_value.map(|v| v as u16)),
        (SampleFormat::Uint, 32) => make_backends!(u32, nodata_value.map(|v| v as u32)),
        (SampleFormat::Uint, 64) => make_backends!(u64, nodata_value.map(|v| v as u64)),
        (SampleFormat::Int, 8) => make_backends!(i8, nodata_value.map(|v| v as i8)),
        (SampleFormat::Int, 16) => make_backends!(i16, nodata_value.map(|v| v as i16)),
        (SampleFormat::Int, 32) => make_backends!(i32, nodata_value.map(|v| v as i32)),
        (SampleFormat::Int, 64) => make_backends!(i64, nodata_value.map(|v| v as i64)),
        _ => anyhow::bail!(
            "Unsupported tiled TIFF format: {sample_format:?} / {bits_per_sample} bits per sample"
        ),
    }
}

/// Fetch and decode every strip for `ifd`, then assemble one `NdArray` per band.
///
/// Only little-endian TIFFs are supported. Each strip is decompressed with the
/// compression method declared by the IFD (e.g. LZW, Deflate) via `async-tiff`'s
/// [`DecoderRegistry`] before the pixel bytes are interpreted.
async fn read_pixel_bands_stripped(
    ifd: &ImageFileDirectory,
    reader: &ObjectStoreAsyncReader,
    nodata_value: Option<f64>,
) -> anyhow::Result<Vec<Arc<dyn NdArrayD>>> {
    let strip_offsets = ifd
        .strip_offsets()
        .ok_or_else(|| anyhow::anyhow!("IFD has no strip offsets"))?;
    let strip_byte_counts = ifd
        .strip_byte_counts()
        .ok_or_else(|| anyhow::anyhow!("IFD has no strip byte counts"))?;

    anyhow::ensure!(
        strip_offsets.len() == strip_byte_counts.len(),
        "strip offsets and byte counts length mismatch ({} vs {})",
        strip_offsets.len(),
        strip_byte_counts.len(),
    );

    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;
    let n_bands = ifd.samples_per_pixel() as usize;
    let bits_per_sample = ifd.bits_per_sample().first().copied().unwrap_or(8) as usize;
    let sample_format = ifd
        .sample_format()
        .first()
        .copied()
        .unwrap_or(SampleFormat::Uint);
    let is_planar = ifd.planar_configuration() == PlanarConfiguration::Planar;

    // A predictor is a reversible transform applied before compression. We don't yet
    // reverse it here, so reject anything other than "no predictor" rather than emit
    // silently wrong pixels.
    let predictor = ifd.predictor().unwrap_or(Predictor::None);
    anyhow::ensure!(
        predictor == Predictor::None,
        "Unsupported predictor {predictor:?} for stripped TIFF (only Predictor::None is supported)"
    );

    // Look up a decoder for the IFD's compression method. The default registry covers
    // None, LZW, Deflate, JPEG and ZSTD.
    let compression = ifd.compression();
    let registry = DecoderRegistry::default();
    let decoder = registry.as_ref().get(&compression).ok_or_else(|| {
        anyhow::anyhow!("Unsupported TIFF compression {compression:?} for stripped layout")
    })?;
    let photometric = ifd.photometric_interpretation();
    let jpeg_tables = ifd.jpeg_tables();

    // Decompress each strip and concatenate into one contiguous, little-endian byte buffer.
    let total: u64 = strip_byte_counts.iter().sum();
    let mut raw: Vec<u8> = Vec::with_capacity(total as usize);
    for (&offset, &count) in strip_offsets.iter().zip(strip_byte_counts.iter()) {
        let bytes = reader
            .get_bytes(offset..offset + count)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read strip at offset {offset}: {e}"))?;
        let decoded = decoder
            .decode_tile(
                bytes,
                photometric,
                jpeg_tables,
                n_bands as u16,
                bits_per_sample as u16,
                None,
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to decompress strip at offset {offset} ({compression:?}): {e}"
                )
            })?;
        raw.extend_from_slice(&decoded);
    }

    // Decode raw bytes as little-endian `$T`, then assemble into per-band 2-D buffers.
    macro_rules! decode_strips {
        ($T:ty, $N:expr) => {{
            let values: Vec<$T> = raw
                .chunks_exact($N)
                .map(|chunk| {
                    // `chunks_exact($N)` yields slices of exactly `$N` bytes, so
                    // the array conversion is infallible.
                    let arr: [u8; $N] = chunk
                        .try_into()
                        .expect("chunks_exact yields slices of exactly $N bytes");
                    <$T>::from_le_bytes(arr)
                })
                .collect();

            let mut band_bufs: Vec<Vec<$T>> = (0..n_bands)
                .map(|_| vec![<$T as Default>::default(); image_height * image_width])
                .collect();

            if !is_planar {
                // Chunky: samples interleaved as px0_b0 px0_b1 … px1_b0 …
                for (i, v) in values.into_iter().enumerate() {
                    let band = i % n_bands;
                    let pixel = i / n_bands;
                    let row = pixel / image_width;
                    let col = pixel % image_width;
                    if row < image_height {
                        band_bufs[band][row * image_width + col] = v;
                    }
                }
            } else {
                // Planar: all of band 0, then all of band 1, …
                let plane_size = image_height * image_width;
                for (i, v) in values.into_iter().enumerate() {
                    let band = i / plane_size;
                    let pixel = i % plane_size;
                    if band < n_bands {
                        band_bufs[band][pixel] = v;
                    }
                }
            }

            band_bufs
                .into_iter()
                .map(|buf| -> anyhow::Result<Arc<dyn NdArrayD>> {
                    let nd = NdArray::try_new_from_vec_in_mem(
                        buf,
                        vec![image_height, image_width],
                        vec!["y".to_string(), "x".to_string()],
                        nodata_value.map(|v| v as $T),
                    )?;
                    Ok(Arc::new(nd) as Arc<dyn NdArrayD>)
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }};
    }

    let bands = match (sample_format, bits_per_sample) {
        (SampleFormat::Uint, 8) => decode_strips!(u8, 1),
        (SampleFormat::Uint, 16) => decode_strips!(u16, 2),
        (SampleFormat::Uint, 32) => decode_strips!(u32, 4),
        (SampleFormat::Uint, 64) => decode_strips!(u64, 8),
        (SampleFormat::Int, 8) => decode_strips!(i8, 1),
        (SampleFormat::Int, 16) => decode_strips!(i16, 2),
        (SampleFormat::Int, 32) => decode_strips!(i32, 4),
        (SampleFormat::Int, 64) => decode_strips!(i64, 8),
        (SampleFormat::Float, 32) => decode_strips!(f32, 4),
        (SampleFormat::Float, 64) => decode_strips!(f64, 8),
        _ => anyhow::bail!(
            "Unsupported stripped TIFF format: {sample_format:?} / {bits_per_sample} bits per sample"
        ),
    };

    Ok(bands)
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;
    use object_store::memory::InMemory;

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../test-files/test.tif");
    /// Small synthetic fixture generated by `test-files/gen_lzw_stripped_f32.py`:
    /// a 64×48, LZW-compressed, stripped, float32 GeoTIFF with a nodata block. It
    /// reproduces the compression layout of real-world climate GeoTIFFs without
    /// committing a large binary.
    const LZW_STRIPPED_TIF_BYTES: &[u8] =
        include_bytes!("../test-files/synthetic_lzw_stripped_f32.tif");

    async fn scalar_u32(dataset: &AnyDataset, name: &str) -> u32 {
        let arr = dataset
            .get_array(name)
            .unwrap_or_else(|| panic!("missing array '{name}'"));
        let nd = arr
            .as_any()
            .downcast_ref::<NdArray<u32>>()
            .unwrap_or_else(|| panic!("array '{name}' has unexpected type"));
        nd.clone_into_raw_vec().await[0]
    }

    async fn scalar_u16(dataset: &AnyDataset, name: &str) -> u16 {
        let arr = dataset
            .get_array(name)
            .unwrap_or_else(|| panic!("missing array '{name}'"));
        let nd = arr
            .as_any()
            .downcast_ref::<NdArray<u16>>()
            .unwrap_or_else(|| panic!("array '{name}' has unexpected type"));
        nd.clone_into_raw_vec().await[0]
    }

    #[tokio::test]
    async fn open_dataset_errors_for_missing_object_path() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store;
        let path = Path::from("tests/does-not-exist.tiff");

        let err = open_dataset(object_store, path)
            .await
            .expect_err("missing object should return an error");

        assert!(
            err.to_string().contains("Failed to open TIFF metadata"),
            "unexpected error message: {err}"
        );
    }

    /// Regression test: this fixture is an LZW-compressed, stripped float32 GeoTIFF.
    /// The stripped reader must decompress each strip before decoding pixels — reading
    /// the raw (still-compressed) bytes as little-endian floats produces garbage values.
    ///
    /// The fixture is synthetic (see the generator script under `test-files/`) with a
    /// known layout:
    ///   - 64×48, single band, float32, LZW-compressed, stripped (6 rows/strip)
    ///   - valid pixels form a left→right gradient from 15.0 to 19.0
    ///   - the top-left 8×8 block is nodata (-3.4e38)
    #[tokio::test]
    async fn open_dataset_decodes_lzw_compressed_stripped_geotiff() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/synthetic_lzw.tif");

        store
            .put(
                &path,
                bytes::Bytes::copy_from_slice(LZW_STRIPPED_TIF_BYTES).into(),
            )
            .await
            .expect("should write LZW GeoTIFF fixture into object store");

        let dataset = open_dataset(object_store, path)
            .await
            .expect("LZW-compressed stripped GeoTIFF should open successfully");

        const WIDTH: usize = 64;
        const HEIGHT: usize = 48;
        assert_eq!(scalar_u32(&dataset, "image.width").await, WIDTH as u32);
        assert_eq!(scalar_u32(&dataset, "image.height").await, HEIGHT as u32);
        assert_eq!(scalar_u16(&dataset, "image.samples_per_pixel").await, 1);
        assert_eq!(scalar_u16(&dataset, "image.bits_per_sample").await, 32);

        let band = dataset
            .get_array("band.0")
            .expect("band.0 should be present");
        let band_nd = band
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("band.0 should be NdArray<f32>");
        let values = band_nd.clone_into_raw_vec().await;
        assert_eq!(values.len(), WIDTH * HEIGHT);

        // Every pixel must be either the nodata fill or within the gradient range.
        // Before decompression was wired up, the strip bytes decoded into NaNs and wild
        // magnitudes far outside this band.
        const DATA_MIN: f32 = 15.0;
        const DATA_MAX: f32 = 19.0;
        let mut n_valid = 0usize;
        let mut n_nodata = 0usize;
        for (i, &v) in values.iter().enumerate() {
            if v <= -1e30 {
                n_nodata += 1;
                continue;
            }
            assert!(
                v.is_finite() && (DATA_MIN - 1e-3..=DATA_MAX + 1e-3).contains(&v),
                "band.0[{i}] = {v} is neither nodata nor within [{DATA_MIN}, {DATA_MAX}]"
            );
            n_valid += 1;
        }

        // The generator carves an 8×8 nodata block and fills the rest with the gradient.
        assert_eq!(n_nodata, 8 * 8, "unexpected nodata pixel count");
        assert_eq!(
            n_valid,
            WIDTH * HEIGHT - 8 * 8,
            "unexpected valid pixel count"
        );

        // Row 0 starts inside the nodata block; the first valid pixel is at column 8.
        assert!(values[0] <= -1e30, "band.0[0] should be nodata");
        let first_valid = values[8];
        assert!(
            (first_valid - 15.507_936).abs() < 1e-4,
            "band.0[8] = {first_valid}, expected the gradient value 15.507936"
        );

        // Observed extrema across the decoded band must match the gradient endpoints.
        let observed_min = values
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > -1e30)
            .fold(f32::INFINITY, f32::min);
        let observed_max = values
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > -1e30)
            .fold(f32::NEG_INFINITY, f32::max);
        assert!(
            (observed_min - DATA_MIN).abs() < 1e-4,
            "observed min {observed_min} should match gradient minimum {DATA_MIN}"
        );
        assert!(
            (observed_max - DATA_MAX).abs() < 1e-4,
            "observed max {observed_max} should match gradient maximum {DATA_MAX}"
        );
    }

    #[tokio::test]
    async fn open_dataset_reads_real_stripped_geotiff_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/test.tif");

        store
            .put(&path, bytes::Bytes::copy_from_slice(TEST_TIF_BYTES).into())
            .await
            .expect("should write real stripped GeoTIFF fixture into object store");

        let dataset = open_dataset(object_store, path)
            .await
            .expect("real stripped GeoTIFF should open successfully");

        assert_eq!(scalar_u32(&dataset, "image.width").await, 1287);
        assert_eq!(scalar_u32(&dataset, "image.height").await, 380);
        assert_eq!(scalar_u16(&dataset, "image.samples_per_pixel").await, 1);
        assert_eq!(scalar_u16(&dataset, "image.bits_per_sample").await, 32);

        let band = dataset
            .get_array("band.0")
            .expect("band.0 should be present");
        let band_nd = band
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("band.0 should be NdArray<f32>");
        assert_eq!(band_nd.dimensions(), vec!["y", "x"]);
        assert_eq!(band_nd.clone_into_raw_vec().await.len(), 1287 * 380);

        let lat_arr = dataset
            .get_array("geo.lat")
            .expect("geo.lat should be present");
        let lat = lat_arr
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .expect("geo.lat should be NdArray<f64>");
        assert_eq!(lat.dimensions(), vec!["y"]);
        let lats = lat.clone_into_raw_vec().await;
        assert_eq!(lats.len(), 380);
        // ModelTransformationTag: lat[y] = 0.04166667002172143 * y + 30.16666666498914
        assert!(
            (lats[0] - 30.166_666_664_989_14).abs() < 1e-8,
            "lat[0]={}",
            lats[0]
        );
        assert!(
            (lats[1] - 30.208_333_335_010_863).abs() < 1e-8,
            "lat[1]={}",
            lats[1]
        );
        assert!(
            (lats[379] - 45.958_334_603_221_566).abs() < 1e-8,
            "lat[379]={}",
            lats[379]
        );

        let lon_arr = dataset
            .get_array("geo.lon")
            .expect("geo.lon should be present");
        let lon = lon_arr
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .expect("geo.lon should be NdArray<f64>");
        assert_eq!(lon.dimensions(), vec!["x"]);
        let lons = lon.clone_into_raw_vec().await;
        assert_eq!(lons.len(), 1287);
        // ModelTransformationTag: lon[x] = 0.0416666671610546 * x + -17.312499364464315
        assert!(
            (lons[0] - -17.312_499_364_464_315).abs() < 1e-8,
            "lon[0]={}",
            lons[0]
        );
        assert!(
            (lons[1] - -17.270_832_697_303_263).abs() < 1e-8,
            "lon[1]={}",
            lons[1]
        );
        assert!(
            (lons[1286] - 36.270_834_604_651_895).abs() < 1e-8,
            "lon[1286]={}",
            lons[1286]
        );
    }
}
