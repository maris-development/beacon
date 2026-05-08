use std::{ops::Range, sync::Arc};

use async_tiff::metadata::{TiffMetadataReader, cache::ReadaheadMetadataCache};
use async_tiff::reader::AsyncFileReader;
use async_tiff::tags::{PlanarConfiguration, SampleFormat};
use async_tiff::{ImageFileDirectory, TypedArray, decoder::DecoderRegistry};
use beacon_nd_array::{
    NdArray, NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use indexmap::IndexMap;
use object_store::{ObjectStore, path::Path};

#[derive(Clone)]
struct ObjectStoreAsyncReader {
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

    let first_ifd = ifds
        .first()
        .ok_or_else(|| anyhow::anyhow!("TIFF contains no image file directories (IFDs)."))?;

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
        insert_scalar(
            &mut arrays,
            "image.tile_width",
            first_ifd.tile_width().unwrap(),
        )?;
        insert_scalar(
            &mut arrays,
            "image.tile_height",
            first_ifd.tile_height().unwrap(),
        )?;
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

    if let Some(nodata) = first_ifd.gdal_nodata() {
        insert_scalar(&mut arrays, "geo.nodata", nodata.to_string())?;
    }

    if let Some(gdal_metadata) = first_ifd.gdal_metadata() {
        insert_scalar(&mut arrays, "geo.gdal_metadata", gdal_metadata.to_string())?;
    }

    let bands = if first_ifd.tile_count().is_some() {
        read_pixel_bands(first_ifd, &reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read TIFF pixel data: {e}"))?
    } else {
        read_pixel_bands_stripped(first_ifd, &reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read TIFF strip data: {e}"))?
    };
    for (band_idx, band_array) in bands.into_iter().enumerate() {
        arrays.insert(format!("band.{band_idx}"), band_array);
    }

    if let Some((lon_array, lat_array)) = build_coordinate_arrays(first_ifd)? {
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

/// Fetch and decode every tile for `ifd`, then assemble one `NdArray` per band.
///
/// Returns a `Vec` where index `i` is the per-band 2-D array (dims: `["y", "x"]`)
/// with shape `[image_height, image_width]`.
async fn read_pixel_bands(
    ifd: &ImageFileDirectory,
    reader: &ObjectStoreAsyncReader,
) -> anyhow::Result<Vec<Arc<dyn NdArrayD>>> {
    let (tiles_x, tiles_y) = ifd
        .tile_count()
        .ok_or_else(|| anyhow::anyhow!("IFD has no tiles"))?;

    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;
    let tile_width = ifd.tile_width().unwrap() as usize;
    let tile_height = ifd.tile_height().unwrap() as usize;
    let n_bands = ifd.samples_per_pixel() as usize;
    let is_planar = ifd.planar_configuration() == PlanarConfiguration::Planar;

    let coords: Vec<(usize, usize)> = (0..tiles_y)
        .flat_map(|ty| (0..tiles_x).map(move |tx| (tx, ty)))
        .collect();

    let tiles = ifd
        .fetch_tiles(&coords, reader)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch TIFF tiles: {e}"))?;

    let decoder_registry = DecoderRegistry::default();
    let decoded: Vec<async_tiff::Array> = tiles
        .into_iter()
        .map(|t| t.decode(&decoder_registry))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to decode TIFF tile: {e}"))?;

    if decoded.is_empty() {
        return Ok(vec![]);
    }

    let decoded_with_coords: Vec<(async_tiff::Array, (usize, usize))> =
        decoded.into_iter().zip(coords.into_iter()).collect();

    // Assembles per-band pixel buffers from decoded tiles, then wraps each in an NdArray.
    // Supports both chunky (shape: [tile_h, tile_w, bands]) and planar
    // (shape: [bands, tile_h, tile_w]) TIFF configurations.
    macro_rules! assemble {
        ($variant:ident => $T:ty) => {{
            let mut band_bufs: Vec<Vec<$T>> = (0..n_bands)
                .map(|_| {
                    let zero: $T = Default::default();
                    vec![zero; image_height * image_width]
                })
                .collect();

            for (array, (tx, ty)) in &decoded_with_coords {
                let data = match array.data() {
                    TypedArray::$variant(d) => d.as_slice(),
                    _ => anyhow::bail!("inconsistent tile data types in TIFF"),
                };
                let valid_rows = tile_height.min(image_height.saturating_sub(ty * tile_height));
                let valid_cols = tile_width.min(image_width.saturating_sub(tx * tile_width));

                if !is_planar {
                    // Chunky: flat index = row * tile_w * bands + col * bands + band
                    for row in 0..valid_rows {
                        for col in 0..valid_cols {
                            for band in 0..n_bands {
                                let src = row * tile_width * n_bands + col * n_bands + band;
                                let dst = (ty * tile_height + row) * image_width
                                    + (tx * tile_width + col);
                                band_bufs[band][dst] = data[src];
                            }
                        }
                    }
                } else {
                    // Planar: flat index = band * tile_h * tile_w + row * tile_w + col
                    for band in 0..n_bands {
                        for row in 0..valid_rows {
                            for col in 0..valid_cols {
                                let src = band * tile_height * tile_width + row * tile_width + col;
                                let dst = (ty * tile_height + row) * image_width
                                    + (tx * tile_width + col);
                                band_bufs[band][dst] = data[src];
                            }
                        }
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
                        None,
                    )?;
                    Ok(Arc::new(nd) as Arc<dyn NdArrayD>)
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }};
    }

    let bands = match decoded_with_coords[0].0.data() {
        TypedArray::Bool(_) => assemble!(Bool => bool),
        TypedArray::UInt8(_) => assemble!(UInt8 => u8),
        TypedArray::UInt16(_) => assemble!(UInt16 => u16),
        TypedArray::UInt32(_) => assemble!(UInt32 => u32),
        TypedArray::UInt64(_) => assemble!(UInt64 => u64),
        TypedArray::Int8(_) => assemble!(Int8 => i8),
        TypedArray::Int16(_) => assemble!(Int16 => i16),
        TypedArray::Int32(_) => assemble!(Int32 => i32),
        TypedArray::Int64(_) => assemble!(Int64 => i64),
        TypedArray::Float32(_) => assemble!(Float32 => f32),
        TypedArray::Float64(_) => assemble!(Float64 => f64),
    };

    Ok(bands)
}

/// Fetch and decode every strip for `ifd`, then assemble one `NdArray` per band.
///
/// Only little-endian TIFFs are supported. Uncompressed strips are read directly;
/// no decompression is applied (use tiled layout for compressed data).
async fn read_pixel_bands_stripped(
    ifd: &ImageFileDirectory,
    reader: &ObjectStoreAsyncReader,
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

    // Read all strips into one contiguous byte buffer (little-endian, no decompression).
    let total: u64 = strip_byte_counts.iter().sum();
    let mut raw: Vec<u8> = Vec::with_capacity(total as usize);
    for (&offset, &count) in strip_offsets.iter().zip(strip_byte_counts.iter()) {
        let bytes = reader
            .get_bytes(offset..offset + count)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read strip at offset {offset}: {e}"))?;
        raw.extend_from_slice(&bytes);
    }

    // Decode raw bytes as little-endian `$T`, then assemble into per-band 2-D buffers.
    macro_rules! decode_strips {
        ($T:ty, $N:expr) => {{
            let values: Vec<$T> = raw
                .chunks_exact($N)
                .map(|chunk| {
                    let arr: [u8; $N] = chunk.try_into().unwrap();
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
                        None,
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
