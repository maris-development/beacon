use std::{ops::Range, panic::AssertUnwindSafe, sync::Arc};

use async_tiff::ImageFileDirectory;
use async_tiff::metadata::{TiffMetadataReader, cache::ReadaheadMetadataCache};
use async_tiff::reader::{AsyncFileReader, Endianness};
use async_tiff::tags::SampleFormat;
use beacon_nd_array::{
    NdArray, NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use futures::FutureExt;

use crate::backend::TiffBandBackend;
use crate::block::{BlockLayout, TiffImage, TiffSample};
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
        // Object stores reject an empty range, so answer it here.
        if range.is_empty() {
            return Ok(bytes::Bytes::new());
        }
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
/// - Reads TIFF metadata through `async-tiff`.
/// - Exposes deterministic metadata arrays.
/// - Exposes each band as a lazy array. Tiled and stripped images read their
///   blocks on demand, and sparse blocks read as nodata.
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

    let (first_ifd, endianness) = read_image_ifd(&reader).await?;

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

    let layout = BlockLayout::from_ifd(&first_ifd, endianness)
        .map_err(|e| anyhow::anyhow!("Failed to read TIFF block layout: {e}"))?;

    if layout.tiled {
        insert_scalar(&mut arrays, "image.tile_width", layout.block_width as u32)?;
        insert_scalar(&mut arrays, "image.tile_height", layout.block_height as u32)?;
        insert_scalar(&mut arrays, "image.tile_count_x", layout.blocks_across as u64)?;
        insert_scalar(&mut arrays, "image.tile_count_y", layout.blocks_down as u64)?;
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

    // GDAL ends the tag with a NUL byte, and some writers add spaces.
    let nodata = first_ifd
        .gdal_nodata()
        .map(|s| s.trim_matches(|c: char| c == '\0' || c.is_whitespace()))
        .filter(|s| !s.is_empty());

    if let Some(nodata) = nodata {
        insert_scalar(&mut arrays, "geo.nodata", nodata.to_string())?;
    }

    if let Some(gdal_metadata) = first_ifd.gdal_metadata() {
        insert_scalar(&mut arrays, "geo.gdal_metadata", gdal_metadata.to_string())?;
    }

    let bands = read_pixel_bands(layout, &reader, nodata)
        .map_err(|e| anyhow::anyhow!("Failed to build TIFF band arrays: {e}"))?;
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
/// Each value is the center of its pixel, the same as a CF coordinate.
///
/// Returns `None` when no supported geolocation tags are found, or when the
/// transformation involves a rotation that cannot be collapsed to 1-D axes.
fn build_coordinate_arrays(
    ifd: &ImageFileDirectory,
) -> anyhow::Result<Option<(NdArray<f64>, NdArray<f64>)>> {
    let image_width = ifd.image_width() as usize;
    let image_height = ifd.image_height() as usize;

    // For PixelIsArea, raster point (i, j) is the top-left corner of pixel (i, j),
    // so its center is at (i + 0.5, j + 0.5). For PixelIsPoint, it is the center.
    let center = pixel_center_offset(ifd);

    // Tiepoint + pixel scale (most common GeoTIFF encoding).
    // Formula: lon[x] = tie_wx + (x + center - tie_px) * scale_x
    //          lat[y] = tie_wy - (y + center - tie_py) * scale_y
    if let (Some(tiepoints), Some(pixel_scale)) = (ifd.model_tiepoint(), ifd.model_pixel_scale()) {
        if tiepoints.len() >= 6 && pixel_scale.len() >= 2 {
            let tie_px = tiepoints[0];
            let tie_py = tiepoints[1];
            let tie_wx = tiepoints[3];
            let tie_wy = tiepoints[4];
            let scale_x = pixel_scale[0];
            let scale_y = pixel_scale[1];

            let lons: Vec<f64> = (0..image_width)
                .map(|x| tie_wx + (x as f64 + center - tie_px) * scale_x)
                .collect();
            let lats: Vec<f64> = (0..image_height)
                .map(|y| tie_wy - (y as f64 + center - tie_py) * scale_y)
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
    // lon[x] = a * (x + center) + d
    // lat[y] = f * (y + center) + h
    if let Some(transform) = ifd.model_transformation() {
        if transform.len() >= 16 {
            let a = transform[0];
            let b = transform[1];
            let d = transform[3];
            let e = transform[4];
            let f_coeff = transform[5];
            let h = transform[7];

            if b.abs() < 1e-10 && e.abs() < 1e-10 {
                let lons: Vec<f64> = (0..image_width)
                    .map(|x| a * (x as f64 + center) + d)
                    .collect();
                let lats: Vec<f64> = (0..image_height)
                    .map(|y| f_coeff * (y as f64 + center) + h)
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
    }

    Ok(None)
}

/// The raster offset from a tiepoint to a pixel center, from `GTRasterTypeGeoKey`.
///
/// Returns 0.5 for PixelIsArea (1), and 0 for PixelIsPoint (2). GeoTIFF makes
/// PixelIsArea the default, so a missing or unknown key also gives 0.5.
fn pixel_center_offset(ifd: &ImageFileDirectory) -> f64 {
    const PIXEL_IS_AREA: u16 = 1;
    const PIXEL_IS_POINT: u16 = 2;
    match ifd.geo_key_directory().and_then(|keys| keys.raster_type) {
        Some(PIXEL_IS_POINT) => 0.0,
        Some(PIXEL_IS_AREA) | None => 0.5,
        Some(other) => {
            tracing::warn!(raster_type = other, "unknown GTRasterTypeGeoKey, using PixelIsArea");
            0.5
        }
    }
}

/// Read the IFDs and return the full-resolution image and the byte order.
///
/// `async-tiff` panics on some malformed IFDs. The panic becomes an error here,
/// so a bad file fails its query and not the whole server task.
async fn read_image_ifd(
    reader: &ObjectStoreAsyncReader,
) -> anyhow::Result<(ImageFileDirectory, Endianness)> {
    let cached_reader = ReadaheadMetadataCache::new(reader.clone());
    let read = async {
        let mut metadata_reader = TiffMetadataReader::try_open(&cached_reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open TIFF metadata: {e}"))?;
        let endianness = metadata_reader.endianness();
        let ifds = metadata_reader
            .read_all_ifds(&cached_reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read TIFF metadata IFDs: {e}"))?;
        Ok::<_, anyhow::Error>((ifds, endianness))
    };
    let (ifds, endianness) = AssertUnwindSafe(read).catch_unwind().await.map_err(|panic| {
        let reason = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown cause".to_string());
        anyhow::anyhow!("Failed to read TIFF metadata IFDs: malformed IFD ({reason})")
    })??;

    // NewSubfileType bit 0 marks an overview and bit 2 marks a mask. Skip both.
    let is_full_image = |ifd: &ImageFileDirectory| ifd.new_subfile_type().unwrap_or(0) & 0b101 == 0;
    let index = ifds.iter().position(is_full_image).unwrap_or(0);
    let ifd = ifds
        .into_iter()
        .nth(index)
        .ok_or_else(|| anyhow::anyhow!("TIFF contains no image file directories (IFDs)."))?;
    Ok((ifd, endianness))
}

/// Build one lazy band array per sample of the image.
///
/// No pixel data is fetched here. All bands share one [`TiffImage`], so the
/// bands of a pixel-interleaved image fetch and decode each block one time.
fn read_pixel_bands(
    layout: BlockLayout,
    reader: &ObjectStoreAsyncReader,
    nodata: Option<&str>,
) -> anyhow::Result<Vec<Arc<dyn NdArrayD>>> {
    fn make_bands<T: TiffSample>(
        layout: BlockLayout,
        reader: &ObjectStoreAsyncReader,
        nodata: Option<&str>,
    ) -> anyhow::Result<Vec<Arc<dyn NdArrayD>>> {
        let fill_value = nodata.and_then(|text| {
            let value = T::parse_nodata(text);
            if value.is_none() {
                tracing::warn!(value = %text, "ignoring GDAL_NODATA value that the band type cannot hold");
            }
            value
        });
        let n_bands = layout.n_bands;
        let image = Arc::new(TiffImage::<T>::new(reader.clone(), layout)?);
        (0..n_bands)
            .map(|band| -> anyhow::Result<Arc<dyn NdArrayD>> {
                let backend = TiffBandBackend {
                    image: Arc::clone(&image),
                    band,
                    fill_value,
                };
                Ok(Arc::new(NdArray::new_with_backend(backend)?) as Arc<dyn NdArrayD>)
            })
            .collect()
    }

    match (layout.sample_format, layout.bits_per_sample) {
        (SampleFormat::Float, 32) => make_bands::<f32>(layout, reader, nodata),
        (SampleFormat::Float, 64) => make_bands::<f64>(layout, reader, nodata),
        (SampleFormat::Uint, 8) => make_bands::<u8>(layout, reader, nodata),
        (SampleFormat::Uint, 16) => make_bands::<u16>(layout, reader, nodata),
        (SampleFormat::Uint, 32) => make_bands::<u32>(layout, reader, nodata),
        (SampleFormat::Uint, 64) => make_bands::<u64>(layout, reader, nodata),
        (SampleFormat::Int, 8) => make_bands::<i8>(layout, reader, nodata),
        (SampleFormat::Int, 16) => make_bands::<i16>(layout, reader, nodata),
        (SampleFormat::Int, 32) => make_bands::<i32>(layout, reader, nodata),
        (SampleFormat::Int, 64) => make_bands::<i64>(layout, reader, nodata),
        (sample_format, bits) => anyhow::bail!(
            "Unsupported TIFF format: {sample_format:?} / {bits} bits per sample"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;
    use beacon_nd_array::array::subset::ArraySubset;
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

    // Fixtures from `test-files/gen_block_fixtures.py`. Each pixel follows a formula.
    const SPARSE_TILED: &[u8] = include_bytes!("../test-files/sparse_tiled_i16.tif");
    const SPARSE_TILED_BIGTIFF: &[u8] =
        include_bytes!("../test-files/sparse_tiled_bigtiff_i16.tif");
    const SPARSE_TILED_NO_NODATA: &[u8] =
        include_bytes!("../test-files/sparse_tiled_no_nodata_i16.tif");
    const SPARSE_STRIPPED: &[u8] = include_bytes!("../test-files/sparse_stripped_i16.tif");
    const PLANAR_PARTLY_SPARSE: &[u8] =
        include_bytes!("../test-files/planar_partly_sparse_u8.tif");
    const BIG_ENDIAN_PREDICTOR_TILED: &[u8] =
        include_bytes!("../test-files/big_endian_predictor_tiled_u16.tif");
    const FLOAT_PREDICTOR_STRIPPED: &[u8] =
        include_bytes!("../test-files/float_predictor_stripped_f32.tif");
    const INT_PREDICTOR_STRIPPED: &[u8] =
        include_bytes!("../test-files/int_predictor_stripped_i32.tif");
    const PACKBITS_TILED: &[u8] = include_bytes!("../test-files/packbits_tiled_u16.tif");
    const PIXEL_IS_AREA: &[u8] = include_bytes!("../test-files/pixel_is_area.tif");
    const PIXEL_IS_POINT: &[u8] = include_bytes!("../test-files/pixel_is_point.tif");

    async fn open_fixture(bytes: &'static [u8]) -> anyhow::Result<AnyDataset> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("tests/fixture.tif");
        store
            .put(&path, bytes::Bytes::from_static(bytes).into())
            .await
            .unwrap();
        open_dataset(store, path).await
    }

    fn band<T: beacon_nd_array::datatypes::NdArrayType>(
        dataset: &AnyDataset,
        index: usize,
    ) -> NdArray<T> {
        dataset
            .get_array(&format!("band.{index}"))
            .unwrap_or_else(|| panic!("missing band.{index}"))
            .as_any()
            .downcast_ref::<NdArray<T>>()
            .expect("band has an unexpected type")
            .clone()
    }

    /// Compare every value of a `width` x `height` grid with `expected(row, col)`.
    fn assert_grid<T: PartialEq + std::fmt::Debug>(
        values: &[T],
        width: usize,
        height: usize,
        expected: impl Fn(usize, usize) -> T,
    ) {
        assert_eq!(values.len(), width * height);
        for row in 0..height {
            for col in 0..width {
                let want = expected(row, col);
                assert_eq!(values[row * width + col], want, "pixel (row {row}, col {col})");
            }
        }
    }

    /// Tiles (0,0) and (2,1) of the sparse fixtures hold data. The other tiles are sparse.
    fn sparse_tiled_value(row: usize, col: usize, fill: i16) -> i16 {
        match (row / 16, col / 16) {
            (0, 0) => 7,
            (1, 2) => 5,
            _ => fill,
        }
    }

    #[tokio::test]
    async fn sparse_tiles_read_as_nodata() {
        for fixture in [SPARSE_TILED, SPARSE_TILED_BIGTIFF] {
            let dataset = open_fixture(fixture).await.unwrap();
            let band = band::<i16>(&dataset, 0);
            assert_eq!(band.fill_value().await, Some(-32767));
            assert_grid(&band.clone_into_raw_vec().await, 64, 64, |r, c| {
                sparse_tiled_value(r, c, -32767)
            });
        }
    }

    #[tokio::test]
    async fn sparse_tiles_read_as_zero_without_nodata() {
        let dataset = open_fixture(SPARSE_TILED_NO_NODATA).await.unwrap();
        let band = band::<i16>(&dataset, 0);
        assert_eq!(band.fill_value().await, None);
        assert_grid(&band.clone_into_raw_vec().await, 64, 64, |r, c| {
            sparse_tiled_value(r, c, 0)
        });
    }

    #[tokio::test]
    async fn subset_across_sparse_and_full_tiles() {
        let dataset = open_fixture(SPARSE_TILED).await.unwrap();
        let subset = band::<i16>(&dataset, 0)
            .subset(ArraySubset::new(vec![10, 12], vec![20, 30]))
            .await
            .unwrap();
        assert_grid(&subset.clone_into_raw_vec().await, 30, 20, |r, c| {
            sparse_tiled_value(r + 10, c + 12, -32767)
        });
    }

    #[tokio::test]
    async fn sparse_strips_read_as_nodata() {
        let dataset = open_fixture(SPARSE_STRIPPED).await.unwrap();
        let band = band::<i16>(&dataset, 0);
        assert_eq!(band.fill_value().await, Some(-1));
        assert_grid(&band.clone_into_raw_vec().await, 64, 64, |r, _| {
            if (8..16).contains(&r) { 3 } else { -1 }
        });
    }

    #[tokio::test]
    async fn planar_band_reads_when_another_band_is_sparse() {
        let dataset = open_fixture(PLANAR_PARTLY_SPARSE).await.unwrap();
        let first = band::<u8>(&dataset, 0).clone_into_raw_vec().await;
        assert_grid(&first, 32, 32, |r, c| ((r * 100 + c) % 200) as u8);
        let second = band::<u8>(&dataset, 1).clone_into_raw_vec().await;
        assert_grid(&second, 32, 32, |r, c| if r < 16 && c >= 16 { 9 } else { 255 });
    }

    #[tokio::test]
    async fn big_endian_tiles_with_predictor_and_edge_tiles() {
        let dataset = open_fixture(BIG_ENDIAN_PREDICTOR_TILED).await.unwrap();
        let values = band::<u16>(&dataset, 0).clone_into_raw_vec().await;
        assert_grid(&values, 50, 37, |r, c| (r * 100 + c) as u16);
    }

    #[tokio::test]
    async fn packbits_tiles() {
        let dataset = open_fixture(PACKBITS_TILED).await.unwrap();
        let values = band::<u16>(&dataset, 0).clone_into_raw_vec().await;
        assert_grid(&values, 50, 37, |r, c| (r * 100 + c) as u16);
    }

    #[tokio::test]
    async fn float_predictor_strips_with_short_last_strip() {
        let dataset = open_fixture(FLOAT_PREDICTOR_STRIPPED).await.unwrap();
        for index in 0..3 {
            let values = band::<f32>(&dataset, index).clone_into_raw_vec().await;
            assert_grid(&values, 20, 37, |r, c| (r * 100 + c + index * 10000) as f32 / 4.0);
        }
        // The last strip holds rows 32..37 only.
        let tail = band::<f32>(&dataset, 2)
            .subset(ArraySubset::new(vec![30, 5], vec![7, 10]))
            .await
            .unwrap()
            .clone_into_raw_vec()
            .await;
        assert_grid(&tail, 10, 7, |r, c| ((r + 30) * 100 + c + 5 + 20000) as f32 / 4.0);
    }

    #[tokio::test]
    async fn horizontal_predictor_strips() {
        let dataset = open_fixture(INT_PREDICTOR_STRIPPED).await.unwrap();
        let values = band::<i32>(&dataset, 0).clone_into_raw_vec().await;
        assert_grid(&values, 33, 21, |r, c| (r * 100 + c) as i32 - 1000);
    }

    async fn coordinates(dataset: &AnyDataset) -> (Vec<f64>, Vec<f64>) {
        let read = |name: &str| {
            dataset
                .get_array(name)
                .unwrap_or_else(|| panic!("missing {name}"))
                .as_any()
                .downcast_ref::<NdArray<f64>>()
                .expect("coordinates are f64")
                .clone()
        };
        let lon = read("geo.lon").clone_into_raw_vec().await;
        let lat = read("geo.lat").clone_into_raw_vec().await;
        (lon, lat)
    }

    /// Issue #525: both raster types give the pixel center, the same as CF and `read_netcdf`.
    #[tokio::test]
    async fn coordinates_are_pixel_centers_for_both_raster_types() {
        for fixture in [PIXEL_IS_AREA, PIXEL_IS_POINT] {
            let dataset = open_fixture(fixture).await.unwrap();
            let (lon, lat) = coordinates(&dataset).await;
            assert_eq!(lon, vec![0.5, 1.5, 2.5, 3.5]);
            assert_eq!(lat, vec![2.5, 1.5, 0.5]);
        }
    }

    #[tokio::test]
    async fn truncated_file_is_an_error() {
        for length in [8, 64, 200, 400] {
            let bytes: &'static [u8] = &SPARSE_TILED[..length];
            let result = async {
                let dataset = open_fixture(bytes).await?;
                band::<i16>(&dataset, 0).subset(ArraySubset::new(vec![0, 0], vec![64, 64])).await
            };
            assert!(result.await.is_err(), "a file cut at {length} bytes must fail");
        }
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
        assert_eq!(n_valid, WIDTH * HEIGHT - 8 * 8, "unexpected valid pixel count");

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
        // ModelTransformationTag, PixelIsArea: lat[y] = 0.04166667002172143 * (y + 0.5) + 30.16666666498914
        assert!(
            (lats[0] - 30.1875).abs() < 1e-8,
            "lat[0]={}",
            lats[0]
        );
        assert!(
            (lats[1] - 30.229_166_670_021_723).abs() < 1e-8,
            "lat[1]={}",
            lats[1]
        );
        assert!(
            (lats[379] - 45.979_167_938_232_42).abs() < 1e-8,
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
        // ModelTransformationTag, PixelIsArea: lon[x] = 0.0416666671610546 * (x + 0.5) + -17.312499364464315
        assert!(
            (lons[0] - -17.291_666_030_883_79).abs() < 1e-8,
            "lon[0]={}",
            lons[0]
        );
        assert!(
            (lons[1] - -17.249_999_363_722_733).abs() < 1e-8,
            "lon[1]={}",
            lons[1]
        );
        assert!(
            (lons[1286] - 36.291_667_938_232_42).abs() < 1e-8,
            "lon[1286]={}",
            lons[1286]
        );
    }
}
