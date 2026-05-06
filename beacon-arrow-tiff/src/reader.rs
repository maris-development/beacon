use std::{ops::Range, sync::Arc};

use async_tiff::metadata::{TiffMetadataReader, cache::ReadaheadMetadataCache};
use async_tiff::reader::AsyncFileReader;
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
    let cached_reader = ReadaheadMetadataCache::new(reader);

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

    if first_ifd.tile_count().is_none() {
        anyhow::bail!(
            "Non-tiled TIFF is not supported yet. This reader currently supports tiled TIFF/GeoTIFF files only."
        );
    }

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

    if let Some(tile_width) = first_ifd.tile_width() {
        insert_scalar(&mut arrays, "image.tile_width", tile_width)?;
    }
    if let Some(tile_height) = first_ifd.tile_height() {
        insert_scalar(&mut arrays, "image.tile_height", tile_height)?;
    }

    if let Some((tiles_x, tiles_y)) = first_ifd.tile_count() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;
    use object_store::memory::InMemory;

    const REAL_TILED_TIFF_BYTES: &[u8] = include_bytes!("../test-files/TEMP_AVG_20201201.tif");

    fn ifd_entry(tag: u16, value_type: u16, count: u32, value: u32) -> [u8; 12] {
        let mut entry = [0u8; 12];
        entry[0..2].copy_from_slice(&tag.to_le_bytes());
        entry[2..4].copy_from_slice(&value_type.to_le_bytes());
        entry[4..8].copy_from_slice(&count.to_le_bytes());
        entry[8..12].copy_from_slice(&value.to_le_bytes());
        entry
    }

    fn build_tiff(entries: &[[u8; 12]], image_data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"II"); // Little-endian TIFF header.
        bytes.extend_from_slice(&42u16.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes()); // First IFD starts at byte 8.

        bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for entry in entries {
            bytes.extend_from_slice(entry);
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // No next IFD.
        bytes.extend_from_slice(image_data);
        bytes
    }

    fn minimal_tiled_tiff_bytes() -> Vec<u8> {
        // image data follows immediately after the IFD block.
        let data_offset = 8 + 2 + (10 * 12) + 4;
        let entries = vec![
            ifd_entry(256, 4, 1, 1),           // ImageWidth
            ifd_entry(257, 4, 1, 1),           // ImageLength
            ifd_entry(258, 3, 1, 8),           // BitsPerSample
            ifd_entry(259, 3, 1, 1),           // Compression = none
            ifd_entry(262, 3, 1, 1),           // Photometric = BlackIsZero
            ifd_entry(277, 3, 1, 1),           // SamplesPerPixel
            ifd_entry(322, 4, 1, 1),           // TileWidth
            ifd_entry(323, 4, 1, 1),           // TileLength
            ifd_entry(324, 4, 1, data_offset), // TileOffsets
            ifd_entry(325, 4, 1, 1),           // TileByteCounts
        ];

        build_tiff(&entries, &[0u8])
    }

    fn minimal_stripped_tiff_bytes() -> Vec<u8> {
        // image data follows immediately after the IFD block.
        let data_offset = 8 + 2 + (9 * 12) + 4;
        let entries = vec![
            ifd_entry(256, 4, 1, 1),           // ImageWidth
            ifd_entry(257, 4, 1, 1),           // ImageLength
            ifd_entry(258, 3, 1, 8),           // BitsPerSample
            ifd_entry(259, 3, 1, 1),           // Compression = none
            ifd_entry(262, 3, 1, 1),           // Photometric = BlackIsZero
            ifd_entry(273, 4, 1, data_offset), // StripOffsets
            ifd_entry(277, 3, 1, 1),           // SamplesPerPixel
            ifd_entry(278, 4, 1, 1),           // RowsPerStrip
            ifd_entry(279, 4, 1, 1),           // StripByteCounts
        ];

        build_tiff(&entries, &[0u8])
    }

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

    async fn scalar_u64(dataset: &AnyDataset, name: &str) -> u64 {
        let arr = dataset
            .get_array(name)
            .unwrap_or_else(|| panic!("missing array '{name}'"));
        let nd = arr
            .as_any()
            .downcast_ref::<NdArray<u64>>()
            .unwrap_or_else(|| panic!("array '{name}' has unexpected type"));
        nd.clone_into_raw_vec().await[0]
    }

    #[tokio::test]
    async fn open_dataset_reads_tiled_tiff_from_object_store() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/minimal-tiled.tiff");

        store
            .put(&path, bytes::Bytes::from(minimal_tiled_tiff_bytes()).into())
            .await
            .expect("should write tiled TIFF bytes into object store");

        let dataset = open_dataset(object_store, path.clone())
            .await
            .expect("tiled TIFF should open successfully");

        assert_eq!(dataset.name(), path.to_string());
        assert_eq!(scalar_u32(&dataset, "image.width").await, 1);
        assert_eq!(scalar_u32(&dataset, "image.height").await, 1);
        assert_eq!(scalar_u16(&dataset, "image.samples_per_pixel").await, 1);
        assert_eq!(scalar_u32(&dataset, "image.tile_width").await, 1);
        assert_eq!(scalar_u32(&dataset, "image.tile_height").await, 1);
        assert_eq!(scalar_u64(&dataset, "image.tile_count_x").await, 1);
        assert_eq!(scalar_u64(&dataset, "image.tile_count_y").await, 1);
    }

    #[tokio::test]
    async fn open_dataset_rejects_non_tiled_tiff() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/minimal-stripped.tiff");

        store
            .put(
                &path,
                bytes::Bytes::from(minimal_stripped_tiff_bytes()).into(),
            )
            .await
            .expect("should write stripped TIFF bytes into object store");

        let err = open_dataset(object_store, path)
            .await
            .expect_err("stripped TIFF should be rejected by current reader policy");

        assert!(
            err.to_string()
                .contains("Non-tiled TIFF is not supported yet"),
            "unexpected error message: {err}"
        );
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
    async fn open_dataset_rejects_real_fixture_when_not_tiled() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/TEMP_AVG_20201201.tif");

        store
            .put(
                &path,
                bytes::Bytes::copy_from_slice(REAL_TILED_TIFF_BYTES).into(),
            )
            .await
            .expect("should write real tiled TIFF fixture into object store");

        let err = open_dataset(object_store, path)
            .await
            .expect_err("fixture should fail until non-tiled TIFFs are supported");

        assert!(
            err.to_string()
                .contains("Non-tiled TIFF is not supported yet"),
            "unexpected error message: {err}"
        );
    }
}
