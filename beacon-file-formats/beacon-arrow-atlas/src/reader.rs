//! High-level atlas reader that produces [`AnyDataset`] values.
//!
//! Each atlas store may hold multiple named datasets. [`open_dataset`]
//! returns the contents of **one** atlas dataset as a Beacon
//! [`AnyDataset`], wrapping every array and attribute in a lazy
//! [`NdArrayD`](beacon_nd_array::NdArrayD) backend.

use std::{path::Path, sync::Arc};

use atlas::Atlas;
use beacon_nd_array::{
    NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use indexmap::IndexMap;

use crate::compat;

/// Open an atlas store on the local filesystem and read the named
/// dataset into an [`AnyDataset`].
///
/// This is a convenience for tests and standalone callers; the
/// DataFusion code path opens the [`Atlas`] handle separately and calls
/// [`dataset_from_atlas`] directly.
pub async fn open_dataset<P: AsRef<Path>>(
    store_path: P,
    dataset_name: &str,
) -> anyhow::Result<AnyDataset> {
    let atlas = Atlas::open_path(store_path.as_ref()).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to open atlas store at {:?}: {}",
            store_path.as_ref(),
            e
        )
    })?;
    dataset_from_atlas(Arc::new(atlas), dataset_name, None).await
}

/// Build an [`AnyDataset`] from an already-open atlas handle.
///
/// `projected_names`:
/// - `None` — include every array and attribute in the dataset.
/// - `Some(names)` — include only arrays/attributes whose name appears in
///   `names`. Names not present in the dataset are silently ignored.
///   This lets the DataFusion source skip building `NdArrayD` backends
///   for columns the query won't use.
pub async fn dataset_from_atlas(
    atlas: Arc<Atlas>,
    dataset_name: &str,
    projected_names: Option<&[String]>,
) -> anyhow::Result<AnyDataset> {
    let view = atlas
        .open_dataset(dataset_name)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open atlas dataset '{}': {}", dataset_name, e))?;

    let included =
        |name: &str| projected_names.map_or(true, |names| names.iter().any(|n| n == name));

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    for array_name in view.list_arrays() {
        if !included(&array_name) {
            continue;
        }
        let schema = view.array_meta(&array_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Atlas schema missing for array '{}' in dataset '{}'",
                array_name,
                dataset_name,
            )
        })?;
        let fill_value = view.array_fill_value(&array_name).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to read fill value for atlas array '{}' in dataset '{}': {}",
                array_name,
                dataset_name,
                e
            )
        })?;
        match compat::array_to_nd_array(
            atlas.clone(),
            dataset_name,
            &array_name,
            &schema,
            fill_value,
        ) {
            Ok(nd) => {
                arrays.insert(array_name, nd);
            }
            Err(e) => {
                tracing::warn!(
                    "Skipping atlas array '{}' in dataset '{}': {}",
                    array_name,
                    dataset_name,
                    e
                );
            }
        }
    }

    for (attr_name, attr_value) in view.meta().attributes {
        if !included(&attr_name) {
            continue;
        }
        match compat::attribute_to_nd_array(&attr_name, attr_value) {
            Ok(nd) => {
                arrays.insert(attr_name, nd);
            }
            Err(e) => {
                tracing::warn!(
                    "Skipping atlas attribute '{}' in dataset '{}': {}",
                    attr_name,
                    dataset_name,
                    e
                );
            }
        }
    }

    arrays.sort_keys();

    let dataset = Dataset::new(dataset_name.to_string(), arrays).await;
    AnyDataset::try_from_dataset(dataset)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to wrap atlas dataset as AnyDataset: {}", e))
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Helpers for building atlas store fixtures in tests across the crate.

    use atlas::{Atlas, Attr, FillValue, StoreConfig};
    use std::path::Path;

    /// Build a two-dataset atlas store at `path`.
    ///
    /// Layout:
    /// - `winter`: arrays `temperature: Float32[4]`, `cycle: Int32[4]`
    ///   (fill_value = -1, lets us assert fill propagation end-to-end);
    ///   attribute `season: String("winter")`, `year: Int64(2024)`.
    /// - `summer`: arrays `temperature: Float32[3]`;
    ///   attribute `season: String("summer")`.
    pub async fn build_two_dataset_store(path: &Path) {
        build_two_dataset_store_with_config(path, StoreConfig::default()).await;
    }

    /// Same as [`build_two_dataset_store`] but lets the caller pick the
    /// metadata format / compression so tests can exercise non-default
    /// atlas marker filenames (e.g. `atlas.msgpack.zst`).
    pub async fn build_two_dataset_store_with_config(path: &Path, config: StoreConfig) {
        let mut atlas = Atlas::create_path(path, config)
            .await
            .expect("create atlas store");

        // ── winter ────────────────────────────────────────────────────
        let mut winter = atlas.create_dataset("winter").await.expect("create winter");
        winter
            .define_array::<f32>("temperature", vec!["obs".into()], vec![4], None, None)
            .await
            .expect("define winter.temperature");
        winter
            .define_array::<i32>(
                "cycle",
                vec!["obs".into()],
                vec![4],
                None,
                Some(FillValue::Int(-1)),
            )
            .await
            .expect("define winter.cycle");
        winter.set_attribute("season", Attr::String("winter".into()));
        winter.set_attribute("year", Attr::Int64(2024));

        let temps = ndarray::arr1(&[1.0f32, 2.0, 3.0, 4.0]).into_dyn();
        winter
            .write_array("temperature", vec![0], temps.view())
            .await
            .expect("write winter.temperature");
        let cycles = ndarray::arr1(&[10i32, 20, 30, 40]).into_dyn();
        winter
            .write_array("cycle", vec![0], cycles.view())
            .await
            .expect("write winter.cycle");

        // ── summer ────────────────────────────────────────────────────
        let mut summer = atlas.create_dataset("summer").await.expect("create summer");
        summer
            .define_array::<f32>("temperature", vec!["obs".into()], vec![3], None, None)
            .await
            .expect("define summer.temperature");
        summer.set_attribute("season", Attr::String("summer".into()));

        let temps = ndarray::arr1(&[20.0f32, 21.0, 22.0]).into_dyn();
        summer
            .write_array("temperature", vec![0], temps.view())
            .await
            .expect("write summer.temperature");

        // Persist `atlas.json` + array files to disk.
        atlas.flush().await.expect("flush atlas store");
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::build_two_dataset_store;
    use super::*;
    use beacon_nd_array::NdArray;

    #[tokio::test]
    async fn open_dataset_lists_arrays_and_attributes() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_dataset(tmp.path(), "winter")
            .await
            .expect("open winter");
        assert_eq!(winter.name(), "winter");

        let ds = winter.dataset();
        let names: Vec<&str> = ds.arrays.keys().map(|s| s.as_str()).collect();
        // Sorted insertion: season, temperature, year, cycle - then sorted to:
        // cycle, season, temperature, year
        assert!(names.contains(&"temperature"));
        assert!(names.contains(&"cycle"));
        assert!(names.contains(&"season"));
        assert!(names.contains(&"year"));
    }

    #[tokio::test]
    async fn open_dataset_reads_array_values() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_dataset(tmp.path(), "winter")
            .await
            .expect("open winter");
        let temp = winter
            .get_array("temperature")
            .expect("temperature array")
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("downcast f32");
        let raw = temp.clone_into_raw_vec().await;
        assert_eq!(raw, vec![1.0f32, 2.0, 3.0, 4.0]);

        let cycle = winter
            .get_array("cycle")
            .expect("cycle array")
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .expect("downcast i32");
        let raw = cycle.clone_into_raw_vec().await;
        assert_eq!(raw, vec![10i32, 20, 30, 40]);
    }

    #[tokio::test]
    async fn open_dataset_reads_attributes_as_rank_zero() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_dataset(tmp.path(), "winter")
            .await
            .expect("open winter");
        let season = winter
            .get_array("season")
            .expect("season attribute")
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .expect("downcast string");
        assert!(season.shape().is_empty(), "attribute should be rank-0");
        let raw = season.clone_into_raw_vec().await;
        assert_eq!(raw, vec!["winter".to_string()]);

        let year = winter
            .get_array("year")
            .expect("year attribute")
            .as_any()
            .downcast_ref::<NdArray<i64>>()
            .expect("downcast i64");
        let raw = year.clone_into_raw_vec().await;
        assert_eq!(raw, vec![2024i64]);
    }

    #[tokio::test]
    async fn open_dataset_propagates_array_fill_value() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_dataset(tmp.path(), "winter")
            .await
            .expect("open winter");
        let cycle = winter
            .get_array("cycle")
            .expect("cycle array")
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .expect("downcast i32");
        assert_eq!(cycle.fill_value().await, Some(-1i32));

        // temperature was defined without a fill value.
        let temperature = winter
            .get_array("temperature")
            .expect("temperature array")
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("downcast f32");
        assert_eq!(temperature.fill_value().await, None);
    }

    #[tokio::test]
    async fn open_dataset_distinguishes_between_dataset_views() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_dataset(tmp.path(), "winter")
            .await
            .expect("open winter");
        let summer = open_dataset(tmp.path(), "summer")
            .await
            .expect("open summer");

        assert_eq!(
            winter.dataset().get_array("temperature").unwrap().shape(),
            &[4]
        );
        assert_eq!(
            summer.dataset().get_array("temperature").unwrap().shape(),
            &[3]
        );
        // summer doesn't define `cycle` or `year`.
        assert!(summer.dataset().get_array("cycle").is_none());
        assert!(summer.dataset().get_array("year").is_none());
    }

    #[tokio::test]
    async fn open_dataset_unknown_returns_error() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let err = open_dataset(tmp.path(), "ghost")
            .await
            .expect_err("should fail for unknown dataset");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("ghost") || msg.contains("DatasetNotFound"),
            "error should mention missing dataset name: {msg}"
        );
    }

    #[tokio::test]
    async fn open_dataset_missing_store_path_errors() {
        let tmp = tempfile::tempdir().expect("temp dir");
        // No store created — atlas.json absent.
        let result = open_dataset(tmp.path(), "winter").await;
        assert!(result.is_err());
    }
}
