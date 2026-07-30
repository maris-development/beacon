//! High-level atlas reader that produces [`AnyDataset`] values.
//!
//! An atlas store holds one or more named datasets. [`dataset_from_atlas`]
//! returns the contents of **one** atlas dataset as a Beacon [`AnyDataset`],
//! wrapping every array in a lazy [`NdArrayD`](beacon_nd_array::NdArrayD)
//! backend and every scalar attribute in a rank-0 backend. Array *data* is read
//! on demand — only metadata and (cheap) attribute values are touched here.
//!
//! # Column naming
//! - Each array becomes a column under its own name.
//! - Dataset-level (global) attributes become rank-0 columns under their bare
//!   attribute name.
//! - Per-array attributes become rank-0 columns named `{array}.{attr}`.

use std::sync::Arc;

use beacon_nd_array::{
    NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use indexmap::IndexMap;
use object_store::{ObjectStore, path::Path as OsPath};

use crate::compat;

/// Open an atlas store over `store` rooted at `prefix` (the directory holding
/// the `atlas.json` marker) and read the named dataset into an [`AnyDataset`].
///
/// A convenience for callers holding an object store; the DataFusion code path
/// opens the [`Atlas`](atlas::Atlas) handle once per store and calls
/// [`dataset_from_atlas`] per dataset directly.
pub async fn open_dataset(
    store: Arc<dyn ObjectStore>,
    prefix: OsPath,
    dataset_name: &str,
) -> anyhow::Result<AnyDataset> {
    let atlas = atlas::Atlas::open(store, prefix.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open atlas store at {:?}: {}", prefix, e))?;
    dataset_from_atlas(Arc::new(atlas), dataset_name, None).await
}

/// Build an [`AnyDataset`] from an already-open atlas handle.
///
/// `projected_names`:
/// - `None` — include every array and attribute in the dataset.
/// - `Some(names)` — include only arrays/attributes whose column name appears
///   in `names`. Names not present in the dataset are silently ignored. This
///   lets the DataFusion source skip building backends for columns the query
///   won't use.
pub async fn dataset_from_atlas(
    atlas: Arc<atlas::Atlas>,
    dataset_name: &str,
    projected_names: Option<&[String]>,
) -> anyhow::Result<AnyDataset> {
    let view = atlas
        .open_dataset(dataset_name)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open atlas dataset '{}': {}", dataset_name, e))?;

    let included =
        |name: &str| projected_names.map_or(true, |names| names.iter().any(|n| n == name));

    // The dataset's schema is in-memory metadata — array names/types and the
    // attribute-key namespace — so we can decide what's projected *before*
    // touching any `.af` file. Attribute values (`get_attribute` /
    // `get_array_attribute`) and fill values are only read for columns the
    // projection actually keeps: a column-subset query over a wide dataset
    // never pays for the attributes it didn't ask for.
    let schema = view.schema();
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Arrays and their per-array attributes ────────────────────────────
    for (array_name, array_schema) in &schema.arrays {
        // Per-array attributes ride alongside the array as `{array}.{attr}`
        // columns, independent of whether the array data itself is projected.
        if let Some(attr_keys) = schema.array_attrs.get(array_name) {
            for attr_key in attr_keys.keys() {
                let key = format!("{array_name}.{attr_key}");
                if !included(&key) {
                    continue;
                }
                let Some(attr_value) =
                    view.get_array_attribute(array_name, attr_key).await.map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read attribute '{key}' in atlas dataset '{dataset_name}': {e}"
                        )
                    })?
                else {
                    continue; // key declared in the namespace but unset here
                };
                match compat::attribute_to_nd_array(&attr_value) {
                    Ok(nd) => {
                        arrays.insert(key, nd);
                    }
                    Err(e) => tracing::warn!(
                        "Skipping atlas array attribute '{key}' in dataset '{dataset_name}': {e}"
                    ),
                }
            }
        }

        if !included(array_name) {
            continue;
        }
        let fill_value = view.array_fill_value(array_name).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to read fill value for atlas array '{}' in dataset '{}': {}",
                array_name,
                dataset_name,
                e
            )
        })?;
        match compat::array_to_nd_array(atlas.clone(), dataset_name, array_name, array_schema, fill_value)
        {
            Ok(nd) => {
                arrays.insert(array_name.clone(), nd);
            }
            Err(e) => {
                tracing::warn!("Skipping atlas array '{array_name}' in dataset '{dataset_name}': {e}")
            }
        }
    }

    // ── Dataset-level (global) attributes ────────────────────────────────
    for attr_key in schema.global_attrs.keys() {
        if !included(attr_key) {
            continue;
        }
        let Some(attr_value) = view.get_attribute(attr_key).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to read global attribute '{attr_key}' in atlas dataset '{dataset_name}': {e}"
            )
        })?
        else {
            continue;
        };
        match compat::attribute_to_nd_array(&attr_value) {
            Ok(nd) => {
                arrays.insert(attr_key.clone(), nd);
            }
            Err(e) => tracing::warn!(
                "Skipping atlas global attribute '{attr_key}' in dataset '{dataset_name}': {e}"
            ),
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
    ///   global attributes `season: String("winter")`, `year: Int64(2024)`.
    /// - `summer`: array `temperature: Float32[3]`;
    ///   global attribute `season: String("summer")`.
    pub async fn build_two_dataset_store(path: &Path) {
        build_two_dataset_store_with_config(path, StoreConfig::default()).await;
    }

    /// Same as [`build_two_dataset_store`] but lets the caller pick the
    /// metadata format / compression so tests can exercise non-default atlas
    /// marker filenames (e.g. `atlas.msgpack.zst`).
    pub async fn build_two_dataset_store_with_config(path: &Path, config: StoreConfig) {
        let mut atlas = Atlas::create_path(path, config)
            .await
            .expect("create atlas store");

        // ── winter ────────────────────────────────────────────────────
        {
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
            winter
                .set_attribute("season", Attr::String("winter".into()))
                .expect("set winter.season");
            winter
                .set_attribute("year", Attr::Int64(2024))
                .expect("set winter.year");

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
        }

        // ── summer ────────────────────────────────────────────────────
        {
            let mut summer = atlas.create_dataset("summer").await.expect("create summer");
            summer
                .define_array::<f32>("temperature", vec!["obs".into()], vec![3], None, None)
                .await
                .expect("define summer.temperature");
            summer
                .set_attribute("season", Attr::String("summer".into()))
                .expect("set summer.season");

            let temps = ndarray::arr1(&[20.0f32, 21.0, 22.0]).into_dyn();
            summer
                .write_array("temperature", vec![0], temps.view())
                .await
                .expect("write summer.temperature");
        }

        // Persist the metadata marker + array files to the store.
        atlas.flush().await.expect("flush atlas store");
    }

    /// Build a store where the same array name has *different* dtypes across
    /// datasets, so the collection's merged (table) type widens past either
    /// dataset's own type:
    /// - `a`: `value: Int16[2]  = [1, 2]`
    /// - `b`: `value: Float32[2] = [3.5, 4.5]`
    ///
    /// Merged `value` widens to `Float64`; each dataset must be read at its own
    /// dtype and cast up. `a` also carries `flag: Int32[2]` that `b` lacks, to
    /// exercise null-filling the missing column.
    pub async fn build_widening_store(path: &Path) {
        let mut atlas = Atlas::create_path(path, StoreConfig::default())
            .await
            .expect("create atlas store");
        {
            let mut a = atlas.create_dataset("a").await.expect("create a");
            a.define_array::<i16>("value", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define a.value");
            a.define_array::<i32>("flag", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define a.flag");
            a.write_array("value", vec![0], ndarray::arr1(&[1i16, 2]).into_dyn().view())
                .await
                .expect("write a.value");
            a.write_array("flag", vec![0], ndarray::arr1(&[7i32, 8]).into_dyn().view())
                .await
                .expect("write a.flag");
        }
        {
            let mut b = atlas.create_dataset("b").await.expect("create b");
            b.define_array::<f32>("value", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define b.value");
            b.write_array("value", vec![0], ndarray::arr1(&[3.5f32, 4.5]).into_dyn().view())
                .await
                .expect("write b.value");
        }
        atlas.flush().await.expect("flush atlas store");
    }

    /// Build a store whose two datasets give the *same* array genuinely
    /// incompatible dtypes:
    ///
    /// - `a`: `value: String[2] = ["x", "y"]`
    /// - `b`: `value: Int64[2]  = [1, 2]`
    ///
    /// Unlike [`build_widening_store`], there is no numeric super-type here, so
    /// this pins what the merged schema resolves to and whether a scan can still
    /// read both datasets. `a` also carries `only_a: Int32[2]`, so the
    /// "dataset declares none of the projected columns" path can be exercised by
    /// projecting just that column.
    pub async fn build_incompatible_store(path: &Path) {
        let mut atlas = Atlas::create_path(path, StoreConfig::default())
            .await
            .expect("create atlas store");
        {
            let mut a = atlas.create_dataset("a").await.expect("create a");
            a.define_array::<String>("value", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define a.value");
            a.define_array::<i32>("only_a", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define a.only_a");
            a.write_array(
                "value",
                vec![0],
                ndarray::arr1(&["x".to_string(), "y".to_string()])
                    .into_dyn()
                    .view(),
            )
            .await
            .expect("write a.value");
            a.write_array("only_a", vec![0], ndarray::arr1(&[7i32, 8]).into_dyn().view())
                .await
                .expect("write a.only_a");
        }
        {
            let mut b = atlas.create_dataset("b").await.expect("create b");
            b.define_array::<i64>("value", vec!["obs".into()], vec![2], None, None)
                .await
                .expect("define b.value");
            b.write_array("value", vec![0], ndarray::arr1(&[1i64, 2]).into_dyn().view())
                .await
                .expect("write b.value");
        }
        atlas.flush().await.expect("flush atlas store");
    }

    /// Build a store of `n` datasets each holding `temperature: Float32[4]`,
    /// where dataset `i` covers the disjoint range `[10*i, 10*i + 3]`. A
    /// predicate like `temperature > T` then matches only the datasets whose
    /// range reaches past `T`, so pruning can be checked against a known answer.
    pub async fn build_ranged_store(path: &Path, n: usize) {
        let mut atlas = Atlas::create_path(path, StoreConfig::default())
            .await
            .expect("create atlas store");
        for i in 0..n {
            let mut ds = atlas
                .create_dataset(&format!("d{i}"))
                .await
                .expect("create dataset");
            ds.define_array::<f32>("temperature", vec!["obs".into()], vec![4], None, None)
                .await
                .expect("define temperature");
            let base = (10 * i) as f32;
            let data = ndarray::arr1(&[base, base + 1.0, base + 2.0, base + 3.0]).into_dyn();
            ds.write_array("temperature", vec![0], data.view())
                .await
                .expect("write temperature");
        }
        atlas.flush().await.expect("flush atlas store");
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::build_two_dataset_store;
    use super::*;
    use beacon_nd_array::NdArray;
    use object_store::local::LocalFileSystem;

    /// Open a fixture dataset via the object-store-native path.
    async fn open_fixture_dataset(dir: &std::path::Path, dataset: &str) -> AnyDataset {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        open_dataset(store, OsPath::from(""), dataset)
            .await
            .expect("open dataset")
    }

    #[tokio::test]
    async fn open_dataset_lists_arrays_and_attributes() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_fixture_dataset(tmp.path(), "winter").await;
        assert_eq!(winter.name(), "winter");

        let ds = winter.dataset();
        let names: Vec<&str> = ds.arrays.keys().map(|s| s.as_str()).collect();
        assert!(names.contains(&"temperature"), "{names:?}");
        assert!(names.contains(&"cycle"), "{names:?}");
        assert!(names.contains(&"season"), "{names:?}");
        assert!(names.contains(&"year"), "{names:?}");
    }

    #[tokio::test]
    async fn open_dataset_reads_array_values() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_fixture_dataset(tmp.path(), "winter").await;
        let temp = winter
            .get_array("temperature")
            .expect("temperature array")
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("downcast f32");
        assert_eq!(temp.clone_into_raw_vec().await, vec![1.0f32, 2.0, 3.0, 4.0]);

        let cycle = winter
            .get_array("cycle")
            .expect("cycle array")
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .expect("downcast i32");
        assert_eq!(cycle.clone_into_raw_vec().await, vec![10i32, 20, 30, 40]);
    }

    #[tokio::test]
    async fn open_dataset_reads_attributes_as_rank_zero() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_fixture_dataset(tmp.path(), "winter").await;
        let season = winter
            .get_array("season")
            .expect("season attribute")
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .expect("downcast string");
        assert!(season.shape().is_empty(), "attribute should be rank-0");
        assert_eq!(season.clone_into_raw_vec().await, vec!["winter".to_string()]);

        let year = winter
            .get_array("year")
            .expect("year attribute")
            .as_any()
            .downcast_ref::<NdArray<i64>>()
            .expect("downcast i64");
        assert_eq!(year.clone_into_raw_vec().await, vec![2024i64]);
    }

    #[tokio::test]
    async fn open_dataset_propagates_array_fill_value() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let winter = open_fixture_dataset(tmp.path(), "winter").await;
        let cycle = winter
            .get_array("cycle")
            .expect("cycle array")
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .expect("downcast i32");
        assert_eq!(cycle.fill_value().await, Some(-1i32));

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

        let winter = open_fixture_dataset(tmp.path(), "winter").await;
        let summer = open_fixture_dataset(tmp.path(), "summer").await;

        assert_eq!(
            winter.dataset().get_array("temperature").unwrap().shape(),
            &[4]
        );
        assert_eq!(
            summer.dataset().get_array("temperature").unwrap().shape(),
            &[3]
        );
        assert!(summer.dataset().get_array("cycle").is_none());
        assert!(summer.dataset().get_array("year").is_none());
    }

    #[tokio::test]
    async fn open_dataset_unknown_returns_error() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
        let err = open_dataset(store, OsPath::from(""), "ghost")
            .await
            .expect_err("should fail for unknown dataset");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("ghost") || msg.contains("DatasetNotFound"),
            "error should mention missing dataset name: {msg}"
        );
    }
}
