//! High-level zarr reader that gives a [`Dataset`] for each group.
//!
//! [`dataset_from_group`] walks the child arrays and attributes of a zarr group.
//! It wraps each one in a lazy [`NdArrayD`] backend. Variable data is read on demand.
//! This module reads only metadata.
//!
//! # Naming conventions
//! - Each child array becomes a named column.
//! - Per-array attributes are surfaced as `"{array}.{attr}"` columns.
//! - Global group attributes are surfaced as `".{attr}"` columns.

use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_array::{NdArrayD, dataset_v2::Dataset};
use indexmap::IndexMap;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{attributes::AttributeValue, compat, util::recursive_groups};

/// The merged Arrow schema of every leaf group under `group_path`.
///
/// The function is storage independent. It reads an object store and an Icechunk
/// repository session in the same way.
///
/// `widening` decides the result for a column that two leaf groups describe
/// differently. The caller passes the rule of the session. See
/// [`session_widening`]. One store then merges by the rule that the stores of a
/// table and the files of every other format use.
///
/// [`session_widening`]: beacon_datafusion_ext::type_widening::session_widening
pub async fn schema_from_group_path(
    storage: Arc<dyn AsyncReadableListableStorageTraits>,
    group_path: &str,
    widening: &ArrowTypeWidening,
) -> anyhow::Result<SchemaRef> {
    let group = Group::async_open(storage, group_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zarr group at '{group_path}': {e}"))?;

    let mut leaves = Vec::new();
    recursive_groups(Arc::new(group), &mut leaves).await?;

    let mut schemas = Vec::new();
    for leaf in leaves {
        // The group path names the leaf, so a refused column names both leaves.
        let leaf_path = leaf.path().as_str().to_string();
        let dataset = dataset_from_group(&leaf)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read Zarr group as dataset: {e}"))?;
        schemas.push(LabeledSchema::new(
            Arc::new(dataset.arrow_schema()),
            leaf_path,
        ));
    }

    if schemas.is_empty() {
        anyhow::bail!("No valid Zarr v3 groups found under '{group_path}' to infer schema");
    }
    widening
        .merge_schemas(&schemas)
        .map_err(|e| anyhow::anyhow!("Failed to compute super schema for Zarr groups: {e}"))
}

/// Open the group at `group_path` in `storage` as a [`Dataset`].
///
/// # Errors
///
/// Returns an error when the group does not open, or when its arrays disagree about a dimension size.
pub async fn open_dataset(
    storage: Arc<dyn AsyncReadableListableStorageTraits>,
    group_path: &str,
) -> anyhow::Result<Dataset> {
    let group = Group::async_open(storage, group_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zarr group at '{group_path}': {e}"))?;
    dataset_from_group(&group).await
}

/// Build a [`Dataset`] from a zarr group, with every array and attribute.
///
/// # Errors
///
/// Returns an error when the child arrays do not list, or when they disagree about a dimension size.
pub async fn dataset_from_group(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
) -> anyhow::Result<Dataset> {
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Global group attributes ──────────────────────────────────────────
    for (attr_name, attr_value) in group.attributes() {
        if let Some(av) = AttributeValue::from_json_value(attr_value) {
            let key = format!(".{attr_name}");
            match compat::attribute_to_nd_array(&av) {
                Ok(nd) => {
                    arrays.insert(key, nd);
                }
                Err(e) => tracing::warn!("Skipping zarr global attribute '{key}': {e}"),
            }
        }
    }

    let group_path = group.path().as_str().to_string();

    // ── Child arrays and their per-array attributes ──────────────────────
    let child_arrays = group
        .async_child_arrays()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list zarr child arrays: {e}"))?;

    for array in child_arrays {
        let array_node_path = array.path().as_str().to_string();
        let array_name = array_node_path
            .strip_prefix(&group_path)
            .unwrap_or(&array_node_path);
        let array_name = array_name
            .strip_prefix('/')
            .unwrap_or(array_name)
            .to_string();

        // Parse the array's JSON attributes once: they drive both the
        // surfaced `{array}.{attr}` columns and CF decoding of the array.
        let mut attr_map: IndexMap<String, AttributeValue> = IndexMap::new();
        for (attr_name, attr_value) in array.attributes() {
            if let Some(av) = AttributeValue::from_json_value(attr_value) {
                let key = format!("{array_name}.{attr_name}");
                match compat::attribute_to_nd_array(&av) {
                    Ok(nd) => {
                        arrays.insert(key, nd);
                    }
                    Err(e) => tracing::warn!("Skipping zarr attribute '{key}': {e}"),
                }
                attr_map.insert(attr_name.clone(), av);
            }
        }

        match compat::array_to_nd_array(Arc::new(array), &array_name, &attr_map) {
            Ok(nd) => {
                arrays.insert(array_name, nd);
            }
            Err(e) => tracing::warn!("Skipping zarr array '{array_name}': {e}"),
        }
    }

    arrays.sort_keys();

    // A zarr group names every dimension, so the reader invents none.
    let dataset = Dataset::from_arrays(group_path, arrays, HashSet::new())?;
    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_nd_array::{NdArray, datatypes::NdArrayDataType};
    use object_store::local::LocalFileSystem;
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::AsyncReadableListableStorage;

    use super::*;

    async fn open_example() -> Dataset {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();
        let zarr_store: AsyncReadableListableStorage = Arc::new(AsyncObjectStore::new(local_fs));
        open_dataset(zarr_store, "/gridded-example.zarr")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn dataset_contains_coordinate_and_data_arrays() {
        let dataset = open_example().await;
        assert!(dataset.arrays.contains_key("lat"));
        assert!(dataset.arrays.contains_key("lon"));
        assert!(dataset.arrays.contains_key("analysed_sst"));
        assert!(dataset.invented_dimensions.is_empty());
    }

    #[tokio::test]
    async fn time_coordinate_decodes_to_timestamp() {
        let dataset = open_example().await;
        let time = dataset.arrays.get("time").expect("time array");
        assert_eq!(time.datatype(), NdArrayDataType::Timestamp);
    }

    #[tokio::test]
    async fn lat_values_are_readable() {
        let dataset = open_example().await;
        let lat = dataset
            .arrays
            .get("lat")
            .expect("lat array")
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("lat is f32");
        let raw = lat.clone_into_raw_vec().await;
        assert!(!raw.is_empty());
    }

    #[tokio::test]
    async fn the_schema_holds_arrays_and_attributes() {
        let schema = open_example().await.arrow_schema();

        assert!(schema.field_with_name("analysed_sst").is_ok());
        assert!(
            schema
                .fields()
                .iter()
                .any(|f| f.name().starts_with("analysed_sst."))
        );
    }
}
