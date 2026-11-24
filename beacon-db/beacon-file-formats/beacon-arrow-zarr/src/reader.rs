//! High-level zarr reader producing [`AnyDataset`] values.
//!
//! [`dataset_from_group`] walks a zarr group's child arrays and attributes,
//! wraps each in a lazy [`NdArrayD`](beacon_nd_array::NdArrayD) backend, and
//! returns the result as an [`AnyDataset`]. Variable data is read on demand —
//! only metadata is touched here.
//!
//! [`schema_from_group_path`] opens each leaf group through the ND scan instead.
//!
//! # Naming conventions
//! - Each child array becomes a named column.
//! - Per-array attributes are surfaced as `"{array}.{attr}"` columns.
//! - Global group attributes are surfaced as `".{attr}"` columns.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_array::{
    NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use beacon_nd_scan::{file_schema, open_for_read};
use indexmap::IndexMap;
use object_store::{ObjectMeta, ObjectStore, memory::InMemory, path::Path};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{
    attributes::AttributeValue,
    compat,
    datafusion::opener::{ZarrOpener, scan_options},
    util::{NOMINAL_GROUP_SIZE, ZarrStorage, group_metadata_key, recursive_groups},
};

pub use beacon_nd_array::dataset::{project_read_dimensions, project_read_dimensions_or_skip};

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
/// Each leaf opens as the ND scan opens it, so the schema matches what the scan
/// reads. `_log_label` is not used: the ND open logs each skipped leaf.
///
/// [`session_widening`]: beacon_datafusion_ext::type_widening::session_widening
pub async fn schema_from_group_path(
    storage: Arc<dyn AsyncReadableListableStorageTraits>,
    group_path: &str,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
    _log_label: Option<&str>,
    widening: &ArrowTypeWidening,
) -> anyhow::Result<SchemaRef> {
    let group = Group::async_open(storage.clone(), group_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zarr group at '{group_path}': {e}"))?;

    let mut leaves = Vec::new();
    recursive_groups(Arc::new(group), &mut leaves).await?;

    let opener = ZarrOpener::new(Some(ZarrStorage::new(storage)));
    let options = scan_options(read_dimensions, skip_unbroadcastable);
    // The opener reads the storage above, so it never reads this store.
    let unused_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut schemas = Vec::new();
    let mut skipped = 0usize;
    for leaf in leaves {
        // The group path names the leaf, so a refused column names both leaves.
        let leaf_path = leaf.path().as_str().to_string();
        let meta = ObjectMeta {
            location: Path::from(group_metadata_key(&leaf_path)),
            last_modified: Default::default(),
            size: NOMINAL_GROUP_SIZE,
            e_tag: None,
            version: None,
        };
        let Some(dataset) = open_for_read(&opener, unused_store.clone(), &meta, &options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read Zarr group as dataset: {e}"))?
        else {
            skipped += 1;
            continue;
        };
        let schema = file_schema(dataset.as_ref());
        schemas.push(LabeledSchema::new(Arc::new(schema), leaf_path));
    }

    if schemas.is_empty() {
        // Every leaf was skipped: the store gives the table no column.
        if skipped > 0 {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }
        anyhow::bail!("No valid Zarr v3 groups found under '{group_path}' to infer schema");
    }
    widening
        .merge_schemas(&schemas)
        .map_err(|e| anyhow::anyhow!("Failed to compute super schema for Zarr groups: {e}"))
}

/// The verbatim JSON attributes of a group's child arrays, keyed by the column
/// name the array is surfaced under.
///
/// [`AttributeValue`] models only the scalars Beacon turns into columns, so it
/// drops a JSON array such as `actual_range`. Statistics need that value, hence
/// the untouched map.
pub type ArrayAttributes = IndexMap<String, serde_json::Map<String, serde_json::Value>>;

/// Build an [`AnyDataset`] from a zarr group.
///
/// `projected_names`:
/// - `None` — include every array and attribute.
/// - `Some(names)` — include only arrays/attributes whose column name appears
///   in `names`. This lets the DataFusion source skip building backends for
///   columns the query won't use.
pub async fn dataset_from_group(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
    projected_names: Option<&[String]>,
) -> anyhow::Result<AnyDataset> {
    dataset_and_attributes_from_group(group, projected_names)
        .await
        .map(|(dataset, _)| dataset)
}

/// [`dataset_from_group`], also returning each child array's raw JSON attributes.
///
/// One walk of the group serves both: the arrays are listed and their metadata
/// parsed exactly once, so statistics cost no extra metadata reads.
pub async fn dataset_and_attributes_from_group(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
    projected_names: Option<&[String]>,
) -> anyhow::Result<(AnyDataset, ArrayAttributes)> {
    let included =
        |name: &str| projected_names.map_or(true, |names| names.iter().any(|n| n == name));

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
    let mut array_attributes: ArrayAttributes = IndexMap::new();

    // ── Global group attributes ──────────────────────────────────────────
    for (attr_name, attr_value) in group.attributes() {
        if let Some(av) = AttributeValue::from_json_value(attr_value) {
            let key = format!(".{attr_name}");
            if included(&key) {
                match compat::attribute_to_nd_array(&av) {
                    Ok(nd) => {
                        arrays.insert(key, nd);
                    }
                    Err(e) => tracing::warn!("Skipping zarr global attribute '{key}': {e}"),
                }
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

        array_attributes.insert(array_name.clone(), array.attributes().clone());

        // Parse the array's JSON attributes once: they drive both the
        // surfaced `{array}.{attr}` columns and CF decoding of the array.
        let mut attr_map: IndexMap<String, AttributeValue> = IndexMap::new();
        for (attr_name, attr_value) in array.attributes() {
            if let Some(av) = AttributeValue::from_json_value(attr_value) {
                let key = format!("{array_name}.{attr_name}");
                if included(&key) {
                    match compat::attribute_to_nd_array(&av) {
                        Ok(nd) => {
                            arrays.insert(key, nd);
                        }
                        Err(e) => tracing::warn!("Skipping zarr attribute '{key}': {e}"),
                    }
                }
                attr_map.insert(attr_name.clone(), av);
            }
        }

        if included(&array_name) {
            match compat::array_to_nd_array(Arc::new(array), &array_name, &attr_map) {
                Ok(nd) => {
                    arrays.insert(array_name, nd);
                }
                Err(e) => tracing::warn!("Skipping zarr array '{array_name}': {e}"),
            }
        }
    }

    arrays.sort_keys();

    let dataset = Dataset::new(group_path, arrays).await;
    let dataset = AnyDataset::try_from_dataset(dataset)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to wrap zarr group as AnyDataset: {e}"))?;
    Ok((dataset, array_attributes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use beacon_nd_dataset::{NdDataset, Region};
    use object_store::local::LocalFileSystem;
    use zarrs_object_store::AsyncObjectStore;

    use crate::nd::open_group;

    #[tokio::test]
    async fn lat_values_are_readable() {
        let local = LocalFileSystem::new_with_prefix("./test_files").unwrap();
        let storage = Arc::new(AsyncObjectStore::new(local));
        let dataset = open_group(storage, "/gridded-example.zarr").await.unwrap();
        let lat = dataset.variable("lat").expect("lat array");
        let values = lat.read(&Region::full(lat.dimensions())).await.unwrap();
        assert_eq!(values.values().data_type(), &DataType::Float32);
        assert!(!values.values().is_empty());
    }
}
