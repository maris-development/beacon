//! Schema inference for a Zarr store.
//!
//! [`schema_from_group_path`] opens each leaf group through the ND scan.
//!
//! # Naming conventions
//! - Each child array becomes a named column.
//! - Per-array attributes are surfaced as `"{array}.{attr}"` columns.
//! - Global group attributes are surfaced as `".{attr}"` columns.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_scan::file_schema;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{datafusion::opener::open_leaf, util::recursive_groups};

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

    let mut schemas = Vec::new();
    let mut skipped = 0usize;
    for leaf in leaves {
        // The group path names the leaf, so a refused column names both leaves.
        let leaf_path = leaf.path().as_str().to_string();
        let Some(dataset) = open_leaf(
            storage.clone(),
            &leaf_path,
            read_dimensions.clone(),
            skip_unbroadcastable,
        )
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
