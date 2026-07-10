//! High-level zarr reader producing [`AnyDataset`] values.
//!
//! [`dataset_from_group`] walks a zarr group's child arrays and attributes,
//! wraps each in a lazy [`NdArrayD`](beacon_nd_array::NdArrayD) backend, and
//! returns the result as an [`AnyDataset`]. Variable data is read on demand —
//! only metadata is touched here.
//!
//! # Naming conventions
//! - Each child array becomes a named column.
//! - Per-array attributes are surfaced as `"{array}.{attr}"` columns.
//! - Global group attributes are surfaced as `".{attr}"` columns.

use std::sync::Arc;

use beacon_nd_array::{
    NdArrayD,
    dataset::{AnyDataset, Dataset},
};
use indexmap::IndexMap;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{attributes::AttributeValue, compat};

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
    let included =
        |name: &str| projected_names.map_or(true, |names| names.iter().any(|n| n == name));

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

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
    AnyDataset::try_from_dataset(dataset)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to wrap zarr group as AnyDataset: {e}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_nd_array::{NdArray, datatypes::NdArrayDataType};
    use object_store::local::LocalFileSystem;
    use zarrs::group::Group;
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::AsyncReadableListableStorage;

    use super::*;

    async fn open_example() -> AnyDataset {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();
        let object_store = AsyncObjectStore::new(local_fs);
        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);
        let group = Group::async_open(zarr_store, "/gridded-example.zarr")
            .await
            .unwrap();
        dataset_from_group(&group, None).await.unwrap()
    }

    #[tokio::test]
    async fn dataset_contains_coordinate_and_data_arrays() {
        let any = open_example().await;
        assert!(any.get_array("lat").is_some());
        assert!(any.get_array("lon").is_some());
        assert!(any.get_array("analysed_sst").is_some());
    }

    #[tokio::test]
    async fn time_coordinate_decodes_to_timestamp() {
        let any = open_example().await;
        let time = any.get_array("time").expect("time array");
        assert_eq!(time.datatype(), NdArrayDataType::Timestamp);
    }

    #[tokio::test]
    async fn lat_values_are_readable() {
        let any = open_example().await;
        let lat = any
            .get_array("lat")
            .expect("lat array")
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .expect("lat is f32");
        let raw = lat.clone_into_raw_vec().await;
        assert!(!raw.is_empty());
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_chunks() {
        use beacon_nd_array::arrow::{
            batch::any_dataset_as_record_batch_stream, pushdown_filter::PushdownFilter,
            schema::any_dataset_to_arrow_schema,
        };
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{binary, col, lit};
        use datafusion::scalar::ScalarValue;
        use futures::TryStreamExt;
        use std::sync::Arc;

        async fn row_count(predicate: Option<PushdownFilter>) -> usize {
            let any = open_example().await;
            any_dataset_as_record_batch_stream(any, usize::MAX, predicate, None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum()
        }

        // Determine the latitude range from the data.
        let any = open_example().await;
        let lat_vals = any
            .get_array("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .unwrap()
            .clone_into_raw_vec()
            .await;
        let lat_min = lat_vals.iter().cloned().fold(f32::INFINITY, f32::min);
        let lat_max = lat_vals.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let schema = Arc::new(any_dataset_to_arrow_schema(&any).unwrap());

        let total = row_count(None).await;
        assert!(total > 0, "baseline scan should produce rows");

        // An impossible predicate (lat above the max) must prune every chunk.
        let impossible = binary(
            col("lat", &schema).unwrap(),
            Operator::Gt,
            lit(ScalarValue::Float32(Some(lat_max + 1000.0))),
            &schema,
        )
        .unwrap();
        assert_eq!(
            row_count(Some(PushdownFilter::new(impossible))).await,
            0,
            "an impossible lat predicate should prune all chunks"
        );

        // A permissive predicate (lat >= min) must keep every row.
        let permissive = binary(
            col("lat", &schema).unwrap(),
            Operator::GtEq,
            lit(ScalarValue::Float32(Some(lat_min))),
            &schema,
        )
        .unwrap();
        assert_eq!(
            row_count(Some(PushdownFilter::new(permissive))).await,
            total,
            "a permissive lat predicate should not prune any rows"
        );
    }
}
