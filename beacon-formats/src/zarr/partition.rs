use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::FileGroup;
use object_store::{ObjectMeta, ObjectStore};
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{pushdown_statistics::ZarrPushDownStatistics, util::ZarrPath};

pub(crate) async fn find_partitioned_files(
    group: &Group<dyn AsyncReadableListableStorageTraits>,
) -> Option<Vec<Group<dyn AsyncReadableListableStorageTraits>>> {
    match group.async_child_groups().await {
        Ok(children) => {
            // Find recursively all child groups that contain groups aswell
            let mut result = Vec::new();
            for child in children {
                if let Some(mut sub_children) = Box::pin(find_partitioned_files(&child)).await {
                    result.append(&mut sub_children);
                } else {
                    result.push(child);
                }
            }
            Some(result)
        }
        Err(_) => None,
    }
}

pub(crate) async fn partition_zarr_group(
    table_schema: SchemaRef,
    object: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
    pushdown_statistics: &ZarrPushDownStatistics,
) -> datafusion::error::Result<FileGroup> {
    // Create an Zarr AsyncObjectStore wrapper around the ObjectStore
    let zarr_store = Arc::new(AsyncObjectStore::new(object_store.clone()));

    let zarr_group_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to create ZarrPath from object metadata: {}",
            e
        ))
    })?;

    let group = Group::async_open(zarr_store, &zarr_group_path.as_zarr_path())
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open Zarr group at path '{}': {}",
                zarr_group_path.as_zarr_path(),
                e
            ))
        })?;

    

    todo!()
}
