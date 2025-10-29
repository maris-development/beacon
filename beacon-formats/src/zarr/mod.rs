use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use beacon_arrow_zarr::reader::AsyncArrowZarrGroupReader;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{ColumnStatistics, GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{
            FileGroup, FileGroupPartitioner, FileScanConfig, FileScanConfigBuilder, FileSource,
        },
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{
    array_step_span::NumericArrayStepSpan,
    pushdown_statistics::ZarrPushDownStatistics,
    source::{ZarrSource, fetch_schema},
    util::{ZarrPath, is_zarr_v3_metadata, path_parent, top_level_zarr_meta_v3},
};

pub mod array_step_span;
pub mod expr_util;
pub mod opener;
mod partition;
pub mod pushdown_statistics;
mod source;
mod stream_share;
pub mod util;

pub struct ZarrFormatFactory;

impl Debug for ZarrFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrFormatFactory").finish()
    }
}

impl ZarrFormatFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ZarrFormatFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl GetExt for ZarrFormatFactory {
    fn get_ext(&self) -> String {
        "zarr".to_string()
    }
}

impl FileFormatFactory for ZarrFormatFactory {
    fn create(
        &self,
        state: &dyn datafusion::catalog::Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn FileFormat>> {
        // Here you can use `state` and `format_options` to customize the creation of the FileFormat
        Ok(Arc::new(ZarrFormat::default()))
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        Arc::new(ZarrFormat::default())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct ZarrFormat {
    array_steps: HashMap<String, NumericArrayStepSpan>,
    zarr_pushdown_statistics: ZarrPushDownStatistics,
}

impl ZarrFormat {
    pub fn with_array_steps(mut self, array_steps: HashMap<String, NumericArrayStepSpan>) -> Self {
        self.array_steps = array_steps;
        self
    }

    pub fn with_pushdown_statistics(
        mut self,
        zarr_pushdown_statistics: ZarrPushDownStatistics,
    ) -> Self {
        self.zarr_pushdown_statistics = zarr_pushdown_statistics;
        self
    }
}

#[async_trait::async_trait]
impl FileFormat for ZarrFormat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        "zarr.json".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("zarr.json".to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        // Ensure every group is a zarr.json file.
        for object in objects {
            if !is_zarr_v3_metadata(object) {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Object at location '{}' is not a Zarr v3 metadata file (zarr.json)",
                    object.location.as_ref()
                )));
            }
        }
        // Collect only top-level zarr.json files.
        let verified_objects = top_level_zarr_meta_v3(objects);
        tracing::debug!("Top-level Zarr v3 groups: {:?}", verified_objects);
        let mut schema = None;
        for object in verified_objects {
            let zarr_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create ZarrPath from ObjectMeta at {}: {}",
                    object.location, e
                ))
            })?;
            let object_schema = fetch_schema(store.clone(), &zarr_path).await?;
            schema = Some(object_schema);
        }
        Ok(schema.unwrap())
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Recreate file groups here. Only maintain top-level groups.
        let mut object_metas: Vec<ObjectMeta> = Vec::new();
        for group in &conf.file_groups {
            for file in group.files() {
                object_metas.push(file.object_meta.clone());
            }
        }
        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;

        // Recreate file groups with only top-level Zarr groups.
        let top_level_metas = top_level_zarr_meta_v3(&object_metas);
        let mut file_groups: Vec<FileGroup> = vec![];
        for meta in top_level_metas {
            let file = self
                .partition_zarr_group(&conf.file_schema, &meta, object_store.clone())
                .await?;
            file_groups.push(file);
        }

        let source = ZarrSource::default()
            .with_array_steps(self.array_steps.clone())
            .with_pushdown_zarr_statistics(self.zarr_pushdown_statistics.clone());
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(
            ZarrSource::default()
                .with_array_steps(self.array_steps.clone())
                .with_pushdown_zarr_statistics(self.zarr_pushdown_statistics.clone()),
        )
    }
}

impl ZarrFormat {
    async fn partition_zarr_group(
        &self,
        table_schema: &SchemaRef,
        object: &ObjectMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> datafusion::error::Result<FileGroup> {
        // The object can be assumed to be a top-level zarr.json file.
        let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
            as Arc<dyn AsyncReadableListableStorageTraits>;
        let group_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from ObjectMeta at {}: {}",
                object.location, e
            ))
        })?;
        let group = Group::async_open(zarr_store, &group_path.as_zarr_path())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open Zarr group at {}: {}",
                    object.location, e
                ))
            })?;

        match Self::find_partitioned_files(&group).await {
            Some(partition_groups) => {
                if partition_groups.is_empty() {
                    // No sub-groups found, treat as single group
                    let statistics = pushdown_statistics::generate_statistics_from_zarr_group(
                        table_schema,
                        &self.zarr_pushdown_statistics,
                        Arc::new(group),
                    )
                    .await
                    .unwrap_or(Statistics::new_unknown(table_schema));

                    Ok(FileGroup::new(vec![
                        PartitionedFile::new(group_path.as_zarr_json_path(), 0)
                            .with_statistics(Arc::new(statistics)),
                    ]))
                } else {
                    // Multiple sub-groups found, treat each as a partition
                    let mut files = Vec::new();
                    for group in partition_groups {
                        let group = Arc::new(group);
                        let partition_path = group.path().to_string();
                        let statistics = pushdown_statistics::generate_statistics_from_zarr_group(
                            table_schema,
                            &self.zarr_pushdown_statistics,
                            group.clone(),
                        )
                        .await
                        .unwrap_or(Statistics::new_unknown(table_schema));

                        // Create a PartitionedFile for the sub-group
                        let partitioned_file =
                            PartitionedFile::new(format!("{}/zarr.json", partition_path), 0)
                                .with_statistics(Arc::new(statistics));
                        files.push(partitioned_file);
                    }
                    Ok(FileGroup::new(files))
                }
            }
            None => Ok(FileGroup::new(vec![])),
        }
    }

    async fn find_partitioned_files(
        group: &Group<dyn AsyncReadableListableStorageTraits>,
    ) -> Option<Vec<Group<dyn AsyncReadableListableStorageTraits>>> {
        match group.async_child_groups().await {
            Ok(children) => {
                // Find recursively all child groups that contain groups aswell
                let mut result = Vec::new();
                for child in children {
                    if let Some(mut sub_children) =
                        Box::pin(Self::find_partitioned_files(&child)).await
                    {
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
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::listing::ListingTableUrl;
    use futures::StreamExt;

    #[test]
    fn test_name() {
        let test_path = object_store::path::Path::parse("bucket/key.zarr").unwrap();

        println!("Path: {:?}", test_path);
    }

    #[tokio::test]
    async fn test_listing_table() {
        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let url = ListingTableUrl::try_new(
            "file:///".try_into().unwrap(),
            Some(glob::Pattern::new("gridded-example.zarr").unwrap()),
        )
        .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }

        let url =
            ListingTableUrl::try_new("file:///gridded-example.zarr/".try_into().unwrap(), None)
                .unwrap();
        println!("URL: {:?}", url);

        let session = datafusion::execution::context::SessionContext::new();
        let state = session.state();
        let mut files = url
            .list_all_files(&state, &object_store, "zarr.json")
            .await
            .unwrap();

        while let Some(file) = files.next().await {
            println!("File: {:?}", file);
        }
    }
}
