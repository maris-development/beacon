//! DataFusion integration for zarr stores.
//!
//! Mirrors the netcdf/tiff/atlas crates: a [`ZarrFormatFactory`] discovers
//! zarr stores, [`ZarrFormat`] infers the (super-typed) Arrow schema and plans
//! the scan, and [`ZarrSource`] streams each leaf group through the shared
//! `beacon-nd-array` engine with predicate pushdown.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{
    reader::dataset_from_group,
    util::{
        ZarrPath, find_partitioned_files, is_zarr_v3_metadata, recursive_groups,
        top_level_zarr_meta_v3,
    },
};

pub mod source;

pub use source::ZarrSource;

// ─── Factory ─────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ZarrFormatFactory;

impl std::fmt::Debug for ZarrFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrFormatFactory").finish()
    }
}

impl ZarrFormatFactory {
    pub fn new() -> Self {
        Self
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
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ZarrFormat::default()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ZarrFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for ZarrFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets: Vec<ObjectMeta> = objects
            .iter()
            .filter(|obj| is_zarr_v3_metadata(obj))
            .cloned()
            .collect();

        let top_level_datasets = top_level_zarr_meta_v3(&datasets);
        let zarr_paths: Vec<ZarrPath> = top_level_datasets
            .into_iter()
            .filter_map(|path| ZarrPath::new_from_object_meta(path).ok())
            .collect();

        let datasets: Vec<DatasetMetadata> = zarr_paths
            .into_iter()
            .map(|path| DatasetMetadata::new(path.as_zarr_json_path(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

// ─── Format ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ZarrFormat;

#[async_trait::async_trait]
impl FileFormat for ZarrFormat {
    fn as_any(&self) -> &dyn Any {
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
        for object in objects {
            if !is_zarr_v3_metadata(object) {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Object at location '{}' is not a Zarr v3 metadata file (zarr.json)",
                    object.location.as_ref()
                )));
            }
        }

        let verified_objects = top_level_zarr_meta_v3(objects);
        let mut schemas = Vec::new();
        for object in verified_objects {
            let zarr_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create ZarrPath from ObjectMeta at {}: {e}",
                    object.location
                ))
            })?;
            let zarr_store = Arc::new(AsyncObjectStore::new(store.clone()))
                as Arc<dyn AsyncReadableListableStorageTraits>;
            let group = Group::async_open(zarr_store, &zarr_path.as_zarr_path())
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to open Zarr group at {}: {e}",
                        object.location
                    ))
                })?;

            let mut leaves = Vec::new();
            recursive_groups(Arc::new(group), &mut leaves)
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            for leaf in leaves {
                let any = dataset_from_group(&leaf, None).await.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read Zarr group as dataset: {e}"
                    ))
                })?;
                let schema = any_dataset_to_arrow_schema(&any).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Zarr Arrow schema: {e}"
                    ))
                })?;
                schemas.push(Arc::new(schema));
            }
        }

        if schemas.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "No valid Zarr v3 groups found to infer schema".to_string(),
            ));
        }

        let super_schema = super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to compute super schema for Zarr groups: {e}"
            ))
        })?;
        Ok(Arc::new(super_schema))
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
        let mut object_metas: Vec<ObjectMeta> = Vec::new();
        for group in &conf.file_groups {
            for file in group.files() {
                object_metas.push(file.object_meta.clone());
            }
        }
        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;

        let top_level_metas = top_level_zarr_meta_v3(&object_metas);
        let mut file_groups: Vec<FileGroup> = vec![];
        for meta in top_level_metas {
            let file = self
                .partition_zarr_group(&meta, object_store.clone())
                .await?;
            file_groups.push(file);
        }

        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        let source = ZarrSource::new(table_schema);
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(ZarrSource::new(table_schema))
    }
}

impl ZarrFormat {
    /// Expand a top-level zarr store into one [`PartitionedFile`] per leaf
    /// group, so nested sub-groups are scanned as independent partitions.
    async fn partition_zarr_group(
        &self,
        object: &ObjectMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> datafusion::error::Result<FileGroup> {
        let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
            as Arc<dyn AsyncReadableListableStorageTraits>;
        let group_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from ObjectMeta at {}: {e}",
                object.location
            ))
        })?;
        let group = Group::async_open(zarr_store, &group_path.as_zarr_path())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open Zarr group at {}: {e}",
                    object.location
                ))
            })?;

        match find_partitioned_files(&group).await {
            Some(partition_groups) => {
                if partition_groups.is_empty() {
                    Ok(FileGroup::new(vec![PartitionedFile::new(
                        group_path.as_zarr_json_path(),
                        0,
                    )]))
                } else {
                    let mut files = Vec::new();
                    for group in partition_groups {
                        let partition_path = group.path().to_string();
                        let partitioned_file =
                            PartitionedFile::new(format!("{partition_path}/zarr.json"), 0);
                        files.push(partitioned_file);
                    }
                    Ok(FileGroup::new(files))
                }
            }
            None => Ok(FileGroup::new(vec![])),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;

    use super::{ZarrFormat, ZarrFormatFactory};

    /// Register the bundled `gridded-example.zarr` store as a DataFusion table
    /// backed by [`ZarrFormat`] + [`ListingTable`].
    async fn register_example(ctx: &SessionContext) {
        let store_dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_files/gridded-example.zarr/"
        );
        let table_path = ListingTableUrl::parse(format!("file://{store_dir}")).unwrap();

        let format: Arc<dyn FileFormat> = Arc::new(ZarrFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension("zarr.json");

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("gridded", Arc::new(table)).unwrap();
    }

    #[tokio::test]
    async fn factory_discovers_gridded_example() {
        use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
        use object_store::{ObjectMeta, path::Path};

        let factory = ZarrFormatFactory::new();
        let objects = vec![
            ObjectMeta {
                location: Path::from("gridded-example.zarr/zarr.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
            // A nested array's metadata must NOT become its own dataset.
            ObjectMeta {
                location: Path::from("gridded-example.zarr/lat/zarr.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
        ];
        let datasets = factory.discover_datasets(&objects).unwrap();
        assert_eq!(datasets.len(), 1);
        assert!(
            datasets[0]
                .file_path
                .ends_with("gridded-example.zarr/zarr.json")
        );
        assert_eq!(datasets[0].format, "zarr");
    }

    #[tokio::test]
    async fn reads_gridded_example_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let df = ctx
            .sql("SELECT analysed_sst, lat, lon, time FROM gridded LIMIT 10")
            .await
            .unwrap();

        // CF time must surface as a nanosecond timestamp through the plan.
        let time_field = df.schema().field_with_unqualified_name("time").unwrap();
        assert_eq!(
            time_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10, "LIMIT 10 should yield exactly 10 rows");
        assert_eq!(batches[0].num_columns(), 4);
    }

    #[tokio::test]
    async fn schema_includes_arrays_and_attributes() {
        use datafusion::catalog::TableProvider;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let provider = ctx.table_provider("gridded").await.unwrap();
        let schema = provider.schema();
        let dtype = |name: &str| {
            schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("missing field '{name}' in schema"))
                .data_type()
                .clone()
        };

        // Coordinate + data variables, with CF decoding reflected in the types.
        assert_eq!(dtype("lat"), DataType::Float32);
        assert_eq!(dtype("lon"), DataType::Float32);
        // `time` is int32 "seconds since 1981-01-01" → CF time.
        assert_eq!(
            dtype("time"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        // `analysed_sst` is int16 with scale_factor/add_offset → decoded f64.
        assert_eq!(dtype("analysed_sst"), DataType::Float64);

        // Global (group) attributes are surfaced as ".<attr>" columns.
        assert_eq!(dtype(".Conventions"), DataType::Utf8);
        assert_eq!(dtype(".title"), DataType::Utf8);

        // Per-array attributes are surfaced as "<array>.<attr>" columns.
        assert_eq!(dtype("lat.units"), DataType::Utf8);
        assert_eq!(dtype("analysed_sst.units"), DataType::Utf8);
        assert_eq!(dtype("analysed_sst.scale_factor"), DataType::Float64);
        assert_eq!(dtype("analysed_sst.add_offset"), DataType::Float64);
    }

    #[tokio::test]
    async fn filter_pushdown_prunes_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        // An out-of-range latitude predicate prunes every chunk: the scan
        // should return no rows.
        let batches = ctx
            .sql("SELECT analysed_sst, lat FROM gridded WHERE lat > 100000")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "impossible lat predicate should prune all rows");
    }
}
