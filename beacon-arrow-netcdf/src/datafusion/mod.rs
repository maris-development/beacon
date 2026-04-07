use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        sink::DataSinkExec,
    },
    physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr},
    physical_plan::{
        ExecutionPlan,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    },
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::source::{NetCDFFileSource, fetch_schema};
use crate::datafusion::{execution::unique_values::UniqueValuesExec, sink::{NetCDFNdSink, NetCDFSink}};

pub mod execution;
pub mod sink;
pub mod source;

const NETCDF_EXTENSION: &str = "nc";

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfOptions {
    pub compression: Option<String>,
    #[serde(default)]
    pub unique_value_columns: Vec<String>,
    #[serde(default = "default_replay_batch_size")]
    pub replay_batch_size: usize,
}

fn default_replay_batch_size() -> usize {
    128 * 1024
}

#[derive(Debug, Clone)]
pub struct NetCDFFormatFactory {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
}

impl NetCDFFormatFactory {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: NetcdfOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
    }
}

impl FileFormatFactory for NetCDFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NetcdfFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NetcdfFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for NetCDFFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for NetCDFFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == NETCDF_EXTENSION)
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
}

impl NetcdfFormat {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: NetcdfOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for NetcdfFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(NETCDF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut tasks = vec![];
        for object in objects {
            let schema_task = fetch_schema(self.datasets_object_store.clone(), object.clone());
            tasks.push(schema_task);
        }
        let schemas = futures::future::try_join_all(tasks).await?;
        if schemas.is_empty() {
            return Err(exec_datafusion_err!(
                "No schemas or datasets found when inferring NetCDF schema"
            ));
        }
        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!(
                "Failed to compute super type schema for NetCDF datasets: {}",
                e
            )
        })?;

        Ok(schema.into())
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
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let source = NetCDFFileSource::new(self.datasets_object_store.clone());
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.options.unique_value_columns.is_empty() {
            let netcdf_sink = Arc::new(NetCDFSink::new(conf));
            Ok(Arc::new(DataSinkExec::new(
                input,
                netcdf_sink,
                order_requirements,
            )))
        } else {
            let unique_columns = self.options.unique_value_columns.clone();

            let (unique_exec, collection_handle) =
                UniqueValuesExec::new(input, unique_columns.clone())?;

            let schema = unique_exec.schema();
            let mut sort_exprs = vec![];
            for col in &unique_columns {
                sort_exprs.push(PhysicalSortExpr::new_default(
                    datafusion::physical_expr::expressions::col(col, &schema)?,
                ));
            }
            let lex_order = LexOrdering::new(sort_exprs)
                .ok_or(exec_datafusion_err!("Failed to create LexOrdering"))?;
            let sort_exec = SortExec::new(lex_order.clone(), Arc::new(unique_exec));
            let sort_preserving_merge_exec =
                SortPreservingMergeExec::new(lex_order, Arc::new(sort_exec));

            let netcdf_sink = Arc::new(NetCDFNdSink::new(
                conf,
                unique_columns.len(),
                collection_handle,
            )?);

            Ok(Arc::new(DataSinkExec::new(
                Arc::new(sort_preserving_merge_exec),
                netcdf_sink,
                order_requirements,
            )))
        }
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(NetCDFFileSource::new(self.datasets_object_store.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Arc, Once};

    use beacon_object_storage::{DatasetsStore, get_datasets_object_store};
    use datafusion::{
        datasource::{
            file_format::FileFormat,
            file_format::file_compression_type::FileCompressionType,
            listing::{ListingTableUrl, PartitionedFile},
            physical_plan::{FileGroup, FileScanConfigBuilder, FileSinkConfig},
        },
        execution::object_store::ObjectStoreUrl,
        logical_expr::dml::InsertOp,
        physical_plan::common,
        prelude::SessionContext,
    };
    use futures::StreamExt;

    use crate::reader::NetCDFArrowReader;

    static TEST_FIXTURES: Once = Once::new();

    fn ensure_test_fixtures() {
        TEST_FIXTURES.call_once(|| {
            let src: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("test_files")
                .join("gridded-example.nc");

            let dst_dir: PathBuf = beacon_config::DATASETS_DIR_PATH.join("test-files");
            std::fs::create_dir_all(&dst_dir).expect("create datasets test-files dir");

            let dst = dst_dir.join("gridded-example.nc");
            if !dst.exists() {
                std::fs::copy(&src, &dst).expect("copy NetCDF test fixture into datasets dir");
            }
        });
    }

    async fn test_datasets_object_store() -> Arc<DatasetsStore> {
        ensure_test_fixtures();
        get_datasets_object_store().await
    }

    fn fixture_partitioned_file() -> PartitionedFile {
        PartitionedFile::new("test-files/gridded-example.nc", 0)
    }

    #[tokio::test]
    async fn datafusion_read_plan_executes_for_netcdf() {
        let datasets_object_store = test_datasets_object_store().await;
        let format = NetcdfFormat::new(datasets_object_store, NetcdfOptions::default());

        let session = SessionContext::new();
        let state = session.state();
        let local_store = state
            .runtime_env()
            .object_store(&ObjectStoreUrl::local_filesystem())
            .expect("local object store");

        let inferred_schema = format
            .infer_schema(
                &state,
                &local_store,
                &[fixture_partitioned_file().object_meta.clone()],
            )
            .await
            .expect("infer schema");

        let scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            inferred_schema,
            format.file_source(),
        )
        .with_file(fixture_partitioned_file())
        .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
        .build();

        let plan = format
            .create_physical_plan(&state, scan_config)
            .await
            .expect("create physical plan");

        let stream = plan
            .execute(0, session.task_ctx())
            .expect("execute read plan");
        let batches = common::collect(stream).await.expect("collect read batches");

        assert!(!batches.is_empty(), "expected at least one output batch");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "expected read plan to emit rows");
    }

    #[tokio::test]
    async fn datafusion_roundtrip_read_then_write_netcdf() {
        let datasets_object_store = test_datasets_object_store().await;
        let format = NetcdfFormat::new(datasets_object_store, NetcdfOptions::default());

        let session = SessionContext::new();
        let state = session.state();
        let local_store = state
            .runtime_env()
            .object_store(&ObjectStoreUrl::local_filesystem())
            .expect("local object store");

        let inferred_schema = format
            .infer_schema(
                &state,
                &local_store,
                &[fixture_partitioned_file().object_meta.clone()],
            )
            .await
            .expect("infer schema");

        let read_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            inferred_schema,
            format.file_source(),
        )
        .with_file(fixture_partitioned_file())
        .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
        .build();

        let read_plan = format
            .create_physical_plan(&state, read_scan_config)
            .await
            .expect("create read plan");

        let output_dir = tempfile::tempdir().expect("create temp output dir");
        let output_file = output_dir.path().join("roundtrip.nc");
        let listing_url = ListingTableUrl::parse(output_file.to_string_lossy().to_string())
            .expect("listing table url");

        let write_config = FileSinkConfig {
            original_url: output_file.to_string_lossy().to_string(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_group: FileGroup::new(vec![]),
            table_paths: vec![listing_url.clone()],
            output_schema: read_plan.schema(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "nc".to_string(),
        };

        let writer_plan = format
            .create_writer_physical_plan(read_plan, &state, write_config, None)
            .await
            .expect("create writer plan");

        let write_stream = writer_plan
            .execute(0, session.task_ctx())
            .expect("execute writer plan");
        let _ = common::collect(write_stream)
            .await
            .expect("collect writer output");

        let written_path = std::env::temp_dir().join(listing_url.prefix().as_ref());
        assert!(
            written_path.exists(),
            "expected output file to exist at {}",
            written_path.display()
        );

        let reader = NetCDFArrowReader::new(&written_path).expect("open written netcdf file");
        let mut stream = reader
            .read_as_arrow_stream(1024)
            .await
            .expect("read written file as arrow stream");
        let mut total_rows = 0usize;
        while let Some(next) = stream.next().await {
            let batch = next.expect("batch from written file");
            total_rows += batch.num_rows();
        }

        assert!(total_rows > 0, "expected written file to contain rows");
    }
}
