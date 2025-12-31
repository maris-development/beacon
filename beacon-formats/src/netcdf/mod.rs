use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
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

use crate::{
    Dataset, DatasetFormat, FileFormatFactoryExt,
    netcdf::{
        execution::unique_values::UniqueValuesExec,
        object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver},
        sink::{NetCDFNdSink, NetCDFSink},
        source::{NetCDFFileSource, fetch_schema},
    },
};

pub mod execution;
pub mod object_resolver;
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
    ) -> datafusion::error::Result<Vec<crate::Dataset>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == NETCDF_EXTENSION)
                    .unwrap_or(false)
            })
            .map(|obj| Dataset {
                file_path: obj.location.to_string(),
                format: DatasetFormat::NetCDF,
            })
            .collect();
        Ok(datasets)
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

            // Create lex order requirements based on the unique columns
            let schema = unique_exec.schema();
            let mut sort_exprs = vec![];
            for col in &unique_columns {
                sort_exprs.push(PhysicalSortExpr::new_default(
                    datafusion::physical_expr::expressions::col(col, &schema)?,
                ));
            }
            let lex_order = LexOrdering::new(sort_exprs)
                .ok_or(exec_datafusion_err!("Failed to create LexOrdering"))?;
            // Create sort exec on the unique columns
            let sort_exec = SortExec::new(lex_order.clone(), Arc::new(unique_exec));
            // Create a sort preserving merge exec to ensure global ordering
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
