use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore, local::LocalFileSystem};

use crate::netcdf::opener::NetCDFFileSource;

pub mod opener;
pub mod sink;

const NETCDF_EXTENSION: &str = "nc";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfOptions {
    pub compression: Option<String>,
    // Add more options as needed
}

#[derive(Debug, Clone)]
pub struct NetcdfFormatFactory {
    pub options: NetcdfOptions,
    pub dataset_prefix: object_store::path::Path,
    pub local_datasets_file_store: Arc<LocalFileSystem>,
}

impl NetcdfFormatFactory {
    pub fn new(
        options: NetcdfOptions,
        dataset_prefix: object_store::path::Path,
        local_datasets_file_store: Arc<LocalFileSystem>,
    ) -> Self {
        Self {
            options,
            dataset_prefix,
            local_datasets_file_store,
        }
    }
}

impl FileFormatFactory for NetcdfFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NetcdfFormat::new(
            self.options.clone(),
            self.dataset_prefix.clone(),
            self.local_datasets_file_store.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NetcdfFormat::new(
            self.options.clone(),
            self.dataset_prefix.clone(),
            self.local_datasets_file_store.clone(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for NetcdfFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub options: NetcdfOptions,
    pub dataset_prefix: object_store::path::Path,
    pub local_datasets_file_store: Arc<LocalFileSystem>,
}

impl NetcdfFormat {
    pub fn new(
        options: NetcdfOptions,
        dataset_prefix: object_store::path::Path,
        local_datasets_file_store: Arc<LocalFileSystem>,
    ) -> Self {
        Self {
            options,
            dataset_prefix,
            local_datasets_file_store,
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
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "NetCDF format does not support schema inference yet".to_string(),
        ))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let source = NetCDFFileSource::new(
            self.dataset_prefix.clone(),
            self.local_datasets_file_store.clone(),
        );

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "NetCDF format does not support writing yet".to_string(),
        ))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(NetCDFFileSource::new(
            self.dataset_prefix.clone(),
            self.local_datasets_file_store.clone(),
        ))
    }
}
