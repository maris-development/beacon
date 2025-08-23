use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSource},
    },
    execution::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use beacon_common::super_typing::super_type_schema;
use futures::future::try_join_all;

#[derive(Debug, Default)]
pub struct CsvFormatFactory;

impl GetExt for CsvFormatFactory {
    fn get_ext(&self) -> String {
        "csv".to_string()
    }
}

impl FileFormatFactory for CsvFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(CsvFormat::new(b',', 1000)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(CsvFormat::new(b',', 1000))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct CsvFormat {
    inner_format: datafusion::datasource::file_format::csv::CsvFormat,
}

impl CsvFormat {
    pub fn new(delimiter: u8, infer_records: usize) -> Self {
        Self {
            inner_format: datafusion::datasource::file_format::csv::CsvFormat::default()
                .with_delimiter(delimiter)
                .with_schema_infer_max_rec(infer_records),
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner_format.compression_type()
    }

    fn get_ext(&self) -> String {
        self.inner_format.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner_format
            .get_ext_with_compression(_file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        //Retrieve the schema for each object
        let schemas = try_join_all(objects.iter().map(|object| {
            let store = store.clone();
            let object = object.clone();
            async move {
                self.inner_format
                    .infer_schema(state, &store, &[object])
                    .await
            }
        }))
        .await?;

        //Supertype the schema
        let super_schema = super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to infer schema: {}", e))
        })?;

        Ok(Arc::new(super_schema))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        self.inner_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_format.create_physical_plan(state, conf).await
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        self.inner_format.file_source()
    }
}
