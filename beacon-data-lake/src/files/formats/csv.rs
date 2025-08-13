use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    common::{GetExt, Statistics},
    datasource::{
        file_format::{
            FileFormat, FileFormatFactory, FilePushdownSupport,
            file_compression_type::FileCompressionType,
        },
        physical_plan::FileScanConfig,
    },
    execution::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::util::super_type_schema;
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
        _state: &SessionState,
        _format_options: &std::collections::HashMap<String, String>,
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
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        //Retrieve the schema for each object
        let schemas = try_join_all(objects.iter().map(|object| {
            let state = state.clone();
            let store = store.clone();
            let object = object.clone();
            async move {
                self.inner_format
                    .infer_schema(&state, &store, &[object])
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
        state: &SessionState,
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
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_format
            .create_physical_plan(state, conf, filters)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        file_schema: &Schema,
        table_schema: &Schema,
        filters: &[&Expr],
    ) -> datafusion::error::Result<FilePushdownSupport> {
        self.inner_format
            .supports_filters_pushdown(file_schema, table_schema, filters)
    }
}
