use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    common::Statistics,
    datasource::{
        file_format::{
            csv::CsvFormat, file_compression_type::FileCompressionType, FileFormat,
            FilePushdownSupport,
        },
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    execution::SessionState,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use beacon_common::super_typing;



#[derive(Debug)]
pub struct SuperCsvFormat {
    inner_format: CsvFormat,
}

impl SuperCsvFormat {
    pub fn new(delimiter: u8, infer_records: usize) -> Self {
        Self {
            inner_format: CsvFormat::default()
                .with_delimiter(delimiter)
                .with_schema_infer_max_rec(infer_records),
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for SuperCsvFormat {
    fn as_any(&self) -> &dyn Any {
        self.inner_format.as_any()
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
        let mut schemas = Vec::new();
        for object in objects {
            let schema = self
                .inner_format
                .infer_schema(state, store, &[object.clone()])
                .await?;
            schemas.push(schema);
        }

        //Supertype the schema
        let super_schema = super_typing::super_type_schema(&schemas).map_err(|e| {
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
