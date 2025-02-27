use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    common::Statistics,
    datasource::{
        file_format::{
            arrow::ArrowFormat, file_compression_type::FileCompressionType, parquet::ParquetFormat,
            FileFormat, FilePushdownSupport,
        },
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    execution::SessionState,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::super_typing;

#[derive(Debug)]
pub struct SuperArrowFormat {
    inner_format: ArrowFormat,
}

#[async_trait::async_trait]
impl FileFormat for SuperArrowFormat {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self.inner_format.as_any()
    }

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String {
        self.inner_format.get_ext()
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner_format
            .get_ext_with_compression(_file_compression_type)
    }

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
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

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
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

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
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

    /// Take a list of files and the configuration to convert it to the
    /// appropriate writer executor according to this file format.
    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    /// Check if the specified file format has support for pushing down the provided filters within
    /// the given schemas. Added initially to support the Parquet file format's ability to do this.
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
