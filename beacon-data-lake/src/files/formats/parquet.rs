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

#[derive(Debug)]
pub struct ParquetFormatFactory;

impl GetExt for ParquetFormatFactory {
    fn get_ext(&self) -> String {
        "parquet".to_string()
    }
}

impl FileFormatFactory for ParquetFormatFactory {
    fn create(
        &self,
        _state: &SessionState,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ParquetFormat::new()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ParquetFormat {
    inner: datafusion::datasource::file_format::parquet::ParquetFormat,
}

impl ParquetFormat {
    pub fn new() -> Self {
        Self {
            inner: datafusion::datasource::file_format::parquet::ParquetFormat::default()
                .with_enable_pruning(true)
                .with_skip_metadata(true)
                .with_force_view_types(false),
        }
    }
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schema_futures = objects.iter().map(|object| {
            let state = state.clone();
            let store = Arc::clone(store);
            let object = object.clone();
            async move { self.inner.infer_schema(&state, &store, &[object]).await }
        });

        let schemas = try_join_all(schema_futures).await?;

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
        self.inner
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
        self.inner.create_physical_plan(state, conf, filters).await
    }

    fn supports_filters_pushdown(
        &self,
        file_schema: &Schema,
        table_schema: &Schema,
        filters: &[&Expr],
    ) -> datafusion::error::Result<FilePushdownSupport> {
        self.inner
            .supports_filters_pushdown(file_schema, table_schema, filters)
    }
}
