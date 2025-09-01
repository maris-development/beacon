use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    execution::object_store::ObjectStoreUrl,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionContext},
};

use crate::table::error::TableError;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DefaultTable;

impl DefaultTable {
    pub fn new() -> Self {
        Self
    }

    pub async fn create(
        &self,
        _table_directory: object_store::path::Path,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        Ok(())
    }

    pub async fn table_provider(
        &self,
        _table_directory_store_url: ObjectStoreUrl,
        _table_directory: object_store::path::Path,
        _data_directory_store_url: ObjectStoreUrl,
        _data_directory: object_store::path::Path,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        Ok(Arc::new(DefaultTableProvider))
    }
}

#[derive(Debug)]
struct DefaultTableProvider;

#[async_trait::async_trait]
impl TableProvider for DefaultTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::empty())
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "Reading default table is not supported. Default tables are stubs that contain zero data.".to_string(),
        ));
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        return Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ]);
    }
}
