use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    execution::object_store::ObjectStoreUrl,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
    prelude::{Expr, SessionContext},
};

use crate::table::error::TableError;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct EmptyTable;

impl EmptyTable {
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
        _data_directory_store_url: ObjectStoreUrl,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        Ok(Arc::new(EmptyTableProvider))
    }
}

impl Default for EmptyTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct EmptyTableProvider;

#[async_trait::async_trait]
impl TableProvider for EmptyTableProvider {
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
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
