//! [`IcebergTable`]: beacon's `TableProvider` wrapper around an Iceberg table.
//!
//! Mirrors the wrapper pattern used by `ExternalTable`/`MaterializedView` in
//! `beacon-datafusion-ext`: the wrapper holds its own serializable definition so
//! beacon's schema-persistence layer can downcast to it and recover the
//! `table.json`, while all query/insert work is delegated to the inner
//! [`DataFusionTable`].

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_iceberg::DataFusionTable;

use crate::definition::IcebergTableDefinition;

/// A beacon-managed Iceberg table provider.
#[derive(Debug, Clone)]
pub struct IcebergTable {
    definition: IcebergTableDefinition,
    inner: Arc<DataFusionTable>,
}

impl IcebergTable {
    pub fn new(definition: IcebergTableDefinition, inner: DataFusionTable) -> Self {
        Self {
            definition,
            inner: Arc::new(inner),
        }
    }

    /// The serializable definition used to persist and rebuild this table.
    pub fn definition(&self) -> &IcebergTableDefinition {
        &self.definition
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
