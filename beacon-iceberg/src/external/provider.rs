//! [`ExternalIcebergTable`]: beacon's `TableProvider` wrapper for an *external*
//! Iceberg table.
//!
//! A distinct type from the managed [`crate::IcebergTable`] on purpose: beacon's
//! DDL handlers downcast to `IcebergTable` to reclaim warehouse files on `DROP`
//! and to route `DELETE`/`UPDATE`/`ALTER`. An external table must NOT match those
//! downcasts — beacon does not own its files (so `DROP` only deregisters), and
//! row mutations / schema evolution stay unsupported. Reads and `INSERT` are
//! delegated to the inner [`DataFusionTable`] (whose catalog is the per-location
//! file catalog, so appends commit correctly).

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

use super::definition::ExternalIcebergTableDefinition;

/// A beacon-registered, external Iceberg table provider (read + `INSERT`).
#[derive(Debug, Clone)]
pub struct ExternalIcebergTable {
    definition: ExternalIcebergTableDefinition,
    inner: Arc<DataFusionTable>,
}

impl ExternalIcebergTable {
    pub fn new(definition: ExternalIcebergTableDefinition, inner: DataFusionTable) -> Self {
        Self {
            definition,
            inner: Arc::new(inner),
        }
    }

    /// The serializable definition used to persist and rebuild this table.
    pub fn definition(&self) -> &ExternalIcebergTableDefinition {
        &self.definition
    }
}

#[async_trait]
impl TableProvider for ExternalIcebergTable {
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
