//! Newtype wrapper that pairs a built provider with its definition.
//!
//! `datafusion-table-providers` returns a `FederatedTableProviderAdaptor` — the
//! same concrete type the remote-Beacon table provider returns. Beacon's
//! catalog recovers a table's persisted definition by downcasting the live
//! provider to a known concrete type, so a bare adaptor would be
//! indistinguishable from a remote table. Wrapping it in this newtype gives the
//! catalog a distinct type to downcast to (see
//! `beacon_data_lake::definition_from_provider`).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::definition::SqlDatabaseTableDefinition;

/// A `TableProvider` that delegates everything to the inner federated provider
/// while carrying the [`SqlDatabaseTableDefinition`] it was built from.
#[derive(Debug)]
pub struct BeaconSqlDatabaseTable {
    inner: Arc<dyn TableProvider>,
    definition: SqlDatabaseTableDefinition,
}

impl BeaconSqlDatabaseTable {
    pub fn new(inner: Arc<dyn TableProvider>, definition: SqlDatabaseTableDefinition) -> Self {
        Self { inner, definition }
    }

    /// The persisted definition this table was built from.
    pub fn definition(&self) -> &SqlDatabaseTableDefinition {
        &self.definition
    }
}

#[async_trait]
impl TableProvider for BeaconSqlDatabaseTable {
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
}
