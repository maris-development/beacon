//! A thin [`TableProvider`] wrapper that carries the [`DeltaTableDefinition`].
//!
//! `CREATE EXTERNAL TABLE ... STORED AS DELTA` registers the provider with
//! Beacon's `TableManager`, which persists `table.json` by downcasting the
//! registered provider back to a known definition (see
//! `serialize_table_provider_definition`). The raw delta-rs provider can't carry
//! Beacon's definition, so we wrap it — mirroring how the federated remote table
//! carries its `RemoteTableDefinition`. All query/write behavior is delegated to
//! the inner delta provider.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::TableType;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::arrow::datatypes::SchemaRef;

use crate::definition::DeltaTableDefinition;

/// Wraps a delta-rs [`TableProvider`], pairing it with the Beacon definition that
/// produced it so the catalog can persist and reload the table.
#[derive(Debug)]
pub struct BeaconDeltaTable {
    inner: Arc<dyn TableProvider>,
    definition: DeltaTableDefinition,
}

impl BeaconDeltaTable {
    pub fn new(inner: Arc<dyn TableProvider>, definition: DeltaTableDefinition) -> Self {
        Self { inner, definition }
    }

    pub fn definition(&self) -> &DeltaTableDefinition {
        &self.definition
    }
}

#[async_trait::async_trait]
impl TableProvider for BeaconDeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
