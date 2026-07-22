//! The `TableProvider` every `beacon.system` table is built from.

use std::{any::Any, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::{Session, TableProvider},
    common::Result as DFResult,
    datasource::memory::MemorySourceConfig,
    logical_expr::{Expr, TableType},
    physical_plan::ExecutionPlan,
};

/// Produces the table's rows. Called once per scan, so a table always reflects the
/// runtime's state at query time rather than at registration time.
pub(crate) type Snapshot = Arc<dyn Fn() -> DFResult<RecordBatch> + Send + Sync>;

/// A read-only, in-memory system table: a fixed schema plus a closure that
/// materializes the current rows.
///
/// Everything is snapshotted per scan and held in memory for the duration of the
/// query. That is fine at the sizes these tables have (hundreds of functions, a
/// bounded number of retained query metrics) and keeps the tables free of any
/// persistence concern — a persistent backing can replace the snapshot closure
/// later without changing the SQL surface.
pub(crate) struct SystemTable {
    schema: SchemaRef,
    snapshot: Snapshot,
}

impl SystemTable {
    pub(crate) fn new(schema: SchemaRef, snapshot: Snapshot) -> Self {
        Self { schema, snapshot }
    }
}

impl std::fmt::Debug for SystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for SystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// `View`, not `Base`: these tables are derived from runtime state and can
    /// never be written to.
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = (self.snapshot)()?;
        let exec = MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.schema.clone(),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}
