//! The stand-in for the configured default table.
//!
//! `sql.default_table` names the table a JSON query without a `from` resolves
//! against. Beacon registers an empty stand-in under that name so such a query
//! plans on a fresh database, and drops the stand-in as soon as a real table
//! takes the name.
//!
//! The stand-in is a distinct type, not a bare
//! [`EmptyTable`](datafusion::datasource::empty::EmptyTable), so the `CREATE`
//! paths recognize it. A `CREATE` statement replaces the stand-in instead of
//! reporting that the table exists.

use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::{empty::EmptyTable, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
    sql::TableReference,
};

/// An empty, column-less table that holds the configured default-table name
/// until a real table takes it.
#[derive(Debug)]
pub struct DefaultTablePlaceholder {
    inner: EmptyTable,
}

impl DefaultTablePlaceholder {
    pub fn new() -> Self {
        Self {
            inner: EmptyTable::new(Arc::new(Schema::empty())),
        }
    }
}

impl Default for DefaultTablePlaceholder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TableProvider for DefaultTablePlaceholder {
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
}

/// True when `name` holds nothing but the default-table stand-in.
///
/// The `CREATE` paths call this to tell an occupied name from a name that only
/// carries the stand-in: the second one is free to take.
pub async fn holds_placeholder(session_ctx: &SessionContext, name: TableReference) -> bool {
    match session_ctx.table_provider(name).await {
        Ok(provider) => provider.as_any().is::<DefaultTablePlaceholder>(),
        Err(_) => false,
    }
}
