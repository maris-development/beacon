//! The stand-in for the configured default table.
//!
//! `sql.default_table` names the table a JSON query without a `from` resolves
//! against. Beacon registers an empty stand-in under that name so such a query
//! plans on a fresh database, and drops the stand-in as soon as a real table
//! takes the name.
//!
//! A `CREATE` statement on that name fails, as it does on any other table that
//! exists. Run `DROP TABLE` first to take the name.
//!
//! The stand-in is a distinct type, not a bare
//! [`EmptyTable`](datafusion::datasource::empty::EmptyTable), so the `CREATE`
//! paths recognize it and say that in the error.

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
pub async fn holds_placeholder(session_ctx: &SessionContext, name: TableReference) -> bool {
    match session_ctx.table_provider(name).await {
        Ok(provider) => provider.as_any().is::<DefaultTablePlaceholder>(),
        Err(_) => false,
    }
}

/// The reason a `CREATE` statement refuses `name`.
///
/// A name the stand-in holds looks free to the user, because no one made that
/// table. The message therefore names the stand-in and says how to free the name.
pub async fn already_exists_error(
    session_ctx: &SessionContext,
    name: TableReference,
    subject: &str,
) -> anyhow::Error {
    let table = name.table().to_string();
    if holds_placeholder(session_ctx, name).await {
        return anyhow::anyhow!(
            "{subject} '{table}' already exists. Beacon creates it at startup as the default \
             table. Run `DROP TABLE \"{table}\"` first to take the name."
        );
    }
    anyhow::anyhow!("{subject} '{table}' already exists")
}
