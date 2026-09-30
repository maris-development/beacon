//! A table provider that records the directory its files are in.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::TableType,
    error::Result,
    logical_expr::{dml::InsertOp, Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

/// A provider from a table-format library, with the directory that holds the table's files.
///
/// Read authorization checks the files a scan reaches. The library's own provider does not say
/// where they are, so a table function wraps it with its location. Every call goes to the
/// wrapped provider.
#[derive(Debug)]
pub struct LocatedTable {
    inner: Arc<dyn TableProvider>,
    location: String,
}

impl LocatedTable {
    pub fn new(inner: Arc<dyn TableProvider>, location: impl Into<String>) -> Self {
        Self {
            inner,
            location: location.into(),
        }
    }

    /// The table directory, as the query named it.
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait::async_trait]
impl TableProvider for LocatedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
