use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::{listing::ListingTable, TableType},
    logical_expr::{dml::InsertOp, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

pub mod parquet;

#[derive(Debug)]
pub struct DataSource {
    inner_table: ListingTable,
}

impl DataSource {
    
}

#[async_trait::async_trait]
impl TableProvider for DataSource {
    fn as_any(&self) -> &dyn Any {
        self.inner_table.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.inner_table.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner_table.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner_table.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.inner_table.get_logical_plan()
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        self.inner_table.get_column_default(_column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_table
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner_table.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_table
            .insert_into(_state, _input, _insert_op)
            .await
    }
}
