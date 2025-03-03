use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableType,
    },
    execution::SessionState,
    logical_expr::{dml::InsertOp, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::super_typing::super_type_schema;

pub mod arrow_format;
pub mod csv_format;
pub mod netcdf_format;
pub mod odv_format;
pub mod parquet_format;

#[derive(Debug)]
pub struct DataSource {
    inner_table: ListingTable,
}

impl DataSource {
    pub async fn new(
        session_state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        table_urls: Vec<ListingTableUrl>,
    ) -> anyhow::Result<Self> {
        let listing_options = ListingOptions::new(file_format);

        let mut schemas = vec![];
        for table_url in &table_urls {
            tracing::debug!("Infer schema for table: {}", table_url);
            let schema = listing_options
                .infer_schema(&session_state, table_url)
                .await?;
            schemas.push(schema);
        }

        let super_schema = Arc::new(
            super_type_schema(&schemas)
                .map_err(|e| anyhow::anyhow!("Failed to super type schema: {}", e))?,
        );

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .with_schema(super_schema);

        let table = ListingTable::try_new(config)?;

        Ok(Self { inner_table: table })
    }
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
