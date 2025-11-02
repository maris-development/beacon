use std::{any::Any, borrow::Cow, collections::HashSet, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{plan_datafusion_err, Constraints, Statistics},
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableType,
    },
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{dml::InsertOp, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::super_typing::super_type_schema;

#[derive(Debug)]
pub struct SuperListingTable {
    inner_table: ListingTable,
}

impl SuperListingTable {
    pub async fn new(
        session_state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        table_urls: Vec<ListingTableUrl>,
    ) -> Result<Self, DataFusionError> {
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension("")
            .with_target_partitions(session_state.config_options().execution.target_partitions)
            .with_collect_stat(true);
        let mut schemas = vec![];

        for table_url in &table_urls {
            tracing::debug!("Infer schema for table/file url: {}", table_url);
            let schema = listing_options
                .infer_schema(session_state, table_url)
                .await?;

            schemas.push(schema.clone());
        }

        let super_schema = super_type_schema(&schemas).map_err(|e| {
            plan_datafusion_err!("Failed to compute super schema for listing table: {}", e)
        })?;

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .with_schema(super_schema.into());
        let table = ListingTable::try_new(config)?;

        Ok(Self { inner_table: table })
    }

    pub fn with_pushdown_projection(
        &self,
        projection: Vec<String>,
    ) -> Result<Self, DataFusionError> {
        let mut schema = self.inner_table.schema();
        let options = self.inner_table.options().clone();
        let table_paths = self.inner_table.table_paths().to_vec();

        let maybe_set_projection: HashSet<String> = HashSet::from_iter(projection.iter().cloned());

        if !maybe_set_projection.is_empty() {
            // Only keep fields that are in the projection
            let filtered_fields = schema
                .fields()
                .iter()
                .filter(|f| projection.contains(f.name()))
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();

            if !filtered_fields.is_empty() {
                schema = Arc::new(Schema::new(filtered_fields));
            }
        }

        let table_config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(options)
            .with_schema(schema);

        let table = ListingTable::try_new(table_config)?;

        Ok(Self { inner_table: table })
    }
}

#[async_trait::async_trait]
impl TableProvider for SuperListingTable {
    fn as_any(&self) -> &dyn Any {
        self
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

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
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
        // Apply the projection to the current schema and add is_not_null filters for every column
        let plan = self
            .inner_table
            .scan(state, projection, filters, limit)
            .await?;

        Ok(plan)
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
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_table.insert_into(state, input, insert_op).await
    }
}
