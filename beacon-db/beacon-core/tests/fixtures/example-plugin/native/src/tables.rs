//! Table types: `exkv` keeps CSV files in a local folder, and `expanic` panics in `create`.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::common::{Result, plan_err};
use beacon_plugin_sdk::datafusion::datasource::TableType as DfTableType;
use beacon_plugin_sdk::datafusion::datasource::file_format::csv::CsvFormat;
use beacon_plugin_sdk::datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use beacon_plugin_sdk::datafusion::logical_expr::dml::InsertOp;
use beacon_plugin_sdk::datafusion::logical_expr::{
    CreateExternalTable, Expr, TableProviderFilterPushDown,
};
use beacon_plugin_sdk::datafusion::physical_plan::ExecutionPlan;
use beacon_plugin_sdk::table_type::TableType;

/// A CSV listing table that claims `Inexact` for every filter, so Beacon applies them again.
#[derive(Debug)]
struct KvTable(Arc<ListingTable>);

#[async_trait]
impl TableProvider for KvTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> DfTableType {
        DfTableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, &[], limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.insert_into(state, input, insert_op).await
    }
}

#[derive(Debug)]
struct KvFactory;

#[async_trait]
impl TableProviderFactory for KvFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        if cmd.schema.fields().is_empty() {
            return plan_err!("exkv tables need a column list");
        }
        // `tokio::fs` proves that `create` runs inside the plugin runtime.
        tokio::fs::create_dir_all(&cmd.location).await?;
        let mut folder = cmd.location.clone();
        if !folder.ends_with('/') && !folder.ends_with('\\') {
            folder.push('/');
        }
        let url = ListingTableUrl::parse(&folder)?;
        let format = CsvFormat::default().with_has_header(false);
        let options = ListingOptions::new(Arc::new(format)).with_file_extension(".csv");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(Arc::clone(cmd.schema.inner()));
        let _ = state;
        Ok(Arc::new(KvTable(Arc::new(ListingTable::try_new(config)?))))
    }
}

#[derive(Debug)]
struct PanicFactory;

#[async_trait]
impl TableProviderFactory for PanicFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        panic!("expanic create");
    }
}

pub fn tables() -> Vec<TableType> {
    vec![
        TableType::new("exkv", Arc::new(KvFactory)),
        TableType::new("expanic", Arc::new(PanicFactory)),
    ]
}
