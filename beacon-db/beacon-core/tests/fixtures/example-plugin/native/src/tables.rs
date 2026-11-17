//! Table types: `exkv` keeps CSV files in a local folder, `expanic` panics in `create`,
//! and `exstore` reads text files from a Beacon object store.

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
use beacon_plugin_sdk::datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema};
use beacon_plugin_sdk::datafusion::datasource::MemTable;
use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStoreExt};
use beacon_plugin_sdk::table_type::{TableType, datasets_base_url, listing_url};
use futures::TryStreamExt;

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
        TableType::new("exstore", Arc::new(StoreFactory)),
    ]
}

/// A table over text files in a Beacon object store: one integer for each line.
///
/// `create` and each `scan` read the file through the object stores of the session.
#[derive(Debug)]
struct StoreTable {
    location: String,
    schema: SchemaRef,
}

fn store_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("base", DataType::Utf8, false),
    ]))
}

/// Reads the rows of each file that `location` matches, through the stores of `state`.
async fn store_rows(state: &dyn Session, location: &str) -> Result<RecordBatch> {
    let url = listing_url(state, location)?;
    let store = state.runtime_env().object_store(url.object_store())?;
    let files: Vec<ObjectMeta> = url
        .list_all_files(state, store.as_ref(), "")
        .await?
        .try_collect()
        .await?;
    if files.is_empty() {
        return plan_err!("exstore: file not found: {location}");
    }
    let mut values: Vec<i64> = Vec::new();
    for file in files {
        let bytes = store.get(&file.location).await?.bytes().await?;
        let text = String::from_utf8_lossy(&bytes);
        values.extend(text.lines().filter_map(|line| line.trim().parse::<i64>().ok()));
    }
    let base = datasets_base_url(state)
        .map(|url| url.to_string())
        .unwrap_or_default();
    let bases = StringArray::from(vec![base; values.len()]);
    Ok(RecordBatch::try_new(
        store_schema(),
        vec![Arc::new(Int64Array::from(values)), Arc::new(bases)],
    )?)
}

#[async_trait]
impl TableProvider for StoreTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> DfTableType {
        DfTableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = store_rows(state, &self.location).await?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        table.scan(state, projection, &[], limit).await
    }
}

#[derive(Debug)]
struct StoreFactory;

#[async_trait]
impl TableProviderFactory for StoreFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        // The read fails the statement when the file is not in the store.
        store_rows(state, &cmd.location).await?;
        Ok(Arc::new(StoreTable {
            location: cmd.location.clone(),
            schema: store_schema(),
        }))
    }
}
