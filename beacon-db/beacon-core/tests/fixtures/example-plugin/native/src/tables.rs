//! Table types: `exkv` keeps CSV files in a local folder, and `expanic` panics in `create`.
//! `exgridkv` holds a 2x3 nd grid, and `exndmixed` mixes nd and plain columns.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::array::{Float64Array, Int64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::common::{Result, plan_err};
use beacon_plugin_sdk::datafusion::datasource::MemTable;
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
use beacon_plugin_sdk::nd::encoding::{encode_nd_record_batch, nd_encoded_field};
use beacon_plugin_sdk::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
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

/// A memory table of one nd batch: `lat` over `y` (2), `lon` over `x` (3), and `v`
/// over both, with `v = lat + lon`.
#[derive(Debug)]
struct GridFactory;

#[async_trait]
impl TableProviderFactory for GridFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let dims = |spec: &[(&str, usize)]| {
            Dimensions::try_new(
                spec.iter()
                    .map(|(name, size)| Dimension::new(*name, *size))
                    .collect(),
            )
        };
        let logical = Arc::new(Schema::new(vec![
            Field::new("lat", DataType::Int64, true),
            Field::new("lon", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
        ]));
        let columns = vec![
            NdArrowArray::try_new(Arc::new(Int64Array::from(vec![10, 20])), dims(&[("y", 2)])?)?,
            NdArrowArray::try_new(
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                dims(&[("x", 3)])?,
            )?,
            NdArrowArray::try_new(
                Arc::new(Float64Array::from(vec![11.0, 12.0, 13.0, 21.0, 22.0, 23.0])),
                dims(&[("y", 2), ("x", 3)])?,
            )?,
        ];
        let nd = NdRecordBatch::try_new(logical, columns, dims(&[("y", 2), ("x", 3)])?)?;
        let batch = encode_nd_record_batch(&nd)?;
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}

/// An empty memory table with one nd column and one plain column.
#[derive(Debug)]
struct MixedFactory;

#[async_trait]
impl TableProviderFactory for MixedFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            nd_encoded_field("lat", &DataType::Int64),
            Field::new("name", DataType::Utf8, true),
        ]));
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
    }
}

pub fn tables() -> Vec<TableType> {
    vec![
        TableType::new("exkv", Arc::new(KvFactory)),
        TableType::new("expanic", Arc::new(PanicFactory)),
        TableType::new("exgridkv", Arc::new(GridFactory)),
        TableType::new("exndmixed", Arc::new(MixedFactory)),
    ]
}
