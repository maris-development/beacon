use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_plugin_sdk::datafusion_ffi::util::FFI_Result;
use beacon_plugin_sdk::nd::encoding::{encode_nd_record_batch, nd_encoded_field};
use beacon_plugin_sdk::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use beacon_plugin_sdk::table_type::{TableType, export_table_types};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::{MemTable, TableType as DfTableType};
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    ColumnarValue, CreateExternalTable, DdlStatement, Expr, LogicalPlan, ScalarUDF,
    TableProviderFilterPushDown, Volatility, col, create_udf, lit,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    InstalledTableTypes, PluginTable, PluginTableDefinition, PluginTableType, check_table_types,
    create_plugin_table, import_table_types,
};

/// A memory table that claims `Inexact` for every filter.
#[derive(Debug)]
struct Greedy(Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for Greedy {
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
        // A filter on the value 99999 fails, as a filter that the plugin cannot decode.
        if filters.iter().any(|f| f.to_string().contains("99999")) {
            return datafusion::common::plan_err!("cannot decode the filter");
        }
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
struct GreedyFactory;

#[async_trait]
impl TableProviderFactory for GreedyFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(Greedy(Arc::new(table))))
    }
}

fn exported(names: &'static [&'static str]) -> Vec<PluginTableType> {
    names
        .iter()
        .map(|name| PluginTableType::new(name, Arc::new(GreedyFactory)))
        .collect()
}

/// The `greedy` type, through the FFI export and import.
fn foreign_types() -> Vec<PluginTableType> {
    let FFI_Result::Ok(ffi) =
        export_table_types(|| vec![TableType::new("greedy", Arc::new(GreedyFactory))])
    else {
        panic!("export failed");
    };
    import_table_types(ffi.into_iter().collect()).unwrap()
}

fn session(installed: Arc<InstalledTableTypes>) -> Arc<SessionContext> {
    let config = SessionConfig::new().with_extension(installed);
    Arc::new(SessionContext::new_with_config(config))
}

async fn command(ctx: &SessionContext, sql: &str) -> CreateExternalTable {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = plan else {
        panic!("expected CREATE EXTERNAL TABLE");
    };
    cmd
}

fn check_error(installed: &InstalledTableTypes, types: &[PluginTableType]) -> String {
    let state = SessionContext::new().state();
    check_table_types(&state, installed, types)
        .unwrap_err()
        .to_string()
}

#[test]
fn names_are_checked() {
    let none = InstalledTableTypes::default();
    assert!(check_error(&none, &exported(&["Bad-Name"])).contains("lower case"));
    assert!(check_error(&none, &exported(&["erddap"])).contains("built-in table type"));
    assert!(check_error(&none, &exported(&["postgresql"])).contains("built-in table type"));
    assert!(check_error(&none, &exported(&["odbc"])).contains("built-in table type"));
    assert!(check_error(&none, &exported(&["parquet"])).contains("file format"));
    assert!(check_error(&none, &exported(&["t", "t"])).contains("twice"));
    let taken = InstalledTableTypes::default();
    taken.insert("a", exported(&["t"]));
    assert!(check_error(&taken, &exported(&["t"])).contains("plugin 'a'"));
    let state = SessionContext::new().state();
    assert!(check_table_types(&state, &none, &exported(&["my_type1"])).is_ok());
}

#[test]
fn find_ignores_case() {
    let installed = InstalledTableTypes::default();
    installed.insert("p", exported(&["mytype"]));
    let (plugin, found) = installed.find("MyType").unwrap();
    assert_eq!(plugin, "p");
    assert_eq!(found.name(), "mytype");
    assert!(installed.find("other").is_none());
}

fn host_udf() -> ScalarUDF {
    create_udf(
        "beacon_only",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_function_filter_is_not_pushed() {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_types());
    let ctx = session(installed.clone());
    let cmd = command(
        &ctx,
        "CREATE EXTERNAL TABLE t STORED AS greedy LOCATION 'x'",
    )
    .await;
    let table = create_plugin_table(&ctx.state(), &installed, &cmd)
        .await
        .unwrap()
        .unwrap();
    let plain = col("a").gt(lit(1i64));
    let host = host_udf().call(vec![col("a")]).gt(lit(1i64));
    let answers = table.supports_filters_pushdown(&[&plain, &host]).unwrap();
    assert_eq!(
        answers,
        vec![
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Unsupported
        ]
    );
    ctx.register_udf(host_udf());
    ctx.register_table("t", table).unwrap();
    let batches = ctx
        .sql("SELECT a FROM t WHERE beacon_only(a) > 1 AND a < 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uninstalled_type_refuses_scans() {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_types());
    let ctx = session(installed.clone());
    let cmd = command(
        &ctx,
        "CREATE EXTERNAL TABLE t STORED AS greedy LOCATION 'x'",
    )
    .await;
    let table = create_plugin_table(&ctx.state(), &installed, &cmd)
        .await
        .unwrap()
        .unwrap();
    ctx.register_table("t", table).unwrap();
    installed.take("p");
    let err = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("plugin 'p'") && err.contains("uninstalled"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_definition_round_trips_and_rebuilds() {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_types());
    let ctx = session(installed.clone());
    let sql = "CREATE EXTERNAL TABLE t (a BIGINT) STORED AS greedy LOCATION 'x' OPTIONS ('k' 'v')";
    let cmd = command(&ctx, sql).await;
    let definition: Arc<dyn TableDefinition> =
        Arc::new(PluginTableDefinition::from_command(&cmd, "greedy"));
    let json = serde_json::to_value(&definition).unwrap();
    assert_eq!(json["definition_type"], "plugin_table");
    let restored: Arc<dyn TableDefinition> = serde_json::from_value(json).unwrap();
    assert_eq!(restored.table_name(), "t");
    let provider = restored.build_provider(ctx.clone()).await.unwrap();
    assert_eq!(provider.schema().field(0).name(), "a");

    let empty = session(Arc::new(InstalledTableTypes::default()));
    let err = restored
        .build_provider(empty)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("table type 'greedy' is not loaded"), "{err}");
}

/// A memory table of nd-encoded batches that claims `Exact` for every filter.
#[derive(Debug)]
struct NdExact(Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for NdExact {
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
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
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
}

/// `grid` makes a 2x3 grid: `y` over 2 rows, `x` over 3 columns and `v = y * 10 + x`.
/// `mixed` makes one nd column and one plain column.
#[derive(Debug)]
struct NdFactory {
    mixed: bool,
}

fn nd_grid() -> RecordBatch {
    let dims = |spec: &[(&str, usize)]| {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    };
    let logical = Arc::new(Schema::new(vec![
        Field::new("y", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let columns = vec![
        NdArrowArray::try_new(Arc::new(Int64Array::from(vec![1, 2])), dims(&[("y", 2)])).unwrap(),
        NdArrowArray::try_new(Arc::new(Int64Array::from(vec![1, 2, 3])), dims(&[("x", 3)]))
            .unwrap(),
        NdArrowArray::try_new(
            Arc::new(Int64Array::from(vec![11, 12, 13, 21, 22, 23])),
            dims(&[("y", 2), ("x", 3)]),
        )
        .unwrap(),
    ];
    let nd = NdRecordBatch::try_new(logical, columns, dims(&[("y", 2), ("x", 3)])).unwrap();
    encode_nd_record_batch(&nd).unwrap()
}

#[async_trait]
impl TableProviderFactory for NdFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        if self.mixed {
            let schema = Arc::new(Schema::new(vec![
                nd_encoded_field("y", &DataType::Int64),
                Field::new("name", DataType::Utf8, true),
            ]));
            return Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?));
        }
        let batch = nd_grid();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        Ok(Arc::new(NdExact(Arc::new(table))))
    }
}

/// The `ndgrid` and `ndmixed` types, through the FFI export and import.
fn foreign_nd_types() -> Vec<PluginTableType> {
    let FFI_Result::Ok(ffi) = export_table_types(|| {
        vec![
            TableType::new("ndgrid", Arc::new(NdFactory { mixed: false })),
            TableType::new("ndmixed", Arc::new(NdFactory { mixed: true })),
        ]
    }) else {
        panic!("export failed");
    };
    import_table_types(ffi.into_iter().collect()).unwrap()
}

async fn nd_session() -> (Arc<SessionContext>, Arc<InstalledTableTypes>) {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_nd_types());
    (session(installed.clone()), installed)
}

async fn nd_table(ctx: &SessionContext, installed: &InstalledTableTypes) -> Arc<PluginTable> {
    let cmd = command(ctx, "CREATE EXTERNAL TABLE g STORED AS ndgrid LOCATION 'x'").await;
    let table = create_plugin_table(&ctx.state(), installed, &cmd)
        .await
        .unwrap()
        .unwrap();
    let any: Arc<dyn std::any::Any + Send + Sync> = table;
    any.downcast::<PluginTable>().unwrap()
}

async fn sql_i64(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_nd_table_has_the_logical_schema() {
    let (ctx, installed) = nd_session().await;
    let table = nd_table(&ctx, &installed).await;
    assert!(table.is_nd());
    let expected = Schema::new(vec![
        Field::new("y", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]);
    assert_eq!(table.schema().as_ref(), &expected);
    assert!(table.statistics().is_none());
    let flat = greedy_table(&ctx, &{
        let flat = InstalledTableTypes::default();
        flat.insert("q", foreign_types());
        flat
    })
    .await;
    let any: Arc<dyn std::any::Any + Send + Sync> = flat;
    assert!(!any.downcast::<PluginTable>().unwrap().is_nd());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_nd_schema_is_refused() {
    let (ctx, installed) = nd_session().await;
    let cmd = command(
        &ctx,
        "CREATE EXTERNAL TABLE m STORED AS ndmixed LOCATION 'x'",
    )
    .await;
    let err = create_plugin_table(&ctx.state(), &installed, &cmd)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("mixes beacon.nd columns and plain columns (name)"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_nd_table_broadcasts_through_the_foreign_path() {
    let (ctx, installed) = nd_session().await;
    let table = nd_table(&ctx, &installed).await;
    ctx.register_table("g", table).unwrap();
    assert_eq!(
        sql_i64(&ctx, "SELECT y FROM g WHERE v IS NOT NULL ORDER BY v").await,
        vec![1, 1, 1, 2, 2, 2]
    );
    assert_eq!(
        sql_i64(&ctx, "SELECT x FROM g WHERE v IS NOT NULL ORDER BY v").await,
        vec![1, 2, 3, 1, 2, 3]
    );
    assert_eq!(sql_i64(&ctx, "SELECT count(*) FROM g").await, vec![6]);
    assert_eq!(
        sql_i64(&ctx, "SELECT v FROM g WHERE y = 2 AND x < 3 ORDER BY v").await,
        vec![21, 22]
    );
    // The plugin claims `Exact`, and Beacon filters the rows again.
    let filter = col("y").eq(lit(2i64));
    let answers = ctx
        .table_provider("g")
        .await
        .unwrap()
        .supports_filters_pushdown(&[&filter])
        .unwrap();
    assert_eq!(answers, vec![TableProviderFilterPushDown::Inexact]);
}

async fn greedy_table(
    ctx: &SessionContext,
    installed: &InstalledTableTypes,
) -> Arc<dyn TableProvider> {
    let cmd = command(ctx, "CREATE EXTERNAL TABLE t STORED AS greedy LOCATION 'x'").await;
    create_plugin_table(&ctx.state(), installed, &cmd)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_insert_writes_every_input_partition() {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_types());
    let ctx = session(installed.clone());
    let table = greedy_table(&ctx, &installed).await;
    let schema = table.schema();
    let part = |value: i64| {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    };
    let source = MemTable::try_new(schema.clone(), vec![vec![part(10)], vec![part(20)]]).unwrap();
    let state = ctx.state();
    let input = source.scan(&state, None, &[], None).await.unwrap();
    assert_eq!(input.output_partitioning().partition_count(), 2);
    // Beacon runs the insert plan as it is, with no physical optimizer pass.
    let plan = table
        .insert_into(&state, input, InsertOp::Append)
        .await
        .unwrap();
    let counts = collect(plan, ctx.task_ctx()).await.unwrap();
    let count = counts[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);
    ctx.register_table("t", table).unwrap();
    let sum = ctx
        .sql("SELECT sum(a) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let sum = sum[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(sum, 36);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_pushdown_leaves_the_filters_to_beacon() {
    let installed = Arc::new(InstalledTableTypes::default());
    installed.insert("p", foreign_types());
    let ctx = session(installed.clone());
    let table = greedy_table(&ctx, &installed).await;
    let host = host_udf().call(vec![col("a")]).gt(lit(1i64));
    let bad = col("a").eq(lit(99999i64));
    let answers = table.supports_filters_pushdown(&[&host, &bad]).unwrap();
    assert_eq!(answers, vec![TableProviderFilterPushDown::Unsupported; 2]);
    ctx.register_table("t", table).unwrap();
    let rows = ctx
        .sql("SELECT a FROM t WHERE a <> 99999")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}
