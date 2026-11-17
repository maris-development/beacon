use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_plugin_sdk::datafusion_ffi::util::FFI_Result;
use beacon_plugin_sdk::object_store::store::memory::InMemory;
use beacon_plugin_sdk::object_store::store::path::Path;
use beacon_plugin_sdk::object_store::store::{ObjectStoreExt, PutPayload};
use beacon_plugin_sdk::table_type::{TableType, export_table_types, open_location};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::{MemTable, TableType as DfTableType};
use datafusion::error::Result;
use datafusion::execution::object_store::DefaultObjectStoreRegistry;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    ColumnarValue, CreateExternalTable, DdlStatement, Expr, LogicalPlan, ScalarUDF,
    TableProviderFilterPushDown, Volatility, col, create_udf, lit,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    InstalledTableTypes, PluginHostInfo, PluginTableDefinition, PluginTableType, check_table_types,
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
    let info = PluginHostInfo {
        runtime: tokio::runtime::Handle::current(),
        datasets_url: None,
        datasets_base_url: None,
    };
    let host = info.ffi_host(Arc::new(DefaultObjectStoreRegistry::new()));
    let FFI_Result::Ok(ffi) = export_table_types(
        || vec![TableType::new("greedy", Arc::new(GreedyFactory))],
        host,
    ) else {
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

/// A table type that reads the file of `LOCATION` through the session of `create`.
#[derive(Debug)]
struct ReaderFactory;

#[async_trait]
impl TableProviderFactory for ReaderFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let open = open_location(state, &cmd.location)?;
        let bytes = open
            .store
            .get(&Path::from(open.path))
            .await?
            .bytes()
            .await?;
        let value: i64 = std::str::from_utf8(&bytes).unwrap().trim().parse().unwrap();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

/// A session with `obs/a.txt` = `42` in the store `datasets://`, and the `reader` type.
async fn reader_session() -> (Arc<SessionContext>, Arc<InstalledTableTypes>) {
    let installed = Arc::new(InstalledTableTypes::default());
    let ctx = session(installed.clone());
    let datasets = ObjectStoreUrl::parse("datasets://").unwrap();
    let store = InMemory::new();
    store
        .put(&Path::from("obs/a.txt"), PutPayload::from_static(b"42"))
        .await
        .unwrap();
    ctx.register_object_store(datasets.as_ref(), Arc::new(store));
    let info = PluginHostInfo {
        runtime: tokio::runtime::Handle::current(),
        datasets_url: Some(datasets),
        datasets_base_url: None,
    };
    let host = info.ffi_host(Arc::clone(&ctx.runtime_env().object_store_registry));
    let FFI_Result::Ok(ffi) = export_table_types(
        || vec![TableType::new("reader", Arc::new(ReaderFactory))],
        host,
    ) else {
        panic!("export failed");
    };
    installed.insert("p", import_table_types(ffi.into_iter().collect()).unwrap());
    (ctx, installed)
}

async fn reader_value(location: &str) -> i64 {
    let (ctx, installed) = reader_session().await;
    let sql = format!("CREATE EXTERNAL TABLE t STORED AS reader LOCATION '{location}'");
    let cmd = command(&ctx, &sql).await;
    let table = create_plugin_table(&ctx.state(), &installed, &cmd)
        .await
        .unwrap()
        .unwrap();
    ctx.register_table("t", table).unwrap();
    let batches = ctx
        .sql("SELECT a FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_table_reads_a_relative_location() {
    assert_eq!(reader_value("obs/a.txt").await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_table_reads_a_datasets_url() {
    assert_eq!(reader_value("datasets://obs/a.txt").await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_table_keeps_the_location_text() {
    let (ctx, installed) = reader_session().await;
    let cmd = command(
        &ctx,
        "CREATE EXTERNAL TABLE t STORED AS reader LOCATION 'obs/a.txt'",
    )
    .await;
    let table = create_plugin_table(&ctx.state(), &installed, &cmd)
        .await
        .unwrap()
        .unwrap();
    let table = table.downcast_ref::<crate::PluginTable>().unwrap();
    assert_eq!(table.definition().location, "obs/a.txt");
}
