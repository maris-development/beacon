use std::sync::Arc;

use async_trait::async_trait;
use beacon_datafusion_ext::table_ext::TableDefinition;
use beacon_plugin_sdk::datafusion_ffi::util::FFI_Result;
use beacon_plugin_sdk::table_type::{TableType, export_table_types};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::{MemTable, TableType as DfTableType};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, CreateExternalTable, DdlStatement, Expr, LogicalPlan, ScalarUDF,
    TableProviderFilterPushDown, Volatility, col, create_udf, lit,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    InstalledTableTypes, PluginTableDefinition, PluginTableType, check_table_types,
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
