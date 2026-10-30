use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion_ffi::util::FFI_Result;

use super::*;

#[derive(Debug)]
enum Mode {
    Memory,
    Spawn,
    Panic,
    PanicScan,
}

#[derive(Debug)]
struct TestFactory(Mode);

/// A table whose `scan` panics.
#[derive(Debug)]
struct PanicScanTable(Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for PanicScanTable {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        panic!("boom scan");
    }
}

fn memory_table() -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

#[async_trait]
impl TableProviderFactory for TestFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        match self.0 {
            Mode::Memory => {}
            Mode::Spawn => tokio::spawn(async {}).await.unwrap(),
            Mode::Panic => panic!("boom"),
            Mode::PanicScan => return Ok(Arc::new(PanicScanTable(memory_table()))),
        }
        Ok(memory_table())
    }
}

pub(crate) fn make_tables() -> Vec<TableType> {
    vec![TableType::new(
        "memtest",
        Arc::new(TestFactory(Mode::Memory)),
    )]
}

fn one_type(mode: Mode) -> Arc<dyn TableProviderFactory> {
    let mut exported = match export_table_types(match mode {
        Mode::Memory => make_tables,
        Mode::Spawn => || vec![TableType::new("spawn", Arc::new(TestFactory(Mode::Spawn)))],
        Mode::Panic => || vec![TableType::new("panic", Arc::new(TestFactory(Mode::Panic)))],
        Mode::PanicScan => || {
            vec![TableType::new(
                "panic_scan",
                Arc::new(TestFactory(Mode::PanicScan)),
            )]
        },
    }) {
        FFI_Result::Ok(exported) => exported,
        FFI_Result::Err(message) => panic!("export failed: {message}"),
    };
    let (_, factory) = import_table_type(exported.remove(0).unwrap()).unwrap();
    factory
}

fn command(sql: &str, options: &[(&str, &str)]) -> CreateExternalTable {
    let ctx = SessionContext::new();
    let plan = futures::executor::block_on(ctx.state().create_logical_plan(sql)).unwrap();
    let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(mut cmd)) = plan else {
        panic!("expected CREATE EXTERNAL TABLE");
    };
    cmd.options = options
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<_, _>>();
    cmd
}

fn create_command() -> CreateExternalTable {
    command(
        "CREATE EXTERNAL TABLE t STORED AS memtest LOCATION 'x'",
        &[],
    )
}

async fn sum(ctx: &SessionContext) -> i64 {
    let batches = ctx
        .sql("SELECT sum(a) FROM t")
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
async fn a_table_type_round_trips() {
    let factory = one_type(Mode::Memory);
    let ctx = SessionContext::new();
    let table = factory
        .create(&ctx.state(), &create_command())
        .await
        .unwrap();
    ctx.register_table("t", table).unwrap();
    assert_eq!(sum(&ctx).await, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_into_reaches_the_plugin_provider() {
    let factory = one_type(Mode::Memory);
    let ctx = SessionContext::new();
    let table = factory
        .create(&ctx.state(), &create_command())
        .await
        .unwrap();
    ctx.register_table("t", table).unwrap();
    let inserted = ctx
        .sql("INSERT INTO t VALUES (4)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = inserted[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
    assert_eq!(sum(&ctx).await, 10);
}

#[test]
// Under Miri the plugin runtime has no worker thread to run the spawn.
#[cfg_attr(miri, ignore)]
fn create_runs_inside_the_plugin_runtime() {
    // No tokio runtime on this thread: only the plugin runtime can run the spawn.
    let factory = one_type(Mode::Spawn);
    let ctx = SessionContext::new();
    let created = futures::executor::block_on(factory.create(&ctx.state(), &create_command()));
    assert!(created.is_ok(), "{:?}", created.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_create_is_an_error() {
    let factory = one_type(Mode::Panic);
    let ctx = SessionContext::new();
    let err = factory
        .create(&ctx.state(), &create_command())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("boom"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_in_scan_is_a_query_error() {
    let factory = one_type(Mode::PanicScan);
    let ctx = SessionContext::new();
    let table = factory
        .create(&ctx.state(), &create_command())
        .await
        .unwrap();
    ctx.register_table("t", table).unwrap();
    let err = match ctx.sql("SELECT * FROM t").await {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(err.contains("boom scan"), "{err}");
}

#[test]
fn a_panic_in_make_is_an_error() {
    let FFI_Result::Err(message) = export_table_types(|| panic!("boom")) else {
        panic!("expected an error");
    };
    assert!(message.as_str().contains("boom"), "{message}");
}

unsafe extern "C" fn other_version() -> u64 {
    2
}

#[test]
fn another_version_is_refused() {
    let FFI_Result::Ok(mut exported) = export_table_types(make_tables) else {
        panic!("expected types");
    };
    let mut ffi = exported.remove(0).unwrap();
    ffi.version = other_version;
    let err = import_table_type(ffi).unwrap_err().to_string();
    assert!(
        err.contains("table type FFI version 2 is not supported (want 6)"),
        "{err}"
    );
}

#[test]
fn table_option_reads_the_prefixed_key() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS memtest LOCATION 'x'";
    let cmd = command(sql, &[("format.url", "x")]);
    assert_eq!(table_option(&cmd, "url"), Some("x"));
    let cmd = command(sql, &[("url", "y"), ("format.url", "x")]);
    assert_eq!(table_option(&cmd, "url"), Some("y"));
    assert_eq!(table_option(&cmd, "other"), None);
}
