use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_ffi::util::FFI_Result;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use url::Url;

use crate::object_store::tests::foreign_marker_id;

use super::*;

#[derive(Debug)]
enum Mode {
    Memory,
    Spawn,
    Panic,
    PanicScan,
    ReadStore,
}

/// A runtime for the host stores of the tests. It lives to the end of the test binary.
pub(crate) fn test_handle() -> tokio::runtime::Handle {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("test-host")
                .build()
                .unwrap()
        })
        .handle()
        .clone()
}

const BASE_URL: &str = "file:///beacon/data/datasets/";

/// A host registry with `a/x.txt` = `hi` in the store `datasets://`.
fn host_registry() -> Arc<dyn ObjectStoreRegistry> {
    let store = InMemory::new();
    futures::executor::block_on(store.put(&Path::from("a/x.txt"), PutPayload::from_static(b"hi")))
        .unwrap();
    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(&Url::parse("datasets://").unwrap(), Arc::new(store));
    Arc::new(registry)
}

/// A host value whose registry crosses through the foreign path.
pub(crate) fn test_host() -> FFI_PluginHost {
    let mut host = FFI_PluginHost::new(
        host_registry(),
        test_handle(),
        Some(&ObjectStoreUrl::parse("datasets://").unwrap()),
        Some(&Url::parse(BASE_URL).unwrap()),
    );
    host.registry.library_marker_id = foreign_marker_id;
    host
}

/// Reads `a/x.txt` of the datasets store through `env`.
async fn read_hi(env: &Arc<RuntimeEnv>) -> Result<()> {
    let store = env.object_store(ObjectStoreUrl::parse("datasets://")?)?;
    let bytes = store.get(&Path::from("a/x.txt")).await?.bytes().await?;
    assert_eq!(bytes.as_ref(), b"hi");
    Ok(())
}

/// A table that needs the datasets store in `scan`, `insert_into` and `execute`.
#[derive(Debug)]
struct StoreTable(Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for StoreTable {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        read_hi(state.runtime_env()).await?;
        read_hi(&state.task_ctx().runtime_env()).await?;
        let inner = self.0.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(StoreExec(inner)))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        read_hi(state.runtime_env()).await?;
        self.0.insert_into(state, input, insert_op).await
    }
}

/// A plan that needs the datasets store in `execute`.
#[derive(Debug)]
struct StoreExec(Arc<dyn ExecutionPlan>);

impl DisplayAs for StoreExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreExec")
    }
}

impl ExecutionPlan for StoreExec {
    fn name(&self) -> &str {
        "StoreExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        context
            .runtime_env()
            .object_store(ObjectStoreUrl::parse("datasets://")?)?;
        self.0.execute(partition, context)
    }
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
        state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        match self.0 {
            Mode::Memory => {}
            Mode::Spawn => tokio::spawn(async {}).await.unwrap(),
            Mode::Panic => panic!("boom"),
            Mode::PanicScan => return Ok(Arc::new(PanicScanTable(memory_table()))),
            Mode::ReadStore => {
                read_hi(state.runtime_env()).await?;
                read_hi(&state.task_ctx().runtime_env()).await?;
                return Ok(Arc::new(StoreTable(memory_table())));
            }
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
    let make: fn() -> Vec<TableType> = match mode {
        Mode::Memory => make_tables,
        Mode::Spawn => || vec![TableType::new("spawn", Arc::new(TestFactory(Mode::Spawn)))],
        Mode::Panic => || vec![TableType::new("panic", Arc::new(TestFactory(Mode::Panic)))],
        Mode::PanicScan => || {
            vec![TableType::new(
                "panic_scan",
                Arc::new(TestFactory(Mode::PanicScan)),
            )]
        },
        Mode::ReadStore => || {
            vec![TableType::new(
                "read_store",
                Arc::new(TestFactory(Mode::ReadStore)),
            )]
        },
    };
    let mut exported = match export_table_types(make, test_host()) {
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
    let FFI_Result::Err(message) = export_table_types(|| panic!("boom"), test_host()) else {
        panic!("expected an error");
    };
    assert!(message.as_str().contains("boom"), "{message}");
}

unsafe extern "C" fn other_version() -> u64 {
    2
}

#[test]
fn another_version_is_refused() {
    let FFI_Result::Ok(mut exported) = export_table_types(make_tables, test_host()) else {
        panic!("expected types");
    };
    let mut ffi = exported.remove(0).unwrap();
    ffi.version = other_version;
    let err = import_table_type(ffi).unwrap_err().to_string();
    assert!(
        err.contains("table type FFI version 2 is not supported (want 7)"),
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

/// An author runtime that stops without a block, also inside a runtime.
#[derive(Debug)]
struct AuthorRuntime(Option<tokio::runtime::Runtime>);

impl Drop for AuthorRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Fails unless `create` runs on a worker of the author runtime.
#[derive(Debug)]
struct AuthorFactory {
    // The factory owns the author runtime.
    _runtime: Arc<AuthorRuntime>,
}

#[async_trait]
impl TableProviderFactory for AuthorFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let name = tokio::spawn(async { std::thread::current().name().map(str::to_string) })
            .await
            .unwrap();
        assert_eq!(name.as_deref(), Some("author-rt"));
        Ok(memory_table())
    }
}

fn author_tables() -> Vec<TableType> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("author-rt")
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let factory = AuthorFactory {
        _runtime: Arc::new(AuthorRuntime(Some(runtime))),
    };
    vec![TableType::new("author", Arc::new(factory)).with_runtime(handle)]
}

#[test]
// Under Miri a multi-thread runtime leaves threads at exit.
#[cfg_attr(miri, ignore)]
fn with_runtime_runs_create_in_the_author_runtime() {
    let FFI_Result::Ok(mut exported) = export_table_types(author_tables, test_host()) else {
        panic!("expected types");
    };
    let (_, factory) = import_table_type(exported.remove(0).unwrap()).unwrap();
    let ctx = SessionContext::new();
    let created = futures::executor::block_on(factory.create(&ctx.state(), &create_command()));
    assert!(created.is_ok(), "{:?}", created.err());
}

async fn store_table(ctx: &SessionContext) {
    let factory = one_type(Mode::ReadStore);
    let table = factory
        .create(&ctx.state(), &create_command())
        .await
        .unwrap();
    ctx.register_table("t", table).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn create_sees_the_host_store() {
    let factory = one_type(Mode::ReadStore);
    let ctx = SessionContext::new();
    let created = factory.create(&ctx.state(), &create_command()).await;
    assert!(created.is_ok(), "{:?}", created.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_and_execute_see_the_host_store() {
    let ctx = SessionContext::new();
    store_table(&ctx).await;
    assert_eq!(sum(&ctx).await, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_into_sees_the_host_store() {
    let ctx = SessionContext::new();
    store_table(&ctx).await;
    ctx.sql("INSERT INTO t VALUES (4)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(sum(&ctx).await, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_session_without_a_host_store_fails() {
    // The session of the test has no `datasets://` store of its own.
    let ctx = SessionContext::new();
    assert!(read_hi(ctx.state().runtime_env()).await.is_err());
}

#[test]
fn a_host_with_another_version_is_refused() {
    let mut host = test_host();
    host.version = other_version;
    let FFI_Result::Err(message) = export_table_types(make_tables, host) else {
        panic!("expected an error");
    };
    assert!(
        message
            .as_str()
            .contains("plugin host FFI version 2 is not supported (want 7)"),
        "{message}"
    );
}

/// An author runtime type that needs the datasets store in `create`.
fn author_store_tables() -> Vec<TableType> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("author-rt")
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    // The test binary keeps this runtime.
    std::mem::forget(runtime);
    vec![
        TableType::new("author_store", Arc::new(TestFactory(Mode::ReadStore))).with_runtime(handle),
    ]
}

#[test]
#[cfg_attr(miri, ignore)]
fn author_runtime_types_get_the_host_too() {
    let FFI_Result::Ok(mut exported) = export_table_types(author_store_tables, test_host()) else {
        panic!("expected types");
    };
    let (_, factory) = import_table_type(exported.remove(0).unwrap()).unwrap();
    let ctx = SessionContext::new();
    let created = futures::executor::block_on(factory.create(&ctx.state(), &create_command()));
    assert!(created.is_ok(), "{:?}", created.err());
}
