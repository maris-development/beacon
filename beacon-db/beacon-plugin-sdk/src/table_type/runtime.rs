//! The plugin runtime, and the wrappers that run table code inside it.
//!
//! `datafusion-ffi` 54.1 enters the runtime of a table only for record batch streams.
//! It catches no panics. The wrappers here enter the plugin runtime for each poll of
//! `create`, `scan` and `insert_into`, and turn a panic into an error.

use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::Statistics;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext, TaskContextProvider};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use futures::future::BoxFuture;
use tokio::runtime::{Handle, Runtime};

use crate::file_format::panic_safe_stream;
use crate::panic_safe::panic_message;

/// The tokio runtime of the plugin. The plugin has its own tokio copy, so the host runtime
/// does not drive plugin I/O.
pub(crate) fn plugin_runtime() -> &'static Runtime {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("beacon-plugin")
            .enable_all()
            .build()
            .expect("cannot start the plugin runtime")
    });
    &RUNTIME
}

/// The task context provider of the codec. It has the DataFusion default functions only.
pub(crate) fn plugin_task_ctx_provider() -> FFI_TaskContextProvider {
    // The FFI value holds a weak reference, so the static keeps the session alive.
    static SESSION: LazyLock<Arc<dyn TaskContextProvider + Send + Sync>> =
        LazyLock::new(|| Arc::new(SessionContext::new()));
    let provider: Arc<dyn TaskContextProvider> = SESSION.clone();
    (&provider).into()
}

fn panic_error(what: &str, payload: &(dyn std::any::Any + Send)) -> DataFusionError {
    DataFusionError::Execution(format!(
        "panic in plugin {what}: {}",
        panic_message(payload)
    ))
}

/// A future that runs each poll inside `handle`. A panic in a poll is an error.
struct InRuntime<'a, T> {
    inner: BoxFuture<'a, Result<T>>,
    handle: Handle,
    what: &'static str,
}

impl<T> Future for InRuntime<'_, T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = this.handle.enter();
        match catch_unwind(AssertUnwindSafe(|| this.inner.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(payload) => Poll::Ready(Err(panic_error(this.what, payload.as_ref()))),
        }
    }
}

/// Runs the future that `make` builds inside the plugin runtime.
async fn in_runtime<'a, T>(
    what: &'static str,
    make: impl FnOnce() -> BoxFuture<'a, Result<T>>,
) -> Result<T> {
    let handle = plugin_runtime().handle().clone();
    let inner = {
        let _guard = handle.enter();
        catch_unwind(AssertUnwindSafe(make))
            .map_err(|payload| panic_error(what, payload.as_ref()))?
    };
    InRuntime {
        inner,
        handle,
        what,
    }
    .await
}

/// Runs `work` and turns a panic into an error.
fn guarded<T>(what: &'static str, work: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(work))
        .unwrap_or_else(|payload| Err(panic_error(what, payload.as_ref())))
}

/// A factory whose `create` runs inside the plugin runtime.
#[derive(Debug)]
pub(crate) struct RuntimeFactory(pub(crate) Arc<dyn TableProviderFactory>);

#[async_trait]
impl TableProviderFactory for RuntimeFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = Arc::clone(&self.0);
        let provider = in_runtime("table create", || inner.create(state, cmd)).await?;
        Ok(Arc::new(RuntimeProvider(provider)))
    }
}

/// A provider whose `scan` and `insert_into` run inside the plugin runtime.
#[derive(Debug)]
pub(crate) struct RuntimeProvider(pub(crate) Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for RuntimeProvider {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        catch_unwind(AssertUnwindSafe(|| self.0.statistics())).unwrap_or(None)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        guarded("filter pushdown", || {
            self.0.supports_filters_pushdown(filters)
        })
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = in_runtime("table scan", || {
            self.0.scan(state, projection, filters, limit)
        })
        .await?;
        Ok(PanicSafePlan::wrap(plan))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = in_runtime("table insert", || {
            self.0.insert_into(state, input, insert_op)
        })
        .await?;
        Ok(PanicSafePlan::wrap(plan))
    }
}

/// A plan whose `execute` and output streams turn a panic into an error.
pub(crate) struct PanicSafePlan(Arc<dyn ExecutionPlan>);

impl PanicSafePlan {
    fn wrap(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self(plan))
    }
}

impl fmt::Debug for PanicSafePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl DisplayAs for PanicSafePlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(t, f)
    }
}

impl ExecutionPlan for PanicSafePlan {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.0);
        guarded("plan", || inner.with_new_children(children)).map(Self::wrap)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        guarded("plan execute", || self.0.execute(partition, context)).map(panic_safe_stream)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        catch_unwind(AssertUnwindSafe(|| self.0.metrics())).unwrap_or(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        guarded("plan statistics", || self.0.partition_statistics(partition))
    }
}
