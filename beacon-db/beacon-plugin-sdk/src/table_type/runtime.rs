//! The plugin runtime, and the wrappers that run table code inside it.
//!
//! `datafusion-ffi` 54.1 enters the runtime of a table only for record batch streams.
//! It catches no panics. The wrappers here enter the plugin runtime for each poll of
//! `create`, `scan` and `insert_into`, and turn a panic into an error.

use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
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

/// The runtime and the codec session of one export of table types.
///
/// The plugin has its own tokio copy, so the host runtime does not drive plugin I/O.
/// Each Beacon instance in a process exports the types again and gets its own context.
/// The factory, its providers, plans and streams keep the context alive.
pub(crate) struct PluginContext {
    /// `None` when the author owns the runtime, and after drop.
    runtime: Option<Runtime>,
    handle: Handle,
    session: Arc<dyn TaskContextProvider + Send + Sync>,
}

impl fmt::Debug for PluginContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginContext").finish_non_exhaustive()
    }
}

impl PluginContext {
    /// Starts a runtime and a session with the DataFusion default functions only.
    pub(crate) fn new() -> Arc<Self> {
        #[cfg(not(miri))]
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        // Miri refuses threads that live to the end, and a current-thread runtime starts none.
        #[cfg(miri)]
        let mut builder = tokio::runtime::Builder::new_current_thread();
        let runtime = builder
            .thread_name("beacon-plugin")
            .enable_all()
            .build()
            .expect("cannot start the plugin runtime");
        Arc::new(Self {
            handle: runtime.handle().clone(),
            runtime: Some(runtime),
            session: Arc::new(SessionContext::new()),
        })
    }

    /// Uses the runtime of `handle`, which the plugin author owns and stops.
    pub(crate) fn with_handle(handle: Handle) -> Arc<Self> {
        Arc::new(Self {
            runtime: None,
            handle,
            session: Arc::new(SessionContext::new()),
        })
    }

    pub(crate) fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// The task context provider of the codec. The FFI value holds a weak reference.
    pub(crate) fn task_ctx_provider(&self) -> FFI_TaskContextProvider {
        let provider: Arc<dyn TaskContextProvider> = self.session.clone();
        (&provider).into()
    }
}

impl Drop for PluginContext {
    fn drop(&mut self) {
        // A plain drop blocks, and it panics when the last owner drops inside a runtime.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
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

/// Runs the future that `make` builds inside the runtime of `context`.
async fn in_runtime<'a, T>(
    context: &PluginContext,
    what: &'static str,
    make: impl FnOnce() -> BoxFuture<'a, Result<T>>,
) -> Result<T> {
    let handle = context.handle();
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
pub(crate) struct RuntimeFactory {
    inner: Arc<dyn TableProviderFactory>,
    context: Arc<PluginContext>,
}

impl RuntimeFactory {
    pub(crate) fn new(inner: Arc<dyn TableProviderFactory>, context: Arc<PluginContext>) -> Self {
        Self { inner, context }
    }
}

#[async_trait]
impl TableProviderFactory for RuntimeFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = Arc::clone(&self.inner);
        let provider =
            in_runtime(&self.context, "table create", || inner.create(state, cmd)).await?;
        Ok(Arc::new(RuntimeProvider {
            inner: provider,
            context: Arc::clone(&self.context),
        }))
    }
}

/// A provider whose `scan` and `insert_into` run inside the plugin runtime.
#[derive(Debug)]
pub(crate) struct RuntimeProvider {
    inner: Arc<dyn TableProvider>,
    context: Arc<PluginContext>,
}

#[async_trait]
impl TableProvider for RuntimeProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        catch_unwind(AssertUnwindSafe(|| self.inner.statistics())).unwrap_or(None)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        guarded("filter pushdown", || {
            self.inner.supports_filters_pushdown(filters)
        })
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = in_runtime(&self.context, "table scan", || {
            self.inner.scan(state, projection, filters, limit)
        })
        .await?;
        Ok(PanicSafePlan::wrap(plan, &self.context))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = in_runtime(&self.context, "table insert", || {
            self.inner.insert_into(state, input, insert_op)
        })
        .await?;
        Ok(PanicSafePlan::wrap(plan, &self.context))
    }
}

/// A plan whose `execute` and output streams turn a panic into an error.
pub(crate) struct PanicSafePlan {
    inner: Arc<dyn ExecutionPlan>,
    context: Arc<PluginContext>,
}

impl PanicSafePlan {
    fn wrap(plan: Arc<dyn ExecutionPlan>, context: &Arc<PluginContext>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner: plan,
            context: Arc::clone(context),
        })
    }
}

impl fmt::Debug for PanicSafePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl DisplayAs for PanicSafePlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for PanicSafePlan {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner);
        guarded("plan", || inner.with_new_children(children))
            .map(|plan| Self::wrap(plan, &self.context))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = guarded("plan execute", || self.inner.execute(partition, context))?;
        Ok(Box::pin(KeepAlive {
            inner: panic_safe_stream(stream),
            _context: Arc::clone(&self.context),
        }))
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        catch_unwind(AssertUnwindSafe(|| self.inner.metrics())).unwrap_or(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        guarded("plan statistics", || {
            self.inner.partition_statistics(partition)
        })
    }
}

/// A stream that keeps the plugin context alive. A stream can outlive its plan.
struct KeepAlive {
    // Fields drop in this order: the stream before the runtime.
    inner: SendableRecordBatchStream,
    _context: Arc<PluginContext>,
}

impl futures::Stream for KeepAlive {
    type Item = Result<datafusion::arrow::array::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for KeepAlive {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::PluginContext;

    #[test]
    // Under Miri the runtime has no worker thread to run a spawn.
    #[cfg_attr(miri, ignore)]
    fn each_context_has_its_own_runtime() {
        let first = PluginContext::new();
        let second = PluginContext::new();
        assert!(!Arc::ptr_eq(&first, &second));
        let handle = second.handle();
        drop(first);
        // The second runtime still runs after the first one stops.
        let value = futures::executor::block_on(handle.spawn(async { 7 })).unwrap();
        assert_eq!(value, 7);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_context_drops_inside_its_own_runtime() {
        let context = PluginContext::new();
        let handle = context.handle();
        // The last owner drops on a worker thread of the runtime itself.
        futures::executor::block_on(handle.spawn(async move { drop(context) })).unwrap();
    }
}
