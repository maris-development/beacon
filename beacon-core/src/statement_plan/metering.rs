//! Per-query execution limits for non-admin queries.
//!
//! Admins can cap how long a query executes and how large its result may grow
//! (see [`beacon_config::RuntimeConfig`]). Enforcement is a transparent physical
//! node, [`MeteredExec`], inserted into the plan after planning by
//! [`insert_meter`]; the [`MeteredStream`] it produces accumulates *active
//! execution time* (the wall-clock spent inside the child's `poll_next`, which
//! excludes client backpressure and the planning phase) plus running row/byte
//! counts, and aborts the query the moment a configured limit is breached.
//!
//! Placement makes the "count compute, not the write" rule fall out: for a
//! file-output query (a `COPY TO`, i.e. a [`DataSinkExec`] root) the meter wraps
//! the sink's *input*, so the format-writer/file-write time is not counted and
//! the meter sees the real result batches; for a streaming query it wraps the
//! root.

use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::{Stream, StreamExt};

/// The per-query limits applied to non-admin queries. A `None` field means that
/// dimension is unlimited.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryLimits {
    /// Maximum active execution time before the query is aborted.
    pub timeout: Option<Duration>,
    /// Maximum number of result rows before the query is aborted.
    pub max_rows: Option<u64>,
    /// Maximum result size in bytes before the query is aborted.
    pub max_bytes: Option<u64>,
}

impl QueryLimits {
    /// At least one limit is set, so a meter is worth inserting.
    pub(crate) fn is_active(&self) -> bool {
        self.timeout.is_some() || self.max_rows.is_some() || self.max_bytes.is_some()
    }
}

impl From<&beacon_config::RuntimeConfig> for QueryLimits {
    fn from(cfg: &beacon_config::RuntimeConfig) -> Self {
        Self {
            timeout: cfg.query_timeout_secs.map(Duration::from_secs),
            max_rows: cfg.max_output_rows,
            max_bytes: cfg.max_output_bytes,
        }
    }
}

/// Shared, mutable budget for one query. A single instance is shared by every
/// partition stream of a [`MeteredExec`], so under parallelism the counters
/// accumulate across partitions (execution time then behaves closer to CPU-time
/// than wall-clock).
#[derive(Debug)]
struct MeterState {
    limits: QueryLimits,
    elapsed_nanos: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
}

impl MeterState {
    fn new(limits: QueryLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            elapsed_nanos: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }

    /// Return an error if any configured limit has been exceeded.
    fn check(&self) -> Result<()> {
        if let Some(timeout) = self.limits.timeout {
            if self.elapsed_nanos.load(Ordering::Relaxed) > timeout.as_nanos() as u64 {
                return Err(DataFusionError::Execution(format!(
                    "query exceeded execution-time limit of {}s",
                    timeout.as_secs()
                )));
            }
        }
        if let Some(max_rows) = self.limits.max_rows {
            if self.rows.load(Ordering::Relaxed) > max_rows {
                return Err(DataFusionError::Execution(format!(
                    "query exceeded output-row limit of {max_rows}"
                )));
            }
        }
        if let Some(max_bytes) = self.limits.max_bytes {
            if self.bytes.load(Ordering::Relaxed) > max_bytes {
                return Err(DataFusionError::Execution(format!(
                    "query exceeded output-byte limit of {max_bytes}"
                )));
            }
        }
        Ok(())
    }
}

/// A record-batch stream that meters the wrapped stream against a [`MeterState`].
struct MeteredStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    state: Arc<MeterState>,
    done: bool,
}

impl MeteredStream {
    fn new(inner: SendableRecordBatchStream, state: Arc<MeterState>) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            state,
            done: false,
        }
    }
}

impl Stream for MeteredStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }

        // Count only the time the child spends actively producing this batch;
        // time the consumer is not polling (backpressure) is never charged.
        let started = Instant::now();
        let poll = this.inner.poll_next_unpin(cx);
        this.state
            .elapsed_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);

        if let Poll::Ready(Some(Ok(batch))) = &poll {
            this.state
                .rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            this.state
                .bytes
                .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
        }

        if let Err(err) = this.state.check() {
            this.done = true;
            return Poll::Ready(Some(Err(err)));
        }

        poll
    }
}

impl RecordBatchStream for MeteredStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// A transparent physical node that wraps each partition stream of its input in
/// a [`MeteredStream`] sharing one [`MeterState`]. All plan properties are
/// inherited from the input, so it is invisible to partitioning/ordering.
#[derive(Debug)]
pub(crate) struct MeteredExec {
    input: Arc<dyn ExecutionPlan>,
    state: Arc<MeterState>,
    cache: Arc<PlanProperties>,
}

impl MeteredExec {
    fn new(input: Arc<dyn ExecutionPlan>, state: Arc<MeterState>) -> Self {
        let cache = input.properties().clone();
        Self {
            input,
            state,
            cache,
        }
    }
}

impl DisplayAs for MeteredExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MeteredExec")
    }
}

impl ExecutionPlan for MeteredExec {
    fn name(&self) -> &str {
        "MeteredExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.state.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        Ok(Box::pin(MeteredStream::new(inner, self.state.clone())))
    }
}

/// Insert a [`MeteredExec`] enforcing `limits` into `plan`.
///
/// For a `COPY TO` plan (a [`DataSinkExec`] root) the meter is placed on the
/// sink's input so the file write is excluded from the budget; otherwise it
/// wraps the root.
pub(crate) fn insert_meter(
    plan: Arc<dyn ExecutionPlan>,
    limits: QueryLimits,
) -> Arc<dyn ExecutionPlan> {
    let state = MeterState::new(limits);

    if plan.as_any().is::<DataSinkExec>() {
        let children = plan.children();
        if children.len() == 1 {
            let metered: Arc<dyn ExecutionPlan> =
                Arc::new(MeteredExec::new(children[0].clone(), state.clone()));
            // Rebuild the sink with the metered input. `with_new_children`
            // preserves the sink; only its child is swapped.
            if let Ok(rebuilt) = plan.clone().with_new_children(vec![metered]) {
                return rebuilt;
            }
        }
    }

    Arc::new(MeteredExec::new(plan, state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::TryStreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(n: usize) -> RecordBatch {
        let schema = test_schema();
        let array = Int32Array::from((0..n as i32).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    /// Build a `MeteredStream` over a fixed set of batches with the given limits.
    fn metered(batches: Vec<RecordBatch>, limits: QueryLimits) -> MeteredStream {
        let schema = test_schema();
        let inner: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        MeteredStream::new(inner, MeterState::new(limits))
    }

    #[tokio::test]
    async fn no_limits_passes_all_rows() {
        let stream = metered(
            vec![batch(100), batch(100)],
            QueryLimits::default(),
        );
        let out: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 200);
    }

    #[tokio::test]
    async fn row_limit_aborts() {
        let stream = metered(
            vec![batch(100), batch(100), batch(100)],
            QueryLimits {
                max_rows: Some(150),
                ..Default::default()
            },
        );
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("output-row limit"), "{err}");
    }

    #[tokio::test]
    async fn byte_limit_aborts() {
        let stream = metered(
            vec![batch(1000), batch(1000)],
            QueryLimits {
                max_bytes: Some(1),
                ..Default::default()
            },
        );
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("output-byte limit"), "{err}");
    }

    #[tokio::test]
    async fn time_limit_aborts_with_slow_inner() {
        // Each batch is produced by synchronous work inside `poll_next` (a
        // blocking sleep stands in for CPU-bound compute), so the accumulated
        // active poll-time crosses the tiny budget. Async waits would register
        // as `Pending` and — correctly — not be charged, so we block here.
        let schema = test_schema();
        let inner: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(0..10).map(|_| {
                std::thread::sleep(Duration::from_millis(20));
                Ok(batch(1))
            }),
        ));
        let stream = MeteredStream::new(
            inner,
            MeterState::new(QueryLimits {
                timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            }),
        );
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("execution-time limit"), "{err}");
    }
}
