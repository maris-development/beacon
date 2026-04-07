//! Utilities for synchronising NetCDF writes that require a full data scan before emitting rows.
//!
//! A `NetCDFBarrierExec` buffers *all* incoming batches once, exposes the collected
//! values to downstream consumers (e.g. unique-dimension calculators), and then
//! replays the exact same batches again to drive the actual NetCDF sink.

use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use datafusion::{
    common::Statistics,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties, coalesce_partitions::CoalescePartitionsExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

#[derive(Debug)]
pub struct NetCDFBarrierExec {
    /// Input plan guaranteed to have *exactly one* output partition.
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<Schema>,
    /// Shared buffer that stores the first pass over the data.
    pub barrier: Arc<NetCDFBarrier>,
}

impl NetCDFBarrierExec {
    /// Wraps any input plan in a [`CoalescePartitionsExec`] so we only have to
    /// manage a single input stream.
    pub fn new(input: Arc<dyn ExecutionPlan>, barrier: Arc<NetCDFBarrier>) -> Self {
        let input = Arc::new(CoalescePartitionsExec::new(input));

        let schema = input.schema();
        // let mut eq_properties = input.equivalence_properties().clone();
        // eq_properties.clear_orderings();
        // eq_properties.clear_per_partition_constants();
        // let cache = PlanProperties::new(
        //     eq_properties,                        // Equivalence Properties
        //     Partitioning::UnknownPartitioning(1), // Output Partitioning
        //     datafusion::physical_plan::execution_plan::EmissionType::Final, // Emission Type
        //     datafusion::physical_plan::execution_plan::Boundedness::Bounded, // Boundedness
        // );
        Self {
            schema,
            input,
            barrier,
        }
    }
}

impl DisplayAs for NetCDFBarrierExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetCDFBarrierExec")
    }
}

impl ExecutionPlan for NetCDFBarrierExec {
    fn name(&self) -> &str {
        "NetCDFBarrierExec"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "NetCDFBarrierExec expects exactly one child".to_string(),
            ));
        }

        println!(
            "Children Partitioning: {:?}",
            children[0].properties().output_partitioning()
        );

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.barrier.clone(),
        )))
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        // We always force a single partition upstream, so no repartitioning is possible
        Ok(None)
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn statistics(
        &self,
    ) -> std::result::Result<datafusion::common::Statistics, datafusion::error::DataFusionError>
    {
        Ok(Statistics::default())
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // We only expose 1 output partition
        assert_eq!(
            partition, 0,
            "NetCDFBarrierExec only has 1 output partition"
        );

        println!(
            "NetCDFBarrierExec: Input partitions: {}",
            self.input
                .properties()
                .output_partitioning()
                .partition_count()
        );

        // Because we wrapped upstream in CoalescePartitionsExec, it also has 1 partition
        let input_stream = self.input.execute(partition, ctx)?;

        Ok(Box::pin(NetCDFBarrierStream::new(
            input_stream,
            self.barrier.clone(),
        )))
    }
}

#[derive(Debug, Default)]
pub struct NetCDFBarrier {
    /// Buffered batches cloned during the initial pass.
    ///
    /// `RecordBatch` cloning is cheap (it only bumps `Arc` counters), so keeping
    /// everything in memory is acceptable for the current data sizes. We can
    /// spill to disk later if required by swapping the backing store.
    buffer: Mutex<Vec<RecordBatch>>,
}

impl NetCDFBarrier {
    /// Creates an empty barrier buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a batch captured during the *buffering* phase.
    pub fn add_batch(&self, batch: RecordBatch) {
        let mut guard = self.buffer.lock();
        guard.push(batch);
    }

    /// Returns a clone of the fully-buffered batches for replay.
    pub fn finalized_batches(&self) -> Vec<RecordBatch> {
        let guard = self.buffer.lock();
        guard.clone()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Buffering,
    Replaying,
    Done,
}

/// Stream implementation that first buffers, then replays, the upstream batches.
pub struct NetCDFBarrierStream {
    pub input: SendableRecordBatchStream,
    pub barrier: Arc<NetCDFBarrier>,
    phase: Phase,
    replay_batches: Vec<RecordBatch>,
    replay_idx: usize,
}

impl NetCDFBarrierStream {
    fn new(input: SendableRecordBatchStream, barrier: Arc<NetCDFBarrier>) -> Self {
        Self {
            input,
            barrier,
            phase: Phase::Buffering,
            replay_batches: Vec::new(),
            replay_idx: 0,
        }
    }
}

impl Stream for NetCDFBarrierStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.phase {
                Phase::Buffering => {
                    match futures::ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            // Buffer in the shared barrier
                            self.barrier.add_batch(batch.clone());
                            // We don't emit yet, just keep draining upstream
                            continue;
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            println!(
                                "NetCDFBarrierStream: completed buffering {} batches",
                                self.barrier.finalized_batches().len()
                            );
                            // Upstream complete → snapshot the batches for replay
                            self.replay_batches = self.barrier.finalized_batches();
                            self.replay_idx = 0;
                            self.phase = Phase::Replaying;
                            continue;
                        }
                    }
                }

                Phase::Replaying => {
                    if self.replay_idx < self.replay_batches.len() {
                        let batch = self.replay_batches[self.replay_idx].clone();
                        self.replay_idx += 1;
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    self.phase = Phase::Done;
                    continue;
                }

                Phase::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for NetCDFBarrierStream {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        util::pretty::pretty_format_batches,
    };
    use datafusion::{
        physical_plan::common, physical_plan::memory::MemoryStream,
        physical_plan::test::TestMemoryExec, prelude::SessionContext,
    };

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn batch_from(values: &[i32]) -> RecordBatch {
        let array = Arc::new(Int32Array::from_iter_values(values.iter().copied()));
        RecordBatch::try_new(sample_schema(), vec![array]).expect("batch creation")
    }

    fn assert_batches_eq(expected: &[RecordBatch], actual: &[RecordBatch]) {
        let expected_pretty = pretty_format_batches(expected)
            .expect("format expected batches")
            .to_string();
        let actual_pretty = pretty_format_batches(actual)
            .expect("format actual batches")
            .to_string();
        assert_eq!(expected_pretty, actual_pretty);
    }

    #[tokio::test]
    async fn barrier_exec_buffers_and_replays_all_batches() {
        let batch1 = batch_from(&[1, 2]);
        let batch2 = batch_from(&[3, 4, 5]);
        let partitions = vec![vec![batch1.clone()], vec![batch2.clone()]];
        let exec = Arc::new(
            TestMemoryExec::try_new(&partitions, sample_schema(), None).expect("memory exec"),
        );

        let barrier = Arc::new(NetCDFBarrier::new());
        let barrier_exec = Arc::new(NetCDFBarrierExec::new(exec, barrier.clone()));

        let session = SessionContext::new();
        let stream = barrier_exec
            .execute(0, session.task_ctx())
            .expect("execute barrier plan");
        let output = common::collect(stream).await.expect("collect output");

        assert_batches_eq(&[batch1.clone(), batch2.clone()], &output);
        let buffered = barrier.finalized_batches();
        assert_batches_eq(&[batch1, batch2], &buffered);
    }

    #[tokio::test]
    async fn barrier_exec_handles_empty_input() {
        let partitions: Vec<Vec<RecordBatch>> = vec![vec![]];
        let exec = Arc::new(
            TestMemoryExec::try_new(&partitions, sample_schema(), None).expect("memory exec"),
        );
        let barrier = Arc::new(NetCDFBarrier::new());
        let barrier_exec = Arc::new(NetCDFBarrierExec::new(exec, barrier.clone()));

        let session = SessionContext::new();
        let stream = barrier_exec
            .execute(0, session.task_ctx())
            .expect("execute barrier plan");
        let output = common::collect(stream).await.expect("collect output");

        assert!(output.is_empty(), "expected no output batches");
        let buffered = barrier.finalized_batches();
        assert!(buffered.is_empty(), "expected no buffered batches");
    }
}
