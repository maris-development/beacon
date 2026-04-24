//! Append execution plan for combining multiple single-partition inputs.
//!
//! Provides [`AppendExec`], a DataFusion [`ExecutionPlan`] that sequentially
//! concatenates the output of multiple child plans into a single output
//! partition. Unlike DataFusion's built-in `UnionExec`, `AppendExec` requires
//! each child to already have exactly one partition, producing a single
//! sequential stream that preserves per-child row ordering.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};

/// A physical execution plan that sequentially appends multiple single-partition
/// inputs into one output stream.
///
/// Each child plan must produce exactly one partition. The output is a single
/// partition that yields all batches from the first child, then all batches
/// from the second child, and so on.
///
/// This is used when scanning a Beacon table with multiple data files: each
/// file produces a single-partition scan, and `AppendExec` concatenates them.
#[derive(Debug)]
pub struct AppendExec {
    /// The child execution plans to concatenate.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Cached plan properties (single partition, incremental, bounded).
    props: PlanProperties,
    /// The shared output schema (must be identical across all children).
    schema: SchemaRef,
}

impl AppendExec {
    /// Creates a new `AppendExec` from a list of child execution plans.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `inputs` is empty.
    /// - Any child has a different schema than the first.
    /// - Any child has more than one output partition.
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        if inputs.is_empty() {
            return internal_err!("AppendExec requires at least one input");
        }

        let schema = inputs[0].schema();
        for input in &inputs[1..] {
            if input.schema() != schema {
                return internal_err!("All AppendExec inputs must have identical schemas");
            }
        }

        // Strongest contract: every child must already be 1 partition
        for input in &inputs {
            if input.output_partitioning().partition_count() != 1 {
                return internal_err!("AppendExec requires each input to have exactly 1 partition");
            }
        }

        // Build PlanProperties for 1 output partition.
        // Exact constructor details vary by DataFusion version.
        let eq = inputs[0].equivalence_properties().clone();
        let partitioning = Partitioning::UnknownPartitioning(1);
        let props = PlanProperties::new(
            eq,
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Ok(Self {
            inputs,
            props,
            schema,
        })
    }
}

impl DisplayAs for AppendExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AppendExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "AppendExec: {} inputs", self.inputs.len())
            }
        }
    }
}

impl ExecutionPlan for AppendExec {
    fn name(&self) -> &str {
        "AppendExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(children)?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.inputs.len()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("AppendExec has exactly 1 output partition");
        }

        let mut streams = Vec::with_capacity(self.inputs.len());
        for input in &self.inputs {
            streams.push(input.execute(0, context.clone())?);
        }

        let sendable_stream = Box::pin(AppendNStream::new(self.schema.clone(), streams));

        let record_batch_stream =
            RecordBatchStreamAdapter::new(self.schema.clone(), sendable_stream);

        Ok(Box::pin(record_batch_stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Internal stream that sequentially drains multiple [`SendableRecordBatchStream`]s.
///
/// Polls the current stream until it is exhausted, then advances to the next.
/// Once all streams have been consumed, the stream terminates.
struct AppendNStream {
    /// Schema of the output batches (retained for trait compliance).
    schema: SchemaRef,
    /// The child streams to drain in order.
    streams: Vec<SendableRecordBatchStream>,
    /// Index of the stream currently being polled.
    current: usize,
}

impl AppendNStream {
    fn new(schema: SchemaRef, streams: Vec<SendableRecordBatchStream>) -> Self {
        Self {
            schema,
            streams,
            current: 0,
        }
    }
}

impl Stream for AppendNStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        while this.current < this.streams.len() {
            match this.streams[this.current].poll_next_unpin(cx) {
                Poll::Ready(Some(batch)) => return Poll::Ready(Some(batch)),
                Poll::Ready(None) => this.current += 1,
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(None)
    }
}
