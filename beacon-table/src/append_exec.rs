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

#[derive(Debug)]
pub struct AppendExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    props: PlanProperties,
    schema: SchemaRef,
}

impl AppendExec {
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

struct AppendNStream {
    schema: SchemaRef,
    streams: Vec<SendableRecordBatchStream>,
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
