//! Source node bridging arbitrary nd batch producers into the nd pipeline.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream};

/// Producer of nd batch streams. Format crates (zarr, netcdf, …) implement
/// this instead of defining their own `ExecutionPlan`, so [`super::as_nd_plan`]
/// only ever needs to know about [`NdSourceExec`].
pub trait NdBatchProvider: fmt::Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn partition_count(&self) -> usize {
        1
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream>;
}

/// Leaf node that streams nd batches from an [`NdBatchProvider`].
#[derive(Debug, Clone)]
pub struct NdSourceExec {
    provider: Arc<dyn NdBatchProvider>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdSourceExec {
    pub fn new(provider: Arc<dyn NdBatchProvider>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(provider.schema()),
            Partitioning::UnknownPartitioning(provider.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn provider(&self) -> &Arc<dyn NdBatchProvider> {
        &self.provider
    }
}

impl DisplayAs for NdSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NdSourceExec: partitions={}",
            self.provider.partition_count()
        )
    }
}

impl ExecutionPlan for NdSourceExec {
    fn name(&self) -> &str {
        "NdSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The source only produces nd batches (`execute_nd`); flattening to
        // Arrow lives in `NdBroadcastExec`. When this node is executed as a
        // standalone plan, borrow that broadcast behaviour rather than
        // duplicating it here.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl NdExecutionPlan for NdSourceExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        // `output_rows` counts the un-broadcast grid rows each nd batch
        // represents; `nd_batches` counts the batches produced. `baseline` is
        // moved into the closure once and dropped at stream end, recording the
        // elapsed time.
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let nd_batches: Count =
            MetricBuilder::new(&self.metrics).counter("nd_batches", partition);

        let stream = self.provider.execute(partition, context)?.map(move |item| {
            let _timer = baseline.elapsed_compute().timer();
            if let Ok(batch) = &item {
                baseline.record_output(batch.num_rows());
                nd_batches.add(1);
            }
            item
        });
        Ok(Box::pin(stream))
    }
}

/// In-memory provider, useful for tests and for callers that already hold
/// nd batches.
#[derive(Debug)]
pub struct MemoryNdBatchProvider {
    schema: SchemaRef,
    batches: Vec<crate::nd::NdRecordBatch>,
}

impl MemoryNdBatchProvider {
    pub fn new(schema: SchemaRef, batches: Vec<crate::nd::NdRecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl NdBatchProvider for MemoryNdBatchProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        Ok(Box::pin(futures::stream::iter(
            self.batches.clone().into_iter().map(Ok),
        )))
    }
}
