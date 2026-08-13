//! Decoder node: nd-encoded `RecordBatch`es from a child plan → nd batches.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};

use crate::nd::encoding::{decode_nd_record_batch_row, logical_schema, nd_batch_count};

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream};

/// Leaf of the nd pipeline: decodes the `beacon.nd`-encoded `RecordBatch`es
/// produced by a child plan (typically a `DataSourceExec` whose file opener
/// emits encoded batches) into un-broadcast [`NdRecordBatch`]es.
///
/// Its own output schema is the *logical* (decoded) schema; broadcasting to
/// flat Arrow happens in [`NdBroadcastExec`] above it.
#[derive(Debug, Clone)]
pub struct NdSourceExec {
    /// Child plan producing nd-encoded `RecordBatch`es.
    input: Arc<dyn ExecutionPlan>,
    /// Decoded (logical) output schema.
    logical_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdSourceExec {
    /// Wrap a child plan whose output columns are `beacon.nd`-encoded structs.
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let logical_schema = logical_schema(&input.schema())?;
        // Same partitioning/emission/boundedness as the child; only the schema
        // changes (encoded structs → logical value types).
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(logical_schema.clone())),
        );
        Ok(Self {
            input,
            logical_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for NdSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NdSourceExec")
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("NdSourceExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::try_new(input)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The source only produces nd batches (`execute_nd`); flattening to
        // Arrow lives in `NdBroadcastExec`. When executed as a standalone plan,
        // borrow that broadcast behaviour rather than duplicating it.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    /// Hand the filters to the file source below, which prunes on them.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        super::offer_filters_to_child(parent_filters, &self.input)
    }

    /// A decoder applies no predicate, so the `FilterExec` above stays.
    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(super::filters_stay_above(child_pushdown_result))
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
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let nd_batches: Count = MetricBuilder::new(&self.metrics).counter("nd_batches", partition);

        let input = self.input.execute(partition, context)?;
        let stream = input
            .map(move |item| {
                let _timer = baseline.elapsed_compute().timer();
                match item {
                    // An empty encoded batch carries no nd array — skip it.
                    Ok(batch) if batch.num_rows() == 0 => Ok(Vec::new()),
                    Ok(batch) => {
                        // One nd array per encoded row: a batch that has been
                        // through a coalescing operator (RoundRobinBatch
                        // repartitioning, `CoalesceBatchesExec`, …) carries
                        // several, and decoding only row 0 would silently drop
                        // the rest.
                        let count = nd_batch_count(&batch);
                        let mut decoded = Vec::with_capacity(count);
                        for row in 0..count {
                            let nd = decode_nd_record_batch_row(&batch, row)?;
                            nd_batches.add(1);
                            baseline.record_output(nd.num_rows());
                            decoded.push(nd);
                        }
                        Ok(decoded)
                    }
                    Err(e) => Err(e),
                }
            })
            .map_ok(|decoded| futures::stream::iter(decoded.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(stream))
    }
}
