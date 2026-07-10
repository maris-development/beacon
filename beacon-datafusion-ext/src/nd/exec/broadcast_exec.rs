//! Terminal materializer of the nd pipeline.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::{as_nd_plan, materialize_nd_stream};

/// Boundary between the nd pipeline and the rest of the plan: pulls nd
/// batches from its child and emits flat, fully-broadcast `RecordBatch`es.
/// Broadcast and any accumulated selection compose into a single gather per
/// column, so the unfiltered cross-product never exists in memory.
#[derive(Debug, Clone)]
pub struct NdBroadcastExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NdBroadcastExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if as_nd_plan(&input).is_none() {
            return plan_err!(
                "NdBroadcastExec requires an nd-aware input, got {}",
                input.name()
            );
        }
        let properties = input.properties().clone();
        Ok(Self { input, properties })
    }

    /// The nd-aware child whose batches this node materializes.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for NdBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NdBroadcastExec")
    }
}

impl ExecutionPlan for NdBroadcastExec {
    fn name(&self) -> &str {
        "NdBroadcastExec"
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
            DataFusionError::Internal("NdBroadcastExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::try_new(input)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = as_nd_plan(&self.input)
            .expect("validated in try_new")
            .execute_nd(partition, context)?;
        Ok(materialize_nd_stream(self.input.schema(), stream))
    }
}
