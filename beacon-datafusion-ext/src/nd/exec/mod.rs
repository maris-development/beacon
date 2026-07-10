//! Physical operators that keep nd batches un-broadcast between them.
//!
//! DataFusion streams only flat `RecordBatch`es between generic operators, so
//! nd-aware operators exchange [`NdRecordBatch`]es through a side channel: the
//! [`NdExecutionPlan`] trait's `execute_nd`. [`as_nd_plan`] recovers that trait
//! from an `Arc<dyn ExecutionPlan>` child. Every nd operator's standard
//! `execute` still yields flat batches (by materializing), so any nd node is
//! also a correct plan on its own.

mod broadcast_exec;
mod source_exec;

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use futures::stream::BoxStream;

use super::batch::NdRecordBatch;

pub use broadcast_exec::NdBroadcastExec;
pub use source_exec::{MemoryNdBatchProvider, NdBatchProvider, NdSourceExec};

/// Stream of nd batches exchanged between nd-aware operators.
pub type SendableNdBatchStream = BoxStream<'static, Result<NdRecordBatch>>;

/// An [`ExecutionPlan`] that can additionally stream nd batches.
pub trait NdExecutionPlan: ExecutionPlan {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream>;
}

/// Recover the nd side channel from a plan node. Extend this list when a new
/// nd-aware operator is added; external crates plug in through
/// [`NdBatchProvider`] + [`NdSourceExec`] rather than new node types.
pub fn as_nd_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn NdExecutionPlan>> {
    let any = plan.as_any();
    if let Some(source) = any.downcast_ref::<NdSourceExec>() {
        return Some(Arc::new(source.clone()));
    }
    None
}

/// Adapt an nd batch stream into a standard record batch stream, dropping
/// empty batches.
pub fn materialize_nd_stream(
    schema: arrow::datatypes::SchemaRef,
    stream: SendableNdBatchStream,
) -> SendableRecordBatchStream {
    let batches = stream.filter_map(|item| async move {
        match item {
            Ok(batch) => {
                if batch.num_rows() == 0 {
                    None
                } else {
                    Some(batch.materialize())
                }
            }
            Err(e) => Some(Err(e)),
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, batches))
}
