//! Physical operators that keep nd batches un-broadcast between them.
//!
//! DataFusion streams only flat `RecordBatch`es between generic operators, so
//! nd-aware operators exchange [`NdRecordBatch`]es through a side channel: the
//! [`NdExecutionPlan`] trait's `execute_nd`. [`as_nd_plan`] recovers that trait
//! from an `Arc<dyn ExecutionPlan>` child. Every nd operator's standard
//! `execute` still yields flat batches (by materializing), so any nd node is
//! also a correct plan on its own.

mod broadcast_exec;
mod projection_exec;
mod source_exec;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, Count};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use futures::stream::BoxStream;

use super::batch::NdRecordBatch;

pub use broadcast_exec::NdBroadcastExec;
pub use projection_exec::NdProjectionExec;
pub use source_exec::NdSourceExec;

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
    if let Some(projection) = any.downcast_ref::<NdProjectionExec>() {
        return Some(Arc::new(projection.clone()));
    }
    None
}

/// Adapt an nd batch stream into a standard record batch stream, dropping
/// empty batches and recording output rows / materialization time into
/// `baseline`. Per batch, each column is either broadcast with a gather
/// (counted in `broadcasts`) or passed through zero-copy because it is already
/// at full rank (counted in `passthroughs`).
pub fn materialize_nd_stream(
    schema: arrow::datatypes::SchemaRef,
    stream: SendableNdBatchStream,
    baseline: BaselineMetrics,
    broadcasts: Count,
    passthroughs: Count,
) -> SendableRecordBatchStream {
    // `baseline` is moved into this synchronous closure once and borrowed per
    // item; it is dropped when the stream ends, which records the end time.
    let batches = stream
        .map(move |item| -> Option<Result<RecordBatch>> {
            match item {
                Ok(batch) => {
                    if batch.num_rows() == 0 {
                        return None;
                    }
                    let _timer = baseline.elapsed_compute().timer();
                    match batch.materialize_with_stats() {
                        Ok((record_batch, batch_broadcasts, batch_passthroughs)) => {
                            baseline.record_output(record_batch.num_rows());
                            broadcasts.add(batch_broadcasts);
                            passthroughs.add(batch_passthroughs);
                            Some(Ok(record_batch))
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            }
        })
        .filter_map(|item| async move { item });
    Box::pin(RecordBatchStreamAdapter::new(schema, batches))
}
