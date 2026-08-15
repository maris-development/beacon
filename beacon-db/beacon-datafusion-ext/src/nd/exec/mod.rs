//! Physical operators that keep nd batches un-broadcast between them.
//!
//! DataFusion streams only flat `RecordBatch`es between generic operators, so
//! nd-aware operators exchange [`NdRecordBatch`]es through a side channel: the
//! [`NdExecutionPlan`] trait's `execute_nd`. [`as_nd_plan`] recovers that trait
//! from an `Arc<dyn ExecutionPlan>` child. Every nd operator's standard
//! `execute` still yields flat batches (by materializing), so any nd node is
//! also a correct plan on its own.

mod broadcast_exec;
mod expr_column;
mod filter_exec;
mod projection_exec;
mod source_exec;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, Count};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use futures::stream::BoxStream;

use super::batch::NdRecordBatch;

pub use broadcast_exec::NdBroadcastExec;
pub use filter_exec::NdFilterExec;
pub use projection_exec::NdProjectionExec;
pub use source_exec::NdSourceExec;

/// Stream of nd batches exchanged between nd-aware operators.
pub type SendableNdBatchStream = BoxStream<'static, Result<NdRecordBatch>>;

/// Offer a parent's filters to the one child, as a hint and nothing more.
///
/// An nd operator cannot evaluate a flat predicate on nd batches, so it never
/// claims one. It does let the predicate travel: a file source underneath reads
/// the columns a `WHERE` names off it and skips the chunks whose coordinates
/// cannot satisfy it, which is the difference between reading a grid and reading
/// the part of it a query asked for.
///
/// The filters pass through unchanged. An nd column is the encoded form of the
/// logical column of the same name, in the same position, so a `Column` means
/// the same thing on either side of the encoding.
///
/// Pair this with [`filters_stay_above`], which is what makes it safe.
pub(crate) fn offer_filters_to_child(
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    child: &Arc<dyn ExecutionPlan>,
) -> Result<FilterDescription> {
    FilterDescription::from_children(parent_filters, &[child])
}

/// Tell the parent that none of its filters were applied, whatever the child
/// said.
///
/// The child was given the filters to prune with, not to apply. A source that
/// claimed one would leave the rows it dropped unaccounted for, because an nd
/// operator between them cannot re-apply a flat predicate. Reporting everything
/// unsupported keeps the `FilterExec` above this node, so the predicate is
/// always evaluated exactly once, on flat rows.
pub(crate) fn filters_stay_above<T>(
    child_pushdown_result: ChildPushdownResult,
) -> FilterPushdownPropagation<T> {
    FilterPushdownPropagation::all_unsupported(child_pushdown_result)
}

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
    if let Some(filter) = any.downcast_ref::<NdFilterExec>() {
        return Some(Arc::new(filter.clone()));
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

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;

    use crate::nd::array::NdArrowArray;
    use crate::nd::dimensions::{Dimension, Dimensions};
    use crate::nd::encoding::encode_nd_record_batch;

    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    /// An `NdSourceExec` over a single nd chunk on the given grid.
    fn source(target: &[(&str, usize)]) -> Arc<NdSourceExec> {
        let logical = Arc::new(Schema::new(vec![Field::new("time", DataType::Int32, true)]));
        let size = target[0].1;
        let time = NdArrowArray::try_new(
            Arc::new(Int32Array::from((0..size as i32).collect::<Vec<_>>())),
            dims(&target[..1]),
        )
        .unwrap();
        let nd = NdRecordBatch::try_new(logical, vec![time], dims(target)).unwrap();
        let encoded = encode_nd_record_batch(&nd).unwrap();
        let schema = encoded.schema();
        Arc::new(
            NdSourceExec::try_new(
                MemorySourceConfig::try_new_exec(&[vec![encoded]], schema, None).unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn as_nd_plan_recognizes_only_nd_aware_nodes() {
        let source = source(&[("time", 2)]);
        let plan: Arc<dyn ExecutionPlan> = source.clone();
        assert!(as_nd_plan(&plan).is_some());

        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(NdProjectionExec::try_new(plan.clone(), vec![]).unwrap());
        assert!(as_nd_plan(&projection).is_some());

        // The broadcast is the terminal node: it flattens, so it is *not* an nd
        // producer and must not be treated as one.
        let broadcast: Arc<dyn ExecutionPlan> =
            Arc::new(NdBroadcastExec::try_new(plan.clone()).unwrap());
        assert!(as_nd_plan(&broadcast).is_none());

        // …and neither is the underlying flat child.
        assert!(as_nd_plan(source.input()).is_none());
    }

    /// An empty grid carries no rows, so the materializing adapter must drop the
    /// batch entirely rather than emit a zero-row `RecordBatch`.
    #[tokio::test]
    async fn empty_grids_are_dropped_instead_of_emitted() {
        let plan = Arc::new(NdBroadcastExec::try_new(source(&[("time", 0)])).unwrap());
        let batches: Vec<_> = plan
            .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(batches.is_empty(), "expected no batches, got {batches:?}");

        // No rows were produced, so the broadcast reports none.
        assert_eq!(plan.metrics().unwrap().output_rows(), Some(0));
    }
}
