//! Limit over nd batches.
//!
//! The limit cuts the selection of each nd batch and moves no column data. It
//! ends the stream when the fetch count reaches zero, so the scan below stops
//! early.

use std::fmt;
use std::sync::Arc;

use arrow::array::UInt64Array;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use beacon_nd_encoding::batch::NdRecordBatch;

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream, as_nd_plan};

/// Skips the first `skip` kept cells and keeps the next `fetch` cells, per
/// partition, in stream order.
///
/// The cut is a slice of the batch selection, so no column data moves. The node
/// requires an nd-aware child and is itself nd-aware.
///
/// The node keeps the default filter pushdown hooks. A scan that prunes a chunk
/// below a limit changes the cells that the limit keeps.
#[derive(Debug, Clone)]
pub struct NdLimitExec {
    /// nd-aware child that gives the input nd batches.
    input: Arc<dyn ExecutionPlan>,
    /// Number of kept cells to skip before the first output cell.
    skip: usize,
    /// Maximum number of output cells, or `None` for no maximum.
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdLimitExec {
    /// Build a limit over an nd-aware `input`. The output schema is the input
    /// schema.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Self> {
        if as_nd_plan(&input).is_none() {
            return Err(DataFusionError::Plan(format!(
                "NdLimitExec requires an nd-aware input, got {}",
                input.name()
            )));
        }
        Ok(Self {
            input,
            skip,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn skip(&self) -> usize {
        self.skip
    }
}

/// Keep `take` rows of `batch` after the first `skipped` rows.
///
/// The cut is a slice of the inbound selection, or an index range on the full
/// grid when the batch has no selection.
fn cut_batch(batch: NdRecordBatch, skipped: usize, take: usize) -> Result<NdRecordBatch> {
    if skipped == 0 && take == batch.num_rows() {
        return Ok(batch);
    }
    let cut = match batch.selection() {
        Some(selection) => selection.slice(skipped, take),
        None => UInt64Array::from_iter_values(skipped as u64..(skipped + take) as u64),
    };
    batch.with_selection(Some(cut))
}

impl DisplayAs for NdLimitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fetch = self
            .fetch
            .map_or_else(|| "None".to_string(), |fetch| fetch.to_string());
        write!(f, "NdLimitExec: skip={}, fetch={fetch}", self.skip)
    }
}

impl ExecutionPlan for NdLimitExec {
    fn name(&self) -> &str {
        "NdLimitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("NdLimitExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::try_new(input, self.skip, self.fetch)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // A standalone execution wraps this node in an `NdBroadcastExec` to materialize.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The nd child must stay a direct child.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
}

impl NdExecutionPlan for NdLimitExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let input = as_nd_plan(&self.input)
            .expect("validated in try_new")
            .execute_nd(partition, context)?;
        let state = (input, self.skip, self.fetch.unwrap_or(usize::MAX));
        let stream = futures::stream::unfold(state, move |(mut input, mut skip, mut fetch)| {
            let baseline = baseline.clone();
            async move {
                // The input is not polled after the fetch is spent.
                while fetch > 0 {
                    let batch = match input.next().await? {
                        Ok(batch) => batch,
                        Err(error) => return Some((Err(error), (input, skip, 0))),
                    };
                    let _timer = baseline.elapsed_compute().timer();
                    let rows = batch.num_rows();
                    let skipped = skip.min(rows);
                    let take = (rows - skipped).min(fetch);
                    skip -= skipped;
                    fetch -= take;
                    if take == 0 {
                        continue;
                    }
                    baseline.record_output(take);
                    let cut = cut_batch(batch, skipped, take);
                    return Some((cut, (input, skip, fetch)));
                }
                None
            }
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use futures::TryStreamExt;

    use beacon_nd_encoding::array::NdArrowArray;
    use beacon_nd_encoding::dimensions::{Dimension, Dimensions};
    use beacon_nd_encoding::encoding::encode_nd_record_batch;

    use super::super::{NdFilterExec, NdSourceExec};
    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    /// One encoded nd chunk with a `time` column on the grid (time=values.len()).
    fn chunk(values: Vec<i32>) -> RecordBatch {
        let logical: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("time", DataType::Int32, true)]));
        let size = values.len();
        let time =
            NdArrowArray::try_new(Arc::new(Int32Array::from(values)), dims(&[("time", size)]))
                .unwrap();
        let nd = NdRecordBatch::try_new(logical, vec![time], dims(&[("time", size)])).unwrap();
        encode_nd_record_batch(&nd).unwrap()
    }

    /// An `NdSourceExec` with one partition of two chunks: time=4 with values
    /// 0..=3, then time=3 with values 10..=12.
    fn source() -> Arc<NdSourceExec> {
        let first = chunk(vec![0, 1, 2, 3]);
        let second = chunk(vec![10, 11, 12]);
        let schema = first.schema();
        Arc::new(
            NdSourceExec::try_new(
                MemorySourceConfig::try_new_exec(&[vec![first, second]], schema, None).unwrap(),
            )
            .unwrap(),
        )
    }

    /// The nd batches of partition 0 after a limit over `input`.
    async fn limited(
        input: Arc<dyn ExecutionPlan>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Vec<NdRecordBatch> {
        NdLimitExec::try_new(input, skip, fetch)
            .unwrap()
            .execute_nd(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    fn selection(batch: &NdRecordBatch) -> Vec<u64> {
        batch.selection().expect("a selection").values().to_vec()
    }

    #[tokio::test]
    async fn fetch_inside_the_first_chunk_ends_the_stream() {
        let batches = limited(source(), 0, Some(2)).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(selection(&batches[0]), vec![0, 1]);
    }

    #[tokio::test]
    async fn cut_crosses_the_chunk_border() {
        let batches = limited(source(), 3, Some(3)).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(selection(&batches[0]), vec![3]);
        assert_eq!(selection(&batches[1]), vec![0, 1]);
    }

    #[tokio::test]
    async fn no_skip_and_no_fetch_passes_batches_unchanged() {
        let batches = limited(source(), 0, None).await;
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.selection().is_none()));
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 3);
    }

    #[tokio::test]
    async fn skip_past_the_first_chunk_keeps_the_tail() {
        let batches = limited(source(), 5, Some(10)).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(selection(&batches[0]), vec![1, 2]);

        assert!(limited(source(), 7, Some(10)).await.is_empty());
    }

    #[tokio::test]
    async fn cut_slices_an_inbound_selection() {
        let source = source();
        let schema = source.schema();
        // `time > 0` keeps cells [1, 2, 3] of chunk one and [0, 1, 2] of chunk two.
        let predicate = binary(
            col("time", &schema).unwrap(),
            Operator::Gt,
            lit(0i32),
            &schema,
        )
        .unwrap();
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(NdFilterExec::try_new(source, vec![predicate]).unwrap());

        let batches = limited(filter, 1, Some(3)).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(selection(&batches[0]), vec![2, 3]);
        assert_eq!(selection(&batches[1]), vec![0]);
    }

    #[tokio::test]
    async fn flat_execute_gives_the_kept_cells() {
        let plan = NdLimitExec::try_new(source(), 3, Some(3)).unwrap();
        let batches = plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let time: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(time, vec![3, 10, 11]);
    }

    #[test]
    fn try_new_rejects_a_flat_input() {
        let source = source();
        assert!(NdLimitExec::try_new(source.input().clone(), 0, None).is_err());
    }
}
