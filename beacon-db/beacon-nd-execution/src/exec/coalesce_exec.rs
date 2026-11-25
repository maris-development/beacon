//! Merge the partitions of an nd input into one nd partition.

use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream, as_nd_plan};

/// Merges all partitions of an nd input into one nd partition.
///
/// This is the grid form of DataFusion's `CoalescePartitionsExec`. The batches
/// come in the order in which the input partitions yield them.
#[derive(Debug, Clone)]
pub struct NdCoalescePartitionsExec {
    /// nd-aware child whose partitions this node merges.
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdCoalescePartitionsExec {
    /// Build a merge over an nd-aware `input`.
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if as_nd_plan(&input).is_none() {
            return Err(DataFusionError::Plan(format!(
                "NdCoalescePartitionsExec requires an nd-aware input, got {}",
                input.name()
            )));
        }
        // The merge interleaves the partitions, so no order survives.
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(input.schema()))
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        );
        Ok(Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The nd-aware child whose partitions this node merges.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for NdCoalescePartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NdCoalescePartitionsExec")
    }
}

impl ExecutionPlan for NdCoalescePartitionsExec {
    fn name(&self) -> &str {
        "NdCoalescePartitionsExec"
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
            DataFusionError::Internal(
                "NdCoalescePartitionsExec expects exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::try_new(input)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    /// The nd child must stay a direct child.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// Pass the filters down the nd pipeline, towards the file source that can
    /// prune on them.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        super::offer_filters_to_child(parent_filters, &self.input)
    }

    /// The merge applies no predicate, so the `FilterExec` above stays.
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

impl NdExecutionPlan for NdCoalescePartitionsExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "NdCoalescePartitionsExec has one partition, got partition {partition}"
            )));
        }
        let nd_input = as_nd_plan(&self.input).expect("validated in try_new");
        let streams = (0..self.input.output_partitioning().partition_count())
            .map(|input_partition| nd_input.execute_nd(input_partition, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::select_all(streams).map(move |item| {
            let batch = item?;
            baseline.record_output(batch.num_rows());
            Ok(batch)
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::displayable;
    use futures::TryStreamExt;

    use beacon_nd_encoding::array::NdArrowArray;
    use beacon_nd_encoding::batch::NdRecordBatch;
    use beacon_nd_encoding::dimensions::{Dimension, Dimensions};
    use beacon_nd_encoding::encoding::encode_nd_record_batch;

    use super::super::{NdFilterExec, NdSourceExec};
    use super::*;

    /// One encoded nd chunk with a `time` column on a grid of `size` cells.
    fn chunk(size: usize) -> arrow::record_batch::RecordBatch {
        let logical = Arc::new(Schema::new(vec![Field::new("time", DataType::Int32, true)]));
        let dims = || Dimensions::try_new(vec![Dimension::new("time", size)]).unwrap();
        let time = NdArrowArray::try_new(
            Arc::new(Int32Array::from((0..size as i32).collect::<Vec<_>>())),
            dims(),
        )
        .unwrap();
        let nd = NdRecordBatch::try_new(logical, vec![time], dims()).unwrap();
        encode_nd_record_batch(&nd).unwrap()
    }

    /// An `NdSourceExec` with two partitions: time=2 and time=3.
    fn two_partition_source() -> Arc<NdSourceExec> {
        let enc_a = chunk(2);
        let enc_b = chunk(3);
        let schema = enc_a.schema();
        Arc::new(
            NdSourceExec::try_new(
                MemorySourceConfig::try_new_exec(&[vec![enc_a], vec![enc_b]], schema, None)
                    .unwrap(),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn merges_all_partitions_into_one() {
        let source = two_partition_source();
        assert_eq!(source.properties().partitioning.partition_count(), 2);

        let plan = NdCoalescePartitionsExec::try_new(source).unwrap();
        assert_eq!(plan.properties().partitioning.partition_count(), 1);

        let batches: Vec<NdRecordBatch> = plan
            .execute_nd(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn rejects_a_partition_other_than_zero() {
        let plan = NdCoalescePartitionsExec::try_new(two_partition_source()).unwrap();
        assert!(plan.execute_nd(1, Arc::new(TaskContext::default())).is_err());
    }

    #[tokio::test]
    async fn rejects_a_flat_input() {
        let source = two_partition_source();
        assert!(NdCoalescePartitionsExec::try_new(source.input().clone()).is_err());
    }

    /// The nd batches of the one output partition of a merge over `input`.
    async fn merged(input: Arc<dyn ExecutionPlan>) -> Vec<NdRecordBatch> {
        NdCoalescePartitionsExec::try_new(input)
            .unwrap()
            .execute_nd(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn keeps_each_batch_on_its_own_grid() {
        let batches = merged(two_partition_source()).await;
        assert!(batches.iter().all(|batch| batch.selection().is_none()));
        let mut rows: Vec<usize> = batches.iter().map(|batch| batch.num_rows()).collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 3]);
    }

    #[tokio::test]
    async fn keeps_an_inbound_selection() {
        let source = two_partition_source();
        let schema = source.schema();
        // `time > 0` keeps cell [1] of the time=2 chunk and [1, 2] of the time=3 chunk.
        let predicate = binary(
            col("time", &schema).unwrap(),
            Operator::Gt,
            lit(0i32),
            &schema,
        )
        .unwrap();
        let filter = Arc::new(NdFilterExec::try_new(source, vec![predicate]).unwrap());

        let mut selections: Vec<Vec<u64>> = merged(filter)
            .await
            .iter()
            .map(|batch| batch.selection().expect("a selection").values().to_vec())
            .collect();
        selections.sort();
        assert_eq!(selections, vec![vec![1], vec![1, 2]]);
    }

    #[tokio::test]
    async fn one_input_partition_keeps_the_batch_order() {
        let first = chunk(2);
        let second = chunk(3);
        let schema = first.schema();
        let source = Arc::new(
            NdSourceExec::try_new(
                MemorySourceConfig::try_new_exec(&[vec![first, second]], schema, None).unwrap(),
            )
            .unwrap(),
        );

        let rows: Vec<usize> = merged(source)
            .await
            .iter()
            .map(|batch| batch.num_rows())
            .collect();
        assert_eq!(rows, vec![2, 3]);
    }

    #[tokio::test]
    async fn flat_execute_gives_the_cells_of_all_partitions() {
        let plan = NdCoalescePartitionsExec::try_new(two_partition_source()).unwrap();
        let batches: Vec<_> = plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut time: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        time.sort_unstable();
        assert_eq!(time, vec![0, 0, 1, 1, 2]);
    }

    #[tokio::test]
    async fn metrics_count_the_output_rows() {
        let plan = NdCoalescePartitionsExec::try_new(two_partition_source()).unwrap();
        let batches: Vec<NdRecordBatch> = plan
            .execute_nd(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(plan.metrics().unwrap().output_rows(), Some(5));
    }

    #[test]
    fn with_new_children_builds_the_merge_again() {
        let plan = Arc::new(NdCoalescePartitionsExec::try_new(two_partition_source()).unwrap());

        let rebuilt = plan
            .clone()
            .with_new_children(vec![two_partition_source()])
            .unwrap();
        assert_eq!(rebuilt.name(), "NdCoalescePartitionsExec");
        assert_eq!(rebuilt.properties().partitioning.partition_count(), 1);

        assert!(plan.clone().with_new_children(vec![]).is_err());
        let flat = two_partition_source().input().clone();
        assert!(plan.with_new_children(vec![flat]).is_err());
    }

    #[test]
    fn display_gives_the_node_name() {
        let plan = NdCoalescePartitionsExec::try_new(two_partition_source()).unwrap();
        let line = displayable(&plan).one_line().to_string();
        assert_eq!(line.trim(), "NdCoalescePartitionsExec");
    }
}
