//! Union of nd inputs: the grid form of `UNION ALL`.

use std::fmt;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use beacon_nd_encoding::batch::NdRecordBatch;

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream, as_nd_plan};

/// The partitions of all nd inputs in sequence.
///
/// The node changes no batch. Each nd batch keeps its own grid, so inputs with
/// different axis sizes union without a broadcast.
#[derive(Debug, Clone)]
pub struct NdUnionExec {
    /// nd-aware children, in partition order.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdUnionExec {
    /// Build a union over nd-aware `inputs`. All inputs must have one schema.
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        let Some(first) = inputs.first() else {
            return Err(DataFusionError::Plan(
                "NdUnionExec requires at least one input".to_string(),
            ));
        };
        let schema = first.schema();
        if let Some(other) = inputs.iter().find(|input| input.schema() != schema) {
            return Err(DataFusionError::Plan(format!(
                "NdUnionExec inputs must have one schema, got {schema:?} and {:?}",
                other.schema()
            )));
        }
        if let Some(flat) = inputs.iter().find(|input| as_nd_plan(input).is_none()) {
            return Err(DataFusionError::Plan(format!(
                "NdUnionExec requires nd-aware inputs, got {}",
                flat.name()
            )));
        }
        let partitions = inputs
            .iter()
            .map(|input| input.properties().output_partitioning().partition_count())
            .sum();
        let properties = Arc::new(
            first
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(schema))
                .with_partitioning(Partitioning::UnknownPartitioning(partitions)),
        );
        Ok(Self {
            inputs,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn inputs(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.inputs
    }

    /// Find the input that owns `partition` and the partition index in that input.
    fn locate(&self, partition: usize) -> Result<(&Arc<dyn ExecutionPlan>, usize)> {
        let mut rest = partition;
        for input in &self.inputs {
            let count = input.properties().output_partitioning().partition_count();
            if rest < count {
                return Ok((input, rest));
            }
            rest -= count;
        }
        Err(DataFusionError::Internal(format!(
            "NdUnionExec has no partition {partition}"
        )))
    }
}

impl DisplayAs for NdUnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NdUnionExec")
    }
}

impl ExecutionPlan for NdUnionExec {
    fn name(&self) -> &str {
        "NdUnionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // A standalone execution materializes the nd stream through a broadcast.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    /// The nd inputs must stay direct children. A repartition between them
    /// breaks the nd side channel.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl NdExecutionPlan for NdUnionExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        let (input, input_partition) = self.locate(partition)?;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = as_nd_plan(input)
            .expect("validated in try_new")
            .execute_nd(input_partition, context)?
            .map(move |item| -> Result<NdRecordBatch> {
                let batch = item?;
                baseline.record_output(batch.num_rows());
                Ok(batch)
            });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use futures::TryStreamExt;

    use beacon_nd_encoding::array::NdArrowArray;
    use beacon_nd_encoding::dimensions::{Dimension, Dimensions};
    use beacon_nd_encoding::encoding::encode_nd_record_batch;

    use super::super::NdSourceExec;
    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    /// An `NdSourceExec` over one nd chunk on the given grid, with one column
    /// `column` on the first axis.
    fn source(column: &str, target: &[(&str, usize)]) -> Arc<NdSourceExec> {
        let logical = Arc::new(Schema::new(vec![Field::new(column, DataType::Int32, true)]));
        let size = target[0].1;
        let values = NdArrowArray::try_new(
            Arc::new(Int32Array::from((0..size as i32).collect::<Vec<_>>())),
            dims(&target[..1]),
        )
        .unwrap();
        let nd = NdRecordBatch::try_new(logical, vec![values], dims(target)).unwrap();
        let encoded = encode_nd_record_batch(&nd).unwrap();
        let schema = encoded.schema();
        Arc::new(
            NdSourceExec::try_new(
                MemorySourceConfig::try_new_exec(&[vec![encoded]], schema, None).unwrap(),
            )
            .unwrap(),
        )
    }

    /// A union of a time=2 source and a time=3 source.
    fn two_grids() -> NdUnionExec {
        let short: Arc<dyn ExecutionPlan> = source("time", &[("time", 2)]);
        let long: Arc<dyn ExecutionPlan> = source("time", &[("time", 3)]);
        NdUnionExec::try_new(vec![short, long]).unwrap()
    }

    fn context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    #[tokio::test]
    async fn each_partition_keeps_its_own_grid() {
        let union = two_grids();
        assert_eq!(union.properties().output_partitioning().partition_count(), 2);

        let first: Vec<NdRecordBatch> = union
            .execute_nd(0, context())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].num_rows(), 2);
        assert_eq!(first[0].target(), &dims(&[("time", 2)]));

        let second: Vec<NdRecordBatch> = union
            .execute_nd(1, context())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].num_rows(), 3);
        assert_eq!(second[0].target(), &dims(&[("time", 3)]));
    }

    #[test]
    fn partition_out_of_range_is_an_error() {
        assert!(two_grids().execute_nd(2, context()).is_err());
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        assert!(NdUnionExec::try_new(vec![]).is_err());

        let time: Arc<dyn ExecutionPlan> = source("time", &[("time", 2)]);
        let depth: Arc<dyn ExecutionPlan> = source("depth", &[("depth", 2)]);
        assert!(NdUnionExec::try_new(vec![time, depth]).is_err());

        let flat = source("time", &[("time", 2)]).input().clone();
        let err = NdUnionExec::try_new(vec![flat]).unwrap_err();
        assert!(err.to_string().contains("nd-aware"), "{err}");
    }

    #[tokio::test]
    async fn flat_execute_materializes_the_partition() {
        let batches = two_grids()
            .execute(1, context())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
