use std::sync::Arc;

use datafusion::{
    common::pruning::PruningStatistics, physical_expr_adapter::PhysicalExprAdapter,
    physical_optimizer::pruning::PruningPredicate, physical_plan::PhysicalExpr,
};
use object_store::ObjectStore;

use crate::datafusion::opener::AtlasGlobalMetrics;
use crate::partition::{Partition, ops::statistics::PartitionStatistics};

pub async fn prune_partition<S: ObjectStore + Clone>(
    partition: &Partition<S>,
    projection: &[usize],
    predicate: Arc<dyn PhysicalExpr>,
    expr_adapter: Arc<dyn PhysicalExprAdapter>,
) -> Option<Vec<bool>> {
    prune_partition_with_metrics(partition, projection, predicate, expr_adapter, None).await
}

pub async fn prune_partition_with_metrics<S: ObjectStore + Clone>(
    partition: &Partition<S>,
    projection: &[usize],
    predicate: Arc<dyn PhysicalExpr>,
    expr_adapter: Arc<dyn PhysicalExprAdapter>,
    metrics: Option<&AtlasGlobalMetrics>,
) -> Option<Vec<bool>> {
    if let Some(metrics) = metrics {
        metrics.add_pruning_attempt();
    }

    let partition_schema = Arc::new(partition.arrow_schema());
    let projected_partition_schema = Arc::new(partition_schema.project(projection).ok()?);
    let columns = projected_partition_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let statistics = PartitionStatistics::new(partition, &columns).await;

    let adapted_predicate = expr_adapter.rewrite(predicate).ok()?;
    let pruning_predicate =
        PruningPredicate::try_new(adapted_predicate, projected_partition_schema).ok()?;

    let prune_result = pruning_predicate.prune(&statistics).ok()?;
    if let Some(metrics) = metrics {
        let total_containers = prune_result.len();
        let pruned_containers = prune_result.iter().filter(|keep| !**keep).count();

        metrics.add_pruning_total_containers(total_containers);
        metrics.add_pruning_pruned_containers(pruned_containers);

        if pruned_containers > 0 {
            metrics.add_pruning_effective();
        }
    }

    Some(prune_result)
}

impl PruningStatistics for PartitionStatistics {
    fn min_values(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        self.fetched_statistics
            .get(column.name())
            .and_then(|batch| batch.column_by_name("min").cloned())
    }

    fn max_values(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        self.fetched_statistics
            .get(column.name())
            .and_then(|batch| batch.column_by_name("max").cloned())
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn null_counts(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        self.fetched_statistics
            .get(column.name())
            .and_then(|batch| batch.column_by_name("null_count").cloned())
    }

    fn row_counts(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        self.fetched_statistics
            .get(column.name())
            .and_then(|batch| batch.column_by_name("row_count").cloned())
    }

    /// Returns `None` to indicate that the pruning predicate should not be applied, as we don't have the necessary statistics yet.
    fn contained(
        &self,
        _column: &datafusion::prelude::Column,
        _values: &std::collections::HashSet<datafusion::scalar::ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        None
    }
}
