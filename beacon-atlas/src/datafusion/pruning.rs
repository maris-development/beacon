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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, BooleanArray, Float32Array, StringArray},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::DFSchema,
        common::DataFusionError,
        physical_expr::create_physical_expr,
        physical_expr_adapter::{DefaultPhysicalExprAdapter, PhysicalExprAdapter},
        physical_plan::PhysicalExpr,
        prelude::{SessionContext, col, lit},
    };
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::{
        collection::AtlasCollection, column::Column, partition::Partition,
        schema::AtlasSuperTypingMode,
    };

    use super::prune_partition;

    async fn build_partition_with_temperature_entries()
    -> anyhow::Result<Partition<Arc<dyn ObjectStore>>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/pruning-tests");

        let mut collection = AtlasCollection::create(
            store,
            collection_path,
            "pruning-tests",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let mut writer = collection.create_partition("part-00000", None).await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10_i32, 20_i32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-1",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![30_i32, 40_i32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        collection.get_partition("part-00000").await
    }

    async fn build_partition_with_f32_temperature_entries()
    -> anyhow::Result<Partition<Arc<dyn ObjectStore>>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/pruning-tests-f32");

        let mut collection = AtlasCollection::create(
            store,
            collection_path,
            "pruning-tests-f32",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let mut writer = collection.create_partition("part-00000", None).await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10.0_f32, 20.0_f32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-1",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![30.0_f32, 40.0_f32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        collection.get_partition("part-00000").await
    }

    fn build_temperature_predicate(
        schema: Arc<arrow::datatypes::Schema>,
        threshold: i32,
    ) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(schema)?;
        let session = SessionContext::new();
        let state = session.state();

        Ok(create_physical_expr(
            &col("temperature").gt(lit(threshold)),
            &df_schema,
            state.execution_props(),
        )?)
    }

    fn build_temperature_predicate_f64(
        schema: Arc<arrow::datatypes::Schema>,
        threshold: f64,
    ) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(schema)?;
        let session = SessionContext::new();
        let state = session.state();

        Ok(create_physical_expr(
            &col("temperature").gt(lit(threshold)),
            &df_schema,
            state.execution_props(),
        )?)
    }

    #[tokio::test]
    async fn pruning_keeps_only_matching_inserted_datasets() -> anyhow::Result<()> {
        let partition = build_partition_with_temperature_entries().await?;
        let schema = Arc::new(partition.arrow_schema());
        let projection = vec![schema.index_of("temperature")?];
        let predicate = build_temperature_predicate(schema.clone(), 25)?;
        let expr_adapter = Arc::from(DefaultPhysicalExprAdapter::new(
            schema.clone(),
            schema.clone(),
        ));

        let prune_result = prune_partition(&partition, &projection, predicate, expr_adapter)
            .await
            .expect("pruning should be evaluated");

        assert_eq!(prune_result, vec![false, true]);
        Ok(())
    }

    #[tokio::test]
    async fn pruning_retains_all_datasets_when_all_match() -> anyhow::Result<()> {
        let partition = build_partition_with_temperature_entries().await?;
        let schema = Arc::new(partition.arrow_schema());
        let projection = vec![schema.index_of("temperature")?];
        let predicate = build_temperature_predicate(schema.clone(), 5)?;
        let expr_adapter = Arc::from(DefaultPhysicalExprAdapter::new(
            schema.clone(),
            schema.clone(),
        ));

        let prune_result = prune_partition(&partition, &projection, predicate, expr_adapter)
            .await
            .expect("pruning should be evaluated");

        assert_eq!(prune_result, vec![true, true]);
        Ok(())
    }

    #[tokio::test]
    async fn pruning_uses_expr_adapter_for_projected_schema() -> anyhow::Result<()> {
        let partition = build_partition_with_temperature_entries().await?;
        let table_schema = Arc::new(partition.arrow_schema());
        let projection = vec![table_schema.index_of("temperature")?];
        let projected_schema = Arc::new(table_schema.project(&projection)?);
        let predicate = build_temperature_predicate(table_schema.clone(), 25)?;
        let expr_adapter = Arc::from(DefaultPhysicalExprAdapter::new(
            projected_schema,
            table_schema,
        ));

        let prune_result = prune_partition(&partition, &projection, predicate, expr_adapter)
            .await
            .expect("pruning should be evaluated");

        assert_eq!(prune_result, vec![false, true]);
        Ok(())
    }

    #[derive(Debug)]
    struct FailingExprAdapter;

    impl PhysicalExprAdapter for FailingExprAdapter {
        fn rewrite(
            &self,
            _expr: Arc<dyn PhysicalExpr>,
        ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
            Err(DataFusionError::Execution(
                "intentional adapter failure".to_string(),
            ))
        }

        fn with_partition_values(
            &self,
            _partition_values: Vec<(
                Arc<arrow::datatypes::Field>,
                datafusion::scalar::ScalarValue,
            )>,
        ) -> Arc<dyn PhysicalExprAdapter> {
            Arc::new(Self)
        }
    }

    #[tokio::test]
    async fn pruning_returns_none_when_expr_adapter_fails() -> anyhow::Result<()> {
        let partition = build_partition_with_temperature_entries().await?;
        let schema = Arc::new(partition.arrow_schema());
        let projection = vec![schema.index_of("temperature")?];
        let predicate = build_temperature_predicate(schema, 25)?;

        let prune_result = prune_partition(
            &partition,
            &projection,
            predicate,
            Arc::new(FailingExprAdapter),
        )
        .await;

        assert!(prune_result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn pruning_expr_adapter_coerces_logical_f64_to_physical_f32() -> anyhow::Result<()> {
        let partition = build_partition_with_f32_temperature_entries().await?;
        let physical_schema = Arc::new(partition.arrow_schema());

        let logical_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("entry_key", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("temperature", arrow::datatypes::DataType::Float64, true),
        ]));

        let predicate = build_temperature_predicate_f64(logical_schema.clone(), 25.0)?;
        let expr_adapter = DefaultPhysicalExprAdapter::new(logical_schema, physical_schema.clone());
        let rewritten = expr_adapter.rewrite(predicate)?;

        let batch = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(StringArray::from(vec!["dataset-0", "dataset-1"])) as ArrayRef,
                Arc::new(Float32Array::from(vec![20.0_f32, 30.0_f32])) as ArrayRef,
            ],
        )?;

        let evaluated = rewritten.evaluate(&batch)?.into_array(batch.num_rows())?;
        let evaluated = evaluated
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("rewritten predicate should evaluate to boolean array");

        assert!(!evaluated.value(0));
        assert!(evaluated.value(1));

        Ok(())
    }
}
