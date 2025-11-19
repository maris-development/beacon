use std::{sync::Arc, vec};

use arrow::datatypes::SchemaRef;
use beacon_arrow_zarr::reader::AsyncArrowZarrGroupReader;
use datafusion::{
    common::{exec_datafusion_err, pruning::PrunableStatistics},
    datasource::schema_adapter::SchemaAdapter,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::PhysicalExpr,
};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{
    expr_util::extract_range_from_physical_filters,
    opener::stream_share::PartitionedZarrStreamShare,
    statistics::{ZarrStatistics, ZarrStatisticsSelection, pushdown::ArraySlicePushDownResult},
};

pub async fn fetch_partitioned_stream(
    zarr_group_reader: Arc<Group<dyn AsyncReadableListableStorageTraits>>,
    table_schema: SchemaRef,
    table_schema_adapter: Arc<dyn SchemaAdapter>,
    pruning_predicate_expr: Option<Arc<dyn PhysicalExpr>>,
    statistics_selection: Option<Arc<ZarrStatisticsSelection>>,
) -> datafusion::error::Result<Option<PartitionedZarrStreamShare>> {
    let pushdowns = if let Some(pruning_predicate_expr) = &pruning_predicate_expr
        && let Some(statistics_selection) = &statistics_selection
    {
        let pruning_predicate =
            PruningPredicate::try_new(pruning_predicate_expr.clone(), table_schema.clone())?;
        // Create statistics for pruning
        let statistics = ZarrStatistics::create(
            &table_schema,
            statistics_selection,
            zarr_group_reader.clone(),
        )
        .await?;

        let prunable_stats = PrunableStatistics::new(
            vec![Arc::new(statistics.get_statistics())],
            table_schema.clone(),
        );

        let pruning_result = pruning_predicate.prune(&prunable_stats)?;
        // If the file can be pruned, return an empty stream
        if pruning_result.iter().all(|&v| !v) {
            return Ok(None); // early return with empty streams
        }

        // else continue to generate slice pushdowns based on the predicate
        let statistics_columns = statistics.statistics_columns();

        let mut array_slice_pushdowns = vec![];
        for column in statistics_columns {
            // tracing::debug!("Predicate: {:?}", pruning_predicate_expr);
            let range = extract_range_from_physical_filters(
                std::slice::from_ref(pruning_predicate_expr),
                column.as_str(),
            );

            if let Some(zarr_range) = range
                && let Some(pushdown) = statistics.get_slice_pushdown(&column, zarr_range)
            {
                array_slice_pushdowns.push(pushdown);
            }
        }

        let optimized_pushdowns = ArraySlicePushDownResult::optimize(array_slice_pushdowns);
        if ArraySlicePushDownResult::should_prune(&optimized_pushdowns) {
            return Ok(None); // early return with empty streams
        }

        // Generate stream with pushdowns
        let pushdowns = ArraySlicePushDownResult::get_slice_pushdowns(&optimized_pushdowns);

        // tracing::debug!("Zarr slice pushdowns: {:?}", pushdowns);

        Some(pushdowns)
    } else {
        None
    };

    let reader = AsyncArrowZarrGroupReader::new(zarr_group_reader.clone())
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let partition_file_schema = reader.arrow_schema();
    let (table_mapper, projection) = table_schema_adapter.map_schema(&partition_file_schema)?;

    let stream = reader
        .into_parallel_stream_composer(Some(projection), pushdowns)
        .map_err(|e| exec_datafusion_err!("Failed to create zarr stream: {}", e))?;

    let partition_stream =
        PartitionedZarrStreamShare::new(stream, table_mapper, partition_file_schema.clone());
    Ok(Some(partition_stream))
}
