use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use beacon_arrow_zarr::{
    array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader,
    stream::ArrowZarrStream,
};
use datafusion::{
    common::pruning::PrunableStatistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, ParquetSource},
        schema_adapter::SchemaAdapter,
    },
    physical_optimizer::pruning::{FilePruner, PruningPredicate},
    physical_plan::PhysicalExpr,
};
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use parking_lot::Mutex;
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{
    array_step_span::NumericArrayStepSpan,
    expr_util::extract_range_from_physical_filters,
    pushdown_statistics::{
        ZarrPushDownStatistics, ZarrStatistics, generate_zarr_statistics_from_zarr_group,
    },
    source::recursive_groups,
    stream_share::{PartitionedZarrStreamShare, ZarrStreamShare},
    util::ZarrPath,
};

/// Open a Zarr object and produce a future that yields a stream proxy for its partitions.
///
/// This implements FileOpener for ZarrFileOpener and performs the following steps:
/// - Converts the provided FileMeta's ObjectMeta into a ZarrPath, returning a DataFusion
///   Execution error if parsing fails.
/// - Uses an internal, shared map of stream shares keyed by object path to obtain an
///   Arc<ZarrStreamShare>. The map access is protected by a lock; if a share for the
///   path does not exist it is created lazily. This ensures concurrent open requests
///   for the same path share initialization work and the resulting streams.
/// - Builds and returns a boxed asynchronous future. When polled, that future:
///     - Calls get_or_try_init on the ZarrStreamShare to initialize or reuse the
///       partitioned streams for the object. Initialization is performed by
///       fetch_partitioned_streams which may access the object store, apply pruning
///       using pushdown statistics/predicates, and consider array step configuration.
///     - Flattens the resulting partitioned streams into a single stream proxy suitable
///       for DataFusion consumption and returns it.
///
/// Parameters:
/// - file_meta: Metadata for the file to open; its ObjectMeta is used to construct
///   the ZarrPath and to key the stream-share cache.
/// - _file: A PartitionedFile value that is unused by this opener (present to match
///   the trait signature).
///
/// Return:
/// - On success: Ok(BoxFuture) that resolves to a stream proxy of the opened file's
///   partitions.
/// - On error: DataFusionError::Execution with a descriptive message (e.g., failed
///   ZarrPath creation or stream initialization).
///
/// Additional notes:
/// - The method clones necessary handles (object store, schema adapter, predicate,
///   array steps) so the returned future is self-contained and safe to run later.
/// - Debug tracing is emitted for path creation and file open operations to aid
///   diagnosis.
/// - IO and heavy work are deferred until the returned future is awaited/polled.
pub struct ZarrFileOpener {
    pub table_schema: SchemaRef,
    /// Underlying object store for accessing Zarr data.
    pub zarr_object_store: Arc<AsyncObjectStore<Arc<dyn ObjectStore>>>,
    /// Schema adapter for mapping Zarr schema to Arrow schema.
    pub schema_adapter: Arc<dyn SchemaAdapter>,
    /// Stream partition shares for the Zarr file.
    pub stream_partition_shares:
        Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
    /// Array Steps for slicing arrays based on step spans. This is utilized by the pruning predicate pushdown.
    pub array_steps: Arc<HashMap<String, NumericArrayStepSpan>>,
    /// Pushdown Zarr Statistics
    pub pushdown_zarr_statistics: ZarrPushDownStatistics,
    /// Pruning Predicate
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
}

async fn fetch_partitioned_streams(
    zarr_path: ZarrPath,
    zarr_object_store: Arc<AsyncObjectStore<Arc<dyn ObjectStore>>>,
    array_steps: Arc<HashMap<String, NumericArrayStepSpan>>,
    pruning_predicate: Option<Arc<dyn PhysicalExpr>>,
    table_schema: SchemaRef,
    table_schema_adapter: Arc<dyn SchemaAdapter>,
    zarr_pushdown_statistics: ZarrPushDownStatistics,
) -> datafusion::error::Result<Arc<[PartitionedZarrStreamShare]>> {
    let base_group = Group::async_open(
        zarr_object_store.clone() as Arc<dyn AsyncReadableListableStorageTraits>,
        &zarr_path.as_zarr_path(),
    )
    .await
    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let mut partitioned_groups = Vec::new();
    recursive_groups(Arc::new(base_group), &mut partitioned_groups).await?;
    // Iterate over arrays in the group and open them as partitioned groups

    let mut partitioned_streams = Vec::new();
    for group_partition in partitioned_groups {
        let reader = AsyncArrowZarrGroupReader::new(group_partition.clone())
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let file_schema = reader.arrow_schema();

        let (table_mapper, projection) = table_schema_adapter.map_schema(&file_schema)?;
        let partition_file_schema = file_schema
            .project(&projection)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        let arc_partition_file_schema = Arc::new(partition_file_schema);

        // Generate statistics for the partitioned file
        let statistics = generate_zarr_statistics_from_zarr_group(
            &table_schema,
            &zarr_pushdown_statistics,
            group_partition.clone(),
        )
        .await;

        if let Some(pruning_predicate_expr) = pruning_predicate.clone()
            && let Some(statistics) = &statistics
        {
            let prunable_stats = PrunableStatistics::new(
                vec![Arc::new(statistics.statistics())],
                table_schema.clone(),
            );
            let pruning_predicate =
                PruningPredicate::try_new(pruning_predicate_expr.clone(), table_schema.clone())?;

            let pruning_result = pruning_predicate.prune(&prunable_stats)?;

            // If the file can be pruned, return an empty stream
            if pruning_result.iter().all(|&v| !v) {
                return Ok(Arc::from(vec![])); // early return with empty streams
            }

            // else continue to generate slice pushdowns based on the predicate
        }

        // Generate ArraySlicePushDown based on the predicate and array steps
        let array_slice_pushdowns = if let Some(pruning_predicate) = &pruning_predicate {
            todo!()
        } else {
            HashMap::new()
        };

        let stream = reader
            .into_parallel_stream_composer(Some(projection), Some(array_slice_pushdowns))
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let partition_stream = PartitionedZarrStreamShare::new(
            stream,
            table_mapper,
            arc_partition_file_schema.clone(),
        );
        partitioned_streams.push(partition_stream);
    }

    Ok(Arc::from(partitioned_streams))
}

async fn flatten_partitioned_streams(
    partitioned_streams: Arc<[PartitionedZarrStreamShare]>,
) -> Pin<
    Box<
        dyn futures::Stream<Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>>
            + Send,
    >,
> {
    let mut all_streams = Vec::new();
    for partition in partitioned_streams.iter() {
        let stream = flatten_batch_stream(partition.stream_composer.pollable_shared_stream());
        all_streams.push(stream);
    }
    let combined_stream = futures::stream::iter(all_streams).flatten();
    combined_stream.boxed()
}

fn flatten_batch_stream(
    stream: ArrowZarrStream,
) -> Pin<
    Box<
        dyn futures::Stream<Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>>
            + Send,
    >,
> {
    (stream
        .map(|res_nd_batch| {
            let nd_batch = res_nd_batch.map_err(|e| {
                ArrowError::IoError(
                    "Error reading NdRecordBatch from Zarr stream".to_string(),
                    std::io::Error::other(e),
                )
            })?;

            let arrow_batch = nd_batch
                .to_arrow_record_batch()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            Ok(arrow_batch)
        })
        .boxed()) as _
}

impl FileOpener for ZarrFileOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        if let (Some(statistics), Some(predicate)) = (file.statistics, self.predicate.as_ref()) {
            let prunable_stats =
                PrunableStatistics::new(vec![statistics.clone()], self.table_schema.clone());
            let pruning_predicate =
                PruningPredicate::try_new(predicate.clone(), self.table_schema.clone())?;

            let pruning_result = pruning_predicate.prune(&prunable_stats)?;

            // If the file can be pruned, return an empty stream
            if pruning_result.iter().all(|&v| !v) {
                let empty_stream = futures::stream::empty().boxed();
                return Ok(Box::pin(async move { Ok(empty_stream) }));
            }
        }

        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from ObjectMeta: {}",
                e
            ))
        })?;

        let zarr_object_store = self.zarr_object_store.clone();
        let adapter = self.schema_adapter.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file_meta.object_meta.location.clone();
            tracing::debug!(
                "Getting or creating ZarrStreamShare for path: {}",
                object_path
            );
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(ZarrStreamShare::new()))
                .clone()
        };
        let pruning_predicate = self.predicate.clone();
        let array_steps = self.array_steps.clone();
        let pushdown_statistics = self.pushdown_zarr_statistics.clone();

        // Check if the pruning predicate references any arrays in the pushdown statistics
        let fut = async move {
            // Determine based on statistics and predicate which arrays to pushdown
            let partitioned_streams = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file_meta.object_meta.location);

                    fetch_partitioned_streams(
                        zarr_path,
                        zarr_object_store,
                        array_steps,
                        pruning_predicate,
                        adapter,
                        pushdown_statistics,
                    )
                    .await
                })
                .await?
                .clone();

            let stream_proxy = flatten_partitioned_streams(partitioned_streams).await;
            Ok(stream_proxy)
        };

        Ok(fut.boxed())
    }
}

pub enum PushDownResult {
    Prune,
    PushDown(Vec<ArraySlicePushDown>),
    Retain,
}

/// Result of translating the current physical pruning predicate(s) into a
/// concrete range for the provided column span.
///
/// This value is produced by `extract_range_from_physical_filters` and
/// captures the contiguous interval of physical values (or row positions)
/// that are guaranteed to satisfy the pruning predicate applied to
/// `span.column`. The computed range is intended for chunk/block-level
/// pruning so that downstream I/O or scanning can skip data that falls
/// outside the returned interval.
///
/// Behavior notes:
/// - When the predicate can be mapped to a simple interval, `r` holds that
///   interval (e.g., an inclusive/exclusive min..max). Consumers can use it
///   to restrict reads to the overlap with a chunk's bounds.
/// - If the predicate cannot be expressed as a single contiguous range, or
///   if it is contradictory, the function will indicate that no safe range
///   can be derived (typically via `None` or an empty/invalid range), and
///   no pruning should be applied.
/// - The pruning predicate is provided as a slice here to allow the same
///   translation logic to accept zero, one, or multiple physical filters;
///   callers intentionally pass a one-element slice when converting a single
///   predicate.
///
/// Callers should always check the returned value before using it to avoid
/// incorrect data exclusion when translation fails or is inexact.
fn generate_array_slice_pushdowns(
    pruning_predicate: &Arc<dyn PhysicalExpr>,
    statistics: &ZarrStatistics,
) -> PushDownResult {
    let mut pushdown_filters = HashMap::new();
    for column in statistics.statistics_columns() {
        let zarr_range_filter =
            extract_range_from_physical_filters(std::slice::from_ref(pruning_predicate), &column);
        if let Some(zarr_range_filter) = zarr_range_filter {
            pushdown_filters.insert(column, zarr_range_filter);
        }
    }

    let array_slice_pushdowns = statistics.dimension_slice_pushdown(pushdown_filters);

    // Some(array_slice_pushdowns.values().cloned().collect::<Vec<_>>())

    if !array_slice_pushdowns.is_empty() {}

    PushDownResult::Retain
}
