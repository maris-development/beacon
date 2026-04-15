use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
        schema_adapter::{SchemaAdapter, SchemaMapper},
    },
    physical_expr_adapter::PhysicalExprAdapter,
};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use object_store::ObjectStore;

use crate::{
    collection::AtlasCollection,
    consts::{DATASET_PREFETCH_CONCURRENCY, PARTITION_METADATA_FILE, STREAM_CHUNK_SIZE},
    datafusion::pruning,
    partition::ops::read::{Dataset, read_dataset_columns_by_index},
};

/// File opener for Atlas partition metadata entries.
///
/// Each `open` call resolves the collection and partition, obtains projected
/// column readers, and emits a stream of Arrow `RecordBatch` values by
/// processing one dataset at a time.
pub struct AtlasOpener {
    /// Object store used by collection and partition readers.
    pub object_store: Arc<dyn ObjectStore>,
    /// DataFusion schema adapter for projected output mapping.
    pub schema_adapter: Arc<dyn SchemaAdapter>,
    /// DataFusion physical expression adapter for pruning.
    pub expr_adapter: Arc<dyn PhysicalExprAdapter>,
    /// Shared dataset work queues keyed by partition path.
    pub shared_dataset_queues: SharedDatasetQueueRegistry,
    /// Shared opened collections keyed by collection path.
    pub shared_collections: SharedCollectionRegistry,
    /// Filter predicates to apply during pruning.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Aggregated execution counters for Atlas file reads.
    pub metrics: AtlasGlobalMetrics,
}

/// Shared dataset work queues keyed by partition path.
pub(crate) type SharedDatasetQueueRegistry =
    Arc<tokio::sync::Mutex<HashMap<String, Arc<crossbeam::queue::SegQueue<u32>>>>>;

/// Shared opened Atlas collections keyed by collection path.
pub(crate) type SharedCollectionRegistry = Arc<
    tokio::sync::Mutex<
        HashMap<String, Arc<tokio::sync::Mutex<AtlasCollection<Arc<dyn ObjectStore>>>>>,
    >,
>;

type DatasetQueue = Arc<crossbeam::queue::SegQueue<u32>>;
type AtlasColumnReader = crate::column::ColumnReader<Arc<dyn ObjectStore>>;
type SharedCollectionHandle = Arc<tokio::sync::Mutex<AtlasCollection<Arc<dyn ObjectStore>>>>;

/// Global execution counters published through DataFusion metrics.
#[derive(Debug, Clone)]
pub struct AtlasGlobalMetrics {
    row_count: Count,
    batch_count: Count,
    dataset_count: Count,
    error_count: Count,
    pruning_total_container_count: Count,
    pruning_pruned_container_count: Count,
    time_spent_mapping_batches_ms: Count,
    time_spent_io_ms: Count,
}

impl AtlasGlobalMetrics {
    pub fn new(metrics_set: ExecutionPlanMetricsSet) -> Self {
        Self {
            row_count: MetricBuilder::new(&metrics_set).global_counter("atlas_total_row_count"),
            batch_count: MetricBuilder::new(&metrics_set).global_counter("atlas_total_batch_count"),
            dataset_count: MetricBuilder::new(&metrics_set)
                .global_counter("atlas_total_dataset_count"),
            error_count: MetricBuilder::new(&metrics_set).global_counter("atlas_total_error_count"),
            pruning_total_container_count: MetricBuilder::new(&metrics_set)
                .global_counter("atlas_pruning_total_container_count"),
            pruning_pruned_container_count: MetricBuilder::new(&metrics_set)
                .global_counter("atlas_pruning_pruned_container_count"),
            time_spent_mapping_batches_ms: MetricBuilder::new(&metrics_set)
                .global_counter("atlas_time_spent_mapping_batches_ms"),
            time_spent_io_ms: MetricBuilder::new(&metrics_set)
                .global_counter("atlas_time_spent_io_ms"),
        }
    }

    fn add_rows(&self, count: usize) {
        self.row_count.add(count);
    }

    fn add_batch(&self) {
        self.batch_count.add(1);
    }

    fn add_dataset(&self) {
        self.dataset_count.add(1);
    }

    fn add_error(&self) {
        self.error_count.add(1);
    }

    pub(crate) fn add_pruning_total_containers(&self, count: usize) {
        self.pruning_total_container_count.add(count);
    }

    pub(crate) fn add_pruning_pruned_containers(&self, count: usize) {
        self.pruning_pruned_container_count.add(count);
    }

    fn add_time_spent_mapping_batches_ms(&self, ms: usize) {
        self.time_spent_mapping_batches_ms.add(ms);
    }

    fn add_time_spent_io_ms(&self, ms: usize) {
        self.time_spent_io_ms.add(ms);
    }
}

#[derive(Clone)]
struct PartitionReadContext {
    /// Native partition schema before DataFusion projection/mapping.
    partition_schema: SchemaRef,
    /// Projected schema after applying projection.
    projected_schema: SchemaRef,
    /// Maps partition batches into the table schema expected by DataFusion.
    schema_mapper: Arc<dyn SchemaMapper>,
    /// Projected column readers resolved from the partition.
    column_readers: Vec<Arc<AtlasColumnReader>>,
    /// Shared dataset queue for this partition.
    dataset_queue: DatasetQueue,
}

/// Parse `<collection>/partitions/<partition>/atlas_partition.json`.
fn collection_partition_paths_from_file(
    file: &PartitionedFile,
) -> datafusion::error::Result<(object_store::path::Path, String, object_store::path::Path)> {
    let file_path = file.object_meta.location.as_ref();
    let partition_dir = file_path
        .strip_suffix(PARTITION_METADATA_FILE)
        .unwrap_or(file_path)
        .trim_end_matches('/');

    let (collection_dir, partition_name) =
        partition_dir
            .rsplit_once("/partitions/")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Invalid Atlas partition metadata path '{}': expected '<collection>/partitions/<name>/{}'",
                    file_path, PARTITION_METADATA_FILE
                ))
            })?;

    if collection_dir.is_empty() || partition_name.is_empty() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Invalid Atlas partition metadata path '{}': collection path or partition name is empty",
            file_path
        )));
    }

    Ok((
        object_store::path::Path::from(collection_dir),
        partition_name.to_string(),
        object_store::path::Path::from(partition_dir),
    ))
}

async fn get_or_init_dataset_queue(
    shared_dataset_queues: &SharedDatasetQueueRegistry,
    partition_key: &str,
    dataset_indexes: Vec<u32>,
    metrics: &AtlasGlobalMetrics,
) -> DatasetQueue {
    let mut queue_registry = shared_dataset_queues.lock().await;
    if let Some(queue) = queue_registry.get(partition_key) {
        return queue.clone();
    }

    let queue = Arc::new(crossbeam::queue::SegQueue::new());
    for dataset_index in dataset_indexes {
        queue.push(dataset_index);
    }
    queue_registry.insert(partition_key.to_string(), queue.clone());
    queue
}

async fn get_or_init_collection(
    shared_collections: &SharedCollectionRegistry,
    object_store: Arc<dyn ObjectStore>,
    collection_path: object_store::path::Path,
    file_path: &str,
    metrics: &AtlasGlobalMetrics,
) -> datafusion::error::Result<SharedCollectionHandle> {
    let key = collection_path.to_string();

    // Fast path: collection is already opened and shared.
    let mut shared_collections_guard = shared_collections.lock().await;
    if let Some(collection) = shared_collections_guard.get(&key).cloned() {
        return Ok(collection);
    }

    let collection = AtlasCollection::open(object_store, collection_path)
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open Atlas collection for '{}': {}",
                file_path, e
            ))
        })?;
    let shared = Arc::new(tokio::sync::Mutex::new(collection));

    // Insert with double-check so concurrent tasks reuse the same handle.
    shared_collections_guard.insert(key, shared.clone());
    Ok(shared)
}

async fn build_partition_read_context(
    object_store: Arc<dyn ObjectStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    expr_adapter: Arc<dyn PhysicalExprAdapter>,
    shared_dataset_queues: SharedDatasetQueueRegistry,
    shared_collections: SharedCollectionRegistry,
    metrics: AtlasGlobalMetrics,
    file: PartitionedFile,
    predicate: Option<Arc<dyn PhysicalExpr>>,
) -> datafusion::error::Result<PartitionReadContext> {
    let file_path = file.object_meta.location.as_ref().to_string();
    let (collection_path, partition_name, partition_path) =
        collection_partition_paths_from_file(&file)?;

    let collection = get_or_init_collection(
        &shared_collections,
        object_store,
        collection_path,
        &file_path,
        &metrics,
    )
    .await?;
    // Lock is held only while resolving partition metadata/reader handles.
    let mut collection = collection.lock().await;
    let time = std::time::Instant::now();
    let partition = collection
        .get_partition(&partition_name)
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open Atlas partition '{}' at '{}': {}",
                partition_name, file_path, e
            ))
        })?;
    println!(
        "Opened partition '{}' for '{}': {}ms",
        partition_name,
        file_path,
        time.elapsed().as_millis()
    );
    drop(collection);

    let partition_schema = Arc::new(partition.arrow_schema());
    let (schema_mapper, projection) = schema_adapter.map_schema(&partition_schema)?;
    let projected_schema = partition_schema.project(&projection).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to project partition schema for '{}': {}",
            file_path, e
        ))
    })?;

    let schema = partition.schema();
    let projected_columns: Vec<String> = projection
        .iter()
        .filter_map(|&idx| schema.columns.get(idx).map(|c| c.name.clone()))
        .collect();

    let mut column_readers = Vec::with_capacity(projected_columns.len());
    for col_name in &projected_columns {
        let reader = partition.column_reader(col_name).await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open Atlas column reader for '{}': {}",
                col_name, e
            ))
        })?;
        column_readers.push(reader);
    }

    let undeleted_dataset_indexes = partition.undeleted_dataset_indexes();
    let pruned = if let Some(predicate) = predicate {
        pruning::prune_partition_with_metrics(
            &partition,
            &projection,
            predicate,
            expr_adapter,
            Some(&metrics),
        )
        .await
    } else {
        None
    };

    let mut dataset_indexes_mask = partition.deletion_flags().to_vec();
    // if let Some(pruned_mask) = pruned {
    //     for (i, keep) in pruned_mask.iter().enumerate() {
    //         if !keep {
    //             dataset_indexes_mask[i] = true;
    //         }
    //     }
    // }

    // Mask the undeleted dataset indexes with the pruning result so that pruned and deleted datasets are not included in the queue.
    let dataset_indexes: Vec<u32> = undeleted_dataset_indexes
        .into_iter()
        .filter(|&idx| !dataset_indexes_mask[idx as usize])
        .collect();

    let dataset_queue = get_or_init_dataset_queue(
        &shared_dataset_queues,
        partition_path.as_ref(),
        dataset_indexes,
        &metrics,
    )
    .await;

    Ok(PartitionReadContext {
        partition_schema,
        projected_schema: Arc::new(projected_schema),
        schema_mapper,
        column_readers,
        dataset_queue,
    })
}

fn dataset_indexes_stream(dataset_queue: DatasetQueue) -> futures::stream::BoxStream<'static, u32> {
    // Pop until queue is empty; each dataset index is processed at most once.
    futures::stream::unfold(dataset_queue, |queue| async move {
        queue.pop().map(|dataset_index| (dataset_index, queue))
    })
    .boxed()
}

fn read_dataset_columns(
    column_readers: &[Arc<AtlasColumnReader>],
    dataset_index: u32,
) -> datafusion::error::Result<Vec<Option<Arc<dyn NdArrowArray>>>> {
    read_dataset_columns_by_index(column_readers, dataset_index).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to read dataset {}: {}",
            dataset_index, e
        ))
    })
}

fn build_dataset(
    partition_schema: SchemaRef,
    columns: Vec<Option<Arc<dyn NdArrowArray>>>,
    dataset_index: u32,
) -> datafusion::error::Result<Dataset> {
    Dataset::new_unaligned("dataset", partition_schema, columns).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to create dataset {}: {}",
            dataset_index, e
        ))
    })
}

fn dataset_to_record_batch_stream(
    context: PartitionReadContext,
    metrics: AtlasGlobalMetrics,
) -> BoxStream<'static, datafusion::error::Result<arrow::array::RecordBatch>> {
    // Empty projection: no columns requested, so there is nothing to emit.
    if context.column_readers.is_empty() {
        return futures::stream::empty().boxed();
    }

    let dataset_indexes = dataset_indexes_stream(context.dataset_queue);
    let metrics_for_prepare = metrics.clone();
    let metrics_for_flat_map = metrics.clone();

    dataset_indexes
        .map(move |dataset_index| {
            let column_readers = context.column_readers.clone();
            let projected_schema = context.projected_schema.clone();
            let schema_mapper = context.schema_mapper.clone();
            let metrics = metrics_for_prepare.clone();

            async move {
                let columns = read_dataset_columns(&column_readers, dataset_index)?;
                let dataset = build_dataset(projected_schema, columns, dataset_index)?;
                metrics.add_dataset();

                // Stream chunks per dataset to keep memory bounded.
                let arrow_stream = dataset
                    .0
                    .try_as_arrow_stream(STREAM_CHUNK_SIZE)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to convert dataset {} to Arrow stream: {}",
                            dataset_index, e
                        ))
                    })?;

                Ok::<_, datafusion::error::DataFusionError>((arrow_stream, schema_mapper))
            }
        })
        // Keep a bounded number of datasets in-flight so downstream always has work.
        .buffer_unordered(DATASET_PREFETCH_CONCURRENCY.max(1))
        .flat_map_unordered(
            DATASET_PREFETCH_CONCURRENCY.max(1),
            move |result| match result {
                Ok((arrow_stream, schema_mapper)) => {
                    let metrics = metrics_for_flat_map.clone();
                    arrow_stream
                        .map(move |batch_result| {
                            let batch = batch_result.map_err(|e| {
                                metrics.add_error();
                                datafusion::error::DataFusionError::Execution(format!(
                                    "Error reading Arrow batch from Atlas: {}",
                                    e
                                ))
                            })?;
                            let mapped = schema_mapper.map_batch(batch).inspect_err(|_e| {
                                metrics.add_error();
                            })?;

                            metrics.add_batch();
                            metrics.add_rows(mapped.num_rows());
                            Ok(mapped)
                        })
                        .boxed()
                }
                Err(e) => {
                    metrics.add_error();
                    futures::stream::once(async move { Err(e) }).boxed()
                }
            },
        )
        .boxed()
}

impl FileOpener for AtlasOpener {
    fn open(
        &self,
        _file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let object_store = self.object_store.clone();
        let schema_adapter = self.schema_adapter.clone();
        let expr_adapter = self.expr_adapter.clone();
        let shared_dataset_queues = self.shared_dataset_queues.clone();
        let shared_collections = self.shared_collections.clone();
        let metrics = self.metrics.clone();
        let predicate = self.predicate.clone();

        let fut = async move {
            let context = build_partition_read_context(
                object_store,
                schema_adapter,
                expr_adapter,
                shared_dataset_queues,
                shared_collections,
                metrics.clone(),
                file,
                predicate,
            )
            .await?;

            Ok(dataset_to_record_batch_stream(context, metrics))
        };

        Ok(fut.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AtlasGlobalMetrics, AtlasOpener, SharedCollectionRegistry, SharedDatasetQueueRegistry,
        collection_partition_paths_from_file, dataset_indexes_stream, get_or_init_collection,
        get_or_init_dataset_queue,
    };
    use crate::{collection::AtlasCollection, column::Column, schema::AtlasSuperTypingMode};
    use arrow::array::{Array, Int32Array, StringArray};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::{
        physical_plan::{FileMeta, FileOpener},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    };
    use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapter;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::{StreamExt, stream};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn parses_collection_partition_paths_from_partition_metadata_file() {
        let file = PartitionedFile::new(
            "collections/example/partitions/p0/atlas_partition.json".to_string(),
            0,
        );

        let (collection_path, partition_name, partition_path) =
            collection_partition_paths_from_file(&file).expect("parse path");
        assert_eq!(collection_path.to_string(), "collections/example");
        assert_eq!(partition_name, "p0");
        assert_eq!(
            partition_path.to_string(),
            "collections/example/partitions/p0"
        );
    }

    #[tokio::test]
    async fn reuses_existing_dataset_queue_for_same_partition() {
        let registry: SharedDatasetQueueRegistry =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        let metrics = AtlasGlobalMetrics::new(ExecutionPlanMetricsSet::new());
        let first =
            get_or_init_dataset_queue(&registry, "partition-a", vec![10, 11], &metrics).await;
        let second = get_or_init_dataset_queue(&registry, "partition-a", vec![99], &metrics).await;

        assert!(Arc::ptr_eq(&first, &second));

        let mut values = vec![first.pop(), first.pop(), first.pop()];
        values.sort_unstable();
        assert_eq!(values, vec![None, Some(10), Some(11)]);
    }

    #[tokio::test]
    async fn dataset_indexes_stream_drains_queue_once() {
        let queue = Arc::new(crossbeam::queue::SegQueue::new());
        queue.push(1);
        queue.push(2);

        let indexes = dataset_indexes_stream(queue.clone())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(indexes, vec![1, 2]);
        assert!(queue.pop().is_none());
    }

    #[tokio::test]
    async fn reuses_existing_collection_for_same_collection_path() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/reuse-collection");
        AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "reuse-collection",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let registry: SharedCollectionRegistry = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let first = get_or_init_collection(
            &registry,
            store.clone(),
            collection_path.clone(),
            "collections/reuse-collection/partitions/p0/atlas_partition.json",
            &AtlasGlobalMetrics::new(ExecutionPlanMetricsSet::new()),
        )
        .await?;
        let second = get_or_init_collection(
            &registry,
            store,
            collection_path,
            "collections/reuse-collection/partitions/p0/atlas_partition.json",
            &AtlasGlobalMetrics::new(ExecutionPlanMetricsSet::new()),
        )
        .await?;

        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[tokio::test]
    async fn opener_reads_file_open_future_stream_from_collection_partition() -> anyhow::Result<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/opener-example");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "opener-example",
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
        writer.finish().await?;

        let partition = collection.get_partition("part-00000").await?;
        let table_schema = Arc::new(partition.arrow_schema());
        let schema_adapter =
            DefaultSchemaAdapterFactory.create(table_schema.clone(), table_schema.clone());
        let expr_adapter =
            DefaultPhysicalExprAdapter::new(table_schema.clone(), table_schema.clone());

        let opener = AtlasOpener {
            object_store: store,
            schema_adapter: Arc::from(schema_adapter),
            expr_adapter: Arc::from(expr_adapter),
            shared_dataset_queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            shared_collections: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            metrics: AtlasGlobalMetrics::new(ExecutionPlanMetricsSet::new()),
            predicate: None,
        };

        let partition_meta_path = collection_path
            .child("partitions")
            .child("part-00000")
            .child(crate::consts::PARTITION_METADATA_FILE)
            .to_string();
        let file = PartitionedFile::new(partition_meta_path, 0);
        let file_meta = FileMeta {
            object_meta: file.object_meta.clone(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = opener.open(file_meta, file)?.await?;
        let batches = stream.collect::<Vec<_>>().await;
        let batches = batches
            .into_iter()
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        assert!(!batches.is_empty());
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );

        Ok(())
    }

    #[tokio::test]
    async fn opener_stream_covers_all_datasets_in_partition() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/opener-two-datasets");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "opener-two-datasets",
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

        let partition = collection.get_partition("part-00000").await?;
        let table_schema = Arc::new(partition.arrow_schema());
        let schema_adapter =
            DefaultSchemaAdapterFactory.create(table_schema.clone(), table_schema.clone());
        let expr_adapter =
            DefaultPhysicalExprAdapter::new(table_schema.clone(), table_schema.clone());

        let opener = AtlasOpener {
            object_store: store,
            schema_adapter: Arc::from(schema_adapter),
            shared_dataset_queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            shared_collections: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            metrics: AtlasGlobalMetrics::new(ExecutionPlanMetricsSet::new()),
            expr_adapter: Arc::from(expr_adapter),
            predicate: None,
        };

        let partition_meta_path = collection_path
            .child("partitions")
            .child("part-00000")
            .child(crate::consts::PARTITION_METADATA_FILE)
            .to_string();
        let file = PartitionedFile::new(partition_meta_path, 0);
        let file_meta = FileMeta {
            object_meta: file.object_meta.clone(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = opener.open(file_meta, file)?.await?;
        let batches = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(total_rows, 4);

        let mut entry_keys = Vec::new();
        let mut temperatures = Vec::new();
        for batch in &batches {
            let entry = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("entry key should be Utf8");
            for i in 0..entry.len() {
                entry_keys.push(entry.value(i).to_string());
            }

            let temp = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("temperature should be Int32");
            for i in 0..temp.len() {
                temperatures.push(temp.value(i));
            }
        }

        entry_keys.sort();
        temperatures.sort();
        assert_eq!(
            entry_keys,
            vec!["dataset-0", "dataset-0", "dataset-1", "dataset-1"]
        );
        assert_eq!(temperatures, vec![10, 20, 30, 40]);

        Ok(())
    }
}
