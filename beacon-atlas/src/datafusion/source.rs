use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use crossbeam::queue::SegQueue;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaMapper,
    },
    physical_expr::conjunction,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{ObjectStore, path::Path};

use beacon_nd_array::arrow::batch::dataset_as_record_batch_stream;

use super::pruning::{AtlasPruningStatistics, PackedColumnStatistics};
use super::schema_mapper::{AtlasSchemaMapper, required_columns};
use crate::reader::AtlasReader;

/// DataFusion [`FileSource`] for atlas collections.
///
/// Integrates the atlas reader with DataFusion's file scan pipeline.
/// Each partition's file opener shares a `SegQueue` of dataset names so that
/// multiple partitions can process datasets concurrently in a work-stealing fashion.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    object_store: Arc<dyn ObjectStore>,
    cache_capacity_bytes: u64,
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl AtlasSource {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_capacity_bytes: u64) -> Self {
        Self {
            object_store,
            cache_capacity_bytes,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            predicate: None,
        }
    }
}

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        let projected_schema = base_config.projected_schema();
        let schema_mapper = Arc::new(AtlasSchemaMapper::new(projected_schema));

        Arc::new(AtlasOpener {
            object_store: self.object_store.clone(),
            schema_mapper,
            cache_capacity_bytes: self.cache_capacity_bytes,
            predicate: self.predicate.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        if let Some(statistics) = &self.projected_statistics {
            Ok(statistics.clone())
        } else if let Some(schema) = self.override_schema.as_ref() {
            Ok(Statistics::new_unknown(schema))
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "atlas"
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };

        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}

// ─── FileOpener ────────────────────────────────────────────────────────────────

/// Opens an atlas collection and streams datasets as Arrow RecordBatches.
///
/// When `open` is called, the opener:
/// 1. Derives the base path from the footer file location
/// 2. Opens an `AtlasReader`
/// 3. Populates a `SegQueue` with all non-deleted dataset names
/// 4. Returns a stream that pulls datasets from the queue one at a time,
///    reads them, converts to RecordBatches, and applies schema adaptation
struct AtlasOpener {
    object_store: Arc<dyn ObjectStore>,
    schema_mapper: Arc<AtlasSchemaMapper>,
    cache_capacity_bytes: u64,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl AtlasOpener {
    /// Derive the base path of the collection from the footer file path.
    fn base_path_from_footer(location: &Path) -> Path {
        let s = location.to_string();
        let base = s
            .strip_suffix("/collection.atlas")
            .or_else(|| s.strip_suffix("collection.atlas"))
            .unwrap_or(&s);
        Path::from(base)
    }
}

impl FileOpener for AtlasOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: datafusion::datasource::listing::PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let object_store = self.object_store.clone();
        let schema_mapper = self.schema_mapper.clone();
        let cache_capacity_bytes = self.cache_capacity_bytes;
        let location = file_meta.object_meta.location.clone();
        let predicate = self.predicate.clone();

        let fut = async move {
            let base_path = Self::base_path_from_footer(&location);

            let reader = Arc::new(
                AtlasReader::open(object_store, base_path, cache_capacity_bytes)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to open atlas collection: {e}"
                        ))
                    })?,
            );

            // Build the pruning mask if a predicate was pushed down
            let pruning_mask = if let Some(ref pred) = predicate {
                let global_arrow_schema =
                    super::schema::global_schema_to_arrow(reader.global_schema());
                match PruningPredicate::try_new(pred.clone(), global_arrow_schema) {
                    Ok(pruning_predicate) => prune_datasets(&reader, &pruning_predicate).await.ok(),
                    Err(_) => None,
                }
            } else {
                None
            };

            // Populate the work queue with non-deleted, non-pruned dataset names
            let queue: Arc<SegQueue<String>> = Arc::new(SegQueue::new());
            for (name, entry) in reader.schema_store().datasets() {
                if reader.deletion_mask().is_deleted(entry.dataset_id) {
                    continue;
                }
                if let Some(ref mask) = pruning_mask {
                    let idx = entry.dataset_id.as_u32() as usize;
                    if idx < mask.len() && !mask[idx] {
                        continue; // pruned out
                    }
                }
                queue.push(name.to_string());
            }

            // Return a stream that pulls datasets from the queue.
            // A single shared schema mapper handles all datasets.
            let stream = futures::stream::unfold(
                (queue, reader, schema_mapper),
                |(queue, reader, schema_mapper)| async move {
                    let dataset_name = queue.pop()?;

                    let result =
                        read_dataset_as_batches(&reader, &dataset_name, &schema_mapper).await;

                    match result {
                        Ok(batch_stream) => Some((batch_stream, (queue, reader, schema_mapper))),
                        Err(e) => Some((
                            futures::stream::once(async move { Err(e) }).boxed(),
                            (queue, reader, schema_mapper),
                        )),
                    }
                },
            )
            .flatten()
            .boxed();

            Ok(stream)
        };

        Ok(Box::pin(fut))
    }
}

/// Read a single dataset and return a stream of RecordBatches with schema mapping applied.
///
/// For each dataset, we:
/// 1. Get the dataset's own schema and convert it to Arrow
/// 2. Use `required_columns()` to determine which columns to read
/// 3. Read only the required columns from the dataset
/// 4. Map each batch through the shared schema mapper (handles reorder, cast, null-fill)
async fn read_dataset_as_batches(
    reader: &AtlasReader,
    dataset_name: &str,
    schema_mapper: &Arc<AtlasSchemaMapper>,
) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
    // Get the dataset's own schema and convert to Arrow
    let dataset_schema = reader.read_dataset_schema(dataset_name).await;
    let dataset_schema = match dataset_schema {
        Some(s) => s,
        None => {
            return Ok(futures::stream::empty().boxed());
        }
    };

    let dataset_arrow_schema = super::schema::global_schema_to_arrow(&dataset_schema);

    // Determine which columns from this dataset are needed by the output schema
    let projected_columns = required_columns(schema_mapper.output_schema(), &dataset_arrow_schema);

    let projection = if projected_columns.is_empty() {
        None
    } else {
        Some(&projected_columns)
    };

    let dataset = reader
        .read_dataset(dataset_name, projection)
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to read atlas dataset '{}': {e}",
                dataset_name,
            ))
        })?;

    let mapper = schema_mapper.clone();
    let batch_stream = dataset_as_record_batch_stream(dataset, usize::MAX, None)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Error converting atlas dataset to RecordBatch: {e}"
            ))
        })
        .and_then(move |batch| {
            let mapper = mapper.clone();
            async move {
                mapper.map_batch(batch).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to map atlas batch schema: {e}"
                    ))
                })
            }
        })
        .boxed();

    Ok(batch_stream)
}

/// Build a pruning mask for all datasets using the given predicate.
///
/// Returns a `Vec<bool>` indexed by `DatasetId::as_u32()`. Entries that are
/// `true` may contain matching rows; `false` entries can be skipped entirely.
async fn prune_datasets(
    reader: &AtlasReader,
    pruning_predicate: &PruningPredicate,
) -> datafusion::error::Result<Vec<bool>> {
    let num_containers = reader.schema_store().next_dataset_id() as usize;
    let global_schema = reader.global_schema();

    // Determine which columns the predicate references
    let referenced_columns =
        datafusion::physical_expr::utils::collect_columns(pruning_predicate.orig_expr());

    // Load statistics for each referenced column
    let mut columns: HashMap<String, PackedColumnStatistics> = HashMap::new();
    for col in &referenced_columns {
        let col_name = col.name();
        // Find the column's NdArrayDataType from the global schema
        let dtype = global_schema
            .columns
            .iter()
            .find(|c| c.name == col_name)
            .map(|c| c.data_type.clone());

        let dtype = match dtype {
            Some(dt) => dt,
            None => continue, // column not in schema, skip
        };

        // Load statistics (best-effort: skip on error)
        let stats = match reader.load_column_statistics(col_name).await {
            Ok(s) => s,
            Err(_) => continue,
        };

        columns.insert(
            col_name.to_string(),
            PackedColumnStatistics::new(&stats, dtype, num_containers),
        );
    }

    let pruning_stats = AtlasPruningStatistics::new(num_containers, columns);
    let mask = pruning_predicate.prune(&pruning_stats).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to prune atlas datasets: {e}"
        ))
    })?;

    Ok(mask)
}
