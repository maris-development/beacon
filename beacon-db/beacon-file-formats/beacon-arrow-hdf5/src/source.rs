//! DataFusion [`FileSource`]/[`FileOpener`] for HDF5 files on the Rust reader.
//!
//! The opener builds an [`AnyDataset`](beacon_nd_array::dataset::AnyDataset)
//! for the (projected) columns and streams it through the shared ND engine,
//! which handles predicate pushdown (chunk pruning + row masking) via
//! [`PushdownFilter`].
//!
//! This mirrors the netCDF source. The difference is where the bytes come from:
//! this one always reads through the scan's own object store, because its
//! reader needs no local file.

use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream_split, metrics::DatasetReadMetrics,
        nd_provider::any_dataset_as_encoded_stream_split, pushdown_filter::PushdownFilter,
        split::ChunkSplit,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::pruning::PrunableStatistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    error::DataFusionError,
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_expr_adapter::BatchAdapterFactory,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
        EmptyRecordBatchStream,
    },
};
use futures::{stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::cache::Hdf5ReaderCache;

/// DataFusion [`FileSource`] for HDF5 (`.h5`/`.hdf5`) files.
#[derive(Debug, Clone)]
pub struct Hdf5Source {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<Hdf5ReaderCache>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl Hdf5Source {
    pub fn new(read_dimensions: Option<Vec<String>>, table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
            predicate: None,
            cache: None,
            projection: None,
        }
    }

    /// Returns a copy of this source that consults `cache` (when `Some`) for
    /// opened datasets. The format wires in the runtime's shared cache here.
    pub fn with_cache(mut self, cache: Option<Hdf5ReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for Hdf5Source {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(Hdf5Opener::new(
            projected_schema,
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.table_schema.file_schema().clone(),
            self.cache.clone(),
            self.execution_plan_metrics.clone(),
            partition,
            object_store,
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    /// Whether a scan may split one file across partitions. It may.
    ///
    /// This source is the `oxcdf` reader's, and only ever that one. A table on
    /// netcdf-c never reaches here: `Hdf5FormatFactory` hands the whole call to
    /// the netCDF factory when `use_rust_reader` is off, so it is served by
    /// `NetCDFSource`, which declines the split because every netcdf-c call
    /// queues on one process-global mutex. `Hdf5Format` has private fields and
    /// one construction site, behind that same check, so the invariant is
    /// structural rather than a convention. `only_the_rust_reader_splits_one_file`
    /// in `tests/backend_parity.rs` holds it to that.
    ///
    /// `oxcdf` range-reads through the object store and holds no lock, so shares
    /// of one file run at the same time. DataFusion's byte-range split is safe
    /// here because the opener never reads the range as bytes: it reads it as a
    /// fraction of the chunk list, and the fractions of a file tile that list.
    /// See [`ChunkSplit`] and [`Hdf5Opener::read_task`].
    fn supports_repartitioning(&self) -> bool {
        true
    }

    /// Deal the file's slices to partitions round-robin rather than in one
    /// contiguous run each.
    ///
    /// An nd chunk list is C-ordered, so a predicate on the outermost dimension
    /// prunes a prefix of it. A contiguous deal would put that whole prefix in
    /// the first partitions, leaving them idle while the last ones do the work.
    /// See [`beacon_datafusion_ext::file_groups`] for the trade this makes.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        Ok(
            beacon_datafusion_ext::file_groups::interleaved_file_groups(
                &config.file_groups,
                target_partitions,
                repartition_file_min_size,
                output_ordering.is_some(),
            )
            .map(|file_groups| {
                let mut config = config.clone();
                config.file_groups = file_groups;
                config
            }),
        )
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "hdf5"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        let source = Self {
            projection: Some(merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
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

// ─── FileOpener ──────────────────────────────────────────────────────────────

/// Opens a single HDF5 file and streams its contents as ND-encoded Arrow
/// [`RecordBatch`]es.
struct Hdf5Opener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<PruningPredicate>,
    table_schema: SchemaRef,
    cache: Option<Hdf5ReaderCache>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// The store the scan lists from. The reader reads its byte ranges through
    /// it, so s3, gs and az work with no local copy.
    object_store: Arc<dyn ObjectStore>,
}

impl Hdf5Opener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: SchemaRef,
        cache: Option<Hdf5ReaderCache>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let pruning_predicate = predicate
            .as_ref()
            .and_then(|pred| PruningPredicate::try_new(pred.clone(), table_schema.clone()).ok());

        Self {
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            pruning_predicate,
            table_schema,
            cache,
            metrics,
            partition,
            object_store,
        }
    }

    /// Read one file, or one [`ChunkSplit`] of it.
    ///
    /// `split` is this partition's share of the file. It is `None` for a whole
    /// file, which is what an unsplit scan hands every opener.
    ///
    /// Every input below the split has to be identical in every partition of one
    /// file, or the shares stop tiling the chunk list and rows go missing or come
    /// back twice. They are: the dataset, `projected_schema`, `read_dimensions`,
    /// `batch_size` and `predicate`. A scan takes all five from one place, so
    /// they are. Keep it that way.
    #[allow(clippy::too_many_arguments)]
    async fn read_task(
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<Hdf5ReaderCache>,
        metrics: Option<DatasetReadMetrics>,
        split: Option<ChunkSplit>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = crate::cache::open_dataset(cache.as_ref(), &store, &object)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open HDF5 dataset {}: {e}",
                    object.location
                ))
            })?;

        // Apply dimension projection before deriving the file schema. When no
        // explicit dimensions were requested, fall back to the dataset's
        // auto-selected default (matching `fetch_schema`). No log label here:
        // this runs per file/partition, so logging would spam.
        let read_dimensions =
            beacon_nd_array::dataset::resolve_read_dimensions(&dataset, read_dimensions, None);
        let dataset = if let Some(dims) = read_dimensions {
            let proj = DatasetProjection {
                dimension_projection: Some(dims),
                index_projection: None,
            };
            dataset.project(&proj).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to project HDF5 dataset with dimensions: {e}"
                ))
            })?
        } else {
            dataset
        };

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from HDF5 dataset: {e}"
                    ))
                })?
                .into();

        // Columns of this file that the query needs, in file order — used both
        // to prune the read and as the source schema for the batch adapter.
        let projection: Vec<usize> = file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
            .map(|(i, _)| i)
            .collect();

        if projection.is_empty() {
            // No output columns are needed (e.g. `COUNT(*)`). Reading zero
            // columns would yield an empty stream and an incorrect count of 0.
            // Drive the read with the highest-dimensionality variable so the
            // row count equals the full broadcast row count (a scalar attribute
            // like `.title` would give just 1 row), plus any predicate columns
            // so a pushed-down filter still applies (PushdownFilter matches by
            // name). Emit zero-column batches carrying the correct row counts.
            let driver_idx = dataset
                .fields()
                .keys()
                .max_by_key(|name| {
                    dataset
                        .get_array(name)
                        .map(|a| a.shape().iter().product::<usize>())
                        .unwrap_or(0)
                })
                .and_then(|name| file_schema.index_of(name).ok())
                .unwrap_or(0);
            let mut driver: Vec<usize> = vec![driver_idx];
            if let Some(pred) = &predicate {
                for col in datafusion::physical_expr::utils::collect_columns(pred) {
                    if let Ok(idx) = file_schema.index_of(col.name()) {
                        driver.push(idx);
                    }
                }
            }
            driver.sort_unstable();
            driver.dedup();

            let dataset = dataset
                .project(&DatasetProjection {
                    dimension_projection: None,
                    index_projection: Some(driver),
                })
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to project HDF5 dataset for count: {e}"
                    ))
                })?;

            let pushdown_filter = predicate.map(PushdownFilter::new);
            let count_schema = projected_schema.clone();
            // The split applies here too. A count path that read the whole file
            // in every partition would return the row count once per partition.
            let stream = any_dataset_as_record_batch_stream_split(
                dataset,
                batch_size,
                pushdown_filter,
                metrics,
                split,
            )
            .map(move |batch| {
                let batch = batch.map_err(|e| {
                    DataFusionError::Execution(format!("Error reading HDF5 as Arrow stream: {e}"))
                })?;
                RecordBatch::try_new_with_options(
                    count_schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to build count batch: {e}"))
                })
            })
            .boxed();
            return Ok(stream);
        }

        // The opener emits nd-encoded batches, so adaptation happens in the
        // encoded (struct) domain: reorder and null-fill missing columns onto
        // the projected encoded schema.
        let source_schema: SchemaRef = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            &file_schema.project(&projection)?,
        ));
        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

        let dataset = if projection.len() < file_schema.fields().len() {
            let proj = DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            };
            dataset.project(&proj).map_err(|e| {
                DataFusionError::Execution(format!("Failed to project HDF5 dataset: {e}"))
            })?
        } else {
            dataset
        };

        // Emit nd-encoded batches (decoded/broadcast by the NdSourceExec /
        // NdBroadcastExec above the scan), adapted onto the projected encoded
        // schema.
        let _ = metrics;
        let stream = any_dataset_as_encoded_stream_split(dataset, batch_size, split)
            .and_then(move |batch| {
                let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to adapt HDF5 batch schema: {e}"))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for Hdf5Opener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        if let (Some(stats), Some(prune)) = (&file.statistics, &self.pruning_predicate) {
            let result = prune.prune(&PrunableStatistics::new(
                vec![Arc::clone(stats)],
                self.table_schema.clone(),
            ))?[0];
            if !result {
                tracing::debug!(
                    "Pruning HDF5 file {} based on statistics.",
                    file.object_meta.location
                );
                // File is pruned, return empty stream.
                let stream = EmptyRecordBatchStream::new(self.table_schema.clone()).boxed();

                return Ok(futures::future::ready(Ok(stream)).boxed());
            }
        };

        // This partition's share of the file. A byte range of an HDF5 file is
        // not an HDF5 file, so it is never read as bytes. It is read as a
        // fraction of the chunk list the reader builds, the same way a Parquet
        // scan reads its range as a fraction of the row groups. See
        // [`beacon_nd_array::arrow::split`].
        //
        // An unranged file gives `None`, which reads the whole dataset.
        let (range_start, range_end) = file.range();
        let split = ChunkSplit::from_byte_range(range_start..range_end, file.object_meta.size);

        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            self.object_store.clone(),
            file.object_meta,
            self.projected_schema.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.cache.clone(),
            metrics,
            split,
        )
        .boxed();
        Ok(fut)
    }
}
