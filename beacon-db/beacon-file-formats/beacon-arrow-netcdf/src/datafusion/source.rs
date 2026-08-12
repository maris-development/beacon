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
use object_store::ObjectMeta;

use super::reader::{self, FileAccess, NetcdfInput, NetcdfReaderCache};

/// The smallest scan worth splitting across partitions.
///
/// Every share of a file opens that file. The reader cache turns the repeat
/// opens into hits, but each share still derives the schema, resolves the
/// projection and builds the chunk list before it reads a byte, and it holds a
/// partition open for as long as it runs. Below this, that setup costs more than
/// the parallelism returns.
///
/// This replaces DataFusion's `repartition_file_min_size` rather than deferring
/// to it. That default (10 MB) is a parquet number, where a byte range is a run
/// of row groups the reader seeks straight to; a netCDF byte range is a fraction
/// of a chunk list that has to be built first, so the two are not measuring the
/// same cost.
///
/// The test is on one file, not on the scan total. A share pays to open a file,
/// so a file is what has to be large enough to earn it: a collection of small
/// files still scans in parallel, one file per partition, but none of them is
/// cut into shares that would each re-open it for a fraction of its rows.
pub const MIN_SPLIT_SIZE: u64 = 8 * 1024 * 1024;

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// Integrates the `beacon_arrow_netcdf` reader with DataFusion's file scan
/// pipeline via a [`FileOpener`].
#[derive(Debug, Clone)]
pub struct NetCDFSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// How this scan reaches its files, and which reader opens them.
    access: FileAccess,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<NetcdfReaderCache>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl NetCDFSource {
    pub fn new(
        access: FileAccess,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            schema_adapter_factory: None,
            access,
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
    pub fn with_cache(mut self, cache: Option<NetcdfReaderCache>) -> Self {
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

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(NetCDFOpener::new(
            self.access.clone(),
            projected_schema,
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            file_schema,
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

    /// Whether a scan may split one file across partitions.
    ///
    /// `oxcdf` may. It range-reads through the object store and holds no lock,
    /// so partitions of one file run at the same time. DataFusion's byte-range
    /// split is safe here because the opener never reads the range as bytes: it
    /// reads it as a fraction of the chunk list, and the fractions of a file
    /// tile that list. See [`ChunkSplit`] and [`NetCDFOpener::read_task`].
    ///
    /// netcdf-c may not. Every call it makes takes one process-global mutex
    /// (`netcdf_sys::libnetcdf_lock`), which covers the read, the decompression
    /// and the type conversion. Splitting a file would give one partition per
    /// share, each queued on that mutex, plus one open per share. That costs
    /// more than it returns.
    ///
    /// File-level parallelism is separate, and both readers get it: the listing
    /// table spreads files over `target_partitions` before this is consulted.
    /// What splitting adds is parallelism *inside* one file.
    fn supports_repartitioning(&self) -> bool {
        matches!(self.access, FileAccess::Oxcdf)
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
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if !self.supports_repartitioning() {
            return Ok(None);
        }

        Ok(
            beacon_datafusion_ext::file_groups::interleaved_file_groups(
                &config.file_groups,
                target_partitions,
                MIN_SPLIT_SIZE,
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
        "netcdf"
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

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es via [`any_dataset_as_record_batch_stream`].
struct NetCDFOpener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<PruningPredicate>,
    table_schema: SchemaRef,
    cache: Option<NetcdfReaderCache>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// How this opener reaches its files, and which reader opens them.
    access: FileAccess,
    /// The store the scan lists from. The `oxcdf` reader reads through it; the
    /// netcdf-c reader ignores it and opens a resolved native path instead.
    object_store: Arc<dyn object_store::ObjectStore>,
}

impl NetCDFOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        access: FileAccess,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: SchemaRef,
        cache: Option<NetcdfReaderCache>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn object_store::ObjectStore>,
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
            access,
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
        input: NetcdfInput,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<NetcdfReaderCache>,
        metrics: Option<DatasetReadMetrics>,
        split: Option<ChunkSplit>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = reader::open_dataset(cache.as_ref(), input, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open NetCDF dataset {}: {e}",
                    object.location,
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
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project NetCDF dataset with dimensions: {e}"
                ))
            })?
        } else {
            dataset
        };

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from NetCDF dataset: {e}"
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
            // No output columns are needed (e.g. `COUNT(*)`). Reading zero columns
            // would yield an empty stream and an incorrect count of 0. Drive the
            // read with the highest-dimensionality variable so the row count equals
            // the full broadcast row count (a scalar attribute like `.Conventions`
            // would give just 1 row), plus any predicate columns so a pushed-down
            // filter still applies (PushdownFilter matches by name). Emit
            // zero-column batches carrying the correct row counts.
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
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to project NetCDF dataset for count: {e}"
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
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error reading NetCDF as Arrow stream: {e}"
                    ))
                })?;
                RecordBatch::try_new_with_options(
                    count_schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to build count batch: {e}"
                    ))
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
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project NetCDF dataset: {e}"
                ))
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
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to adapt NetCDF batch schema: {e}"
                    ))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for NetCDFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        if let (Some(stats), Some(prune)) = (&file.statistics, &self.pruning_predicate) {
            let result = prune.prune(&PrunableStatistics::new(
                vec![Arc::clone(stats)],
                self.table_schema.clone(),
            ))?[0];
            if !result {
                tracing::debug!(
                    "Pruning NetCDF file {} based on statistics.",
                    file.object_meta.location
                );
                // File is pruned, return empty stream.
                let stream = EmptyRecordBatchStream::new(self.table_schema.clone()).boxed();

                return Ok(futures::future::ready(Ok(stream)).boxed());
            }
        };

        // This partition's share of the file. A byte range of a NetCDF file is
        // not a NetCDF file, so it is never read as bytes. It is read as a
        // fraction of the chunk list the reader builds, the same way a Parquet
        // scan reads its range as a fraction of the row groups. See
        // [`beacon_nd_array::arrow::split`].
        //
        // An unranged file gives `None`, which reads the whole dataset. That is
        // every scan until the source starts to split files.
        let (range_start, range_end) = file.range();
        let split = ChunkSplit::from_byte_range(range_start..range_end, file.object_meta.size);

        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;
        let fut = Self::read_task(
            input,
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

#[cfg(test)]
mod split_tests {
    //! The opener reading one share of a real netCDF file.
    //!
    //! [`NetCDFSource::repartitioned`] will not split a file under
    //! [`MIN_SPLIT_SIZE`], and every bundled fixture is under it, so a scan
    //! cannot reach this path in a test. The opener has no such rule — it reads
    //! whatever range it is handed — so these hand it the ranges directly.
    //!
    //! That is the whole netCDF-specific path: `file.range()` to
    //! [`ChunkSplit::from_byte_range`], through `read_task`'s dimension
    //! resolution and projection, to the encoded stream. Below it,
    //! `beacon-nd-array` checks the chunk-list slicing itself; above it,
    //! `beacon_datafusion_ext::file_groups` checks the deal.

    use std::sync::Arc;

    use arrow::compute::concat_batches;
    use arrow::record_batch::RecordBatch;
    use beacon_datafusion_ext::nd::decode_nd_record_batch;
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use object_store::ObjectStore;

    use super::*;

    /// A data variable on the full grid, so a share covers real chunks. The
    /// whole file would broadcast to gigabytes; one column is 2.3M rows.
    const COLUMN: &str = "analysed_sst";

    /// The bundled gridded fixture, through a local store.
    fn gridded() -> (Arc<dyn ObjectStore>, ObjectMeta) {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join("gridded-example.nc");
        let location =
            object_store::path::Path::from_absolute_path(&path).expect("an absolute object path");
        let file_meta = std::fs::metadata(&path).expect("the bundled file exists");

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let object = ObjectMeta {
            location,
            last_modified: file_meta.modified().map(Into::into).unwrap_or_default(),
            size: file_meta.len(),
            e_tag: None,
            version: None,
        };
        (store, object)
    }

    /// Read `range` of the fixture through a real opener, decoded and broadcast.
    ///
    /// `None` reads the whole file, which is what an unsplit scan hands it.
    ///
    /// The result is empty when the share owns no chunk. That is not a failure:
    /// the fixture stores four chunks, so a scan asking for more partitions than
    /// that leaves the surplus with nothing to read.
    async fn read_share(range: Option<(u64, u64)>) -> Vec<RecordBatch> {
        let (store, object) = gridded();
        let input = FileAccess::Oxcdf
            .input_for(&store, &object)
            .expect("an oxcdf input");

        // One column, in the encoded form the source carries. The opener emits
        // nd-encoded batches, so its table schema is the encoded schema.
        let full = reader::fetch_schema(None, input, object.clone(), None)
            .await
            .expect("the fixture's schema");
        let field = full.field_with_name(COLUMN).expect("the column").clone();
        let logical = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let encoded = Arc::new(beacon_datafusion_ext::nd::encoded_schema(&logical));

        let source = NetCDFSource::new(
            FileAccess::Oxcdf,
            None,
            TableSchema::from_file_schema(encoded),
        );
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();

        let opener = source
            .with_batch_size(8192)
            .create_file_opener(store, &config, 0)
            .expect("an opener");

        let mut file = PartitionedFile::from(object);
        if let Some((start, end)) = range {
            file = file.with_range(start as i64, end as i64);
        }

        let encoded_batches: Vec<RecordBatch> = opener
            .open(file)
            .expect("the open starts")
            .await
            .expect("the open finishes")
            .try_collect()
            .await
            .expect("the stream reads");

        let decoded: Vec<RecordBatch> = encoded_batches
            .iter()
            .map(|batch| {
                decode_nd_record_batch(batch)
                    .expect("decodes")
                    .materialize()
                    .expect("broadcasts")
            })
            .collect();

        decoded
    }

    /// The shares of a file, read in order and concatenated, equal the whole.
    ///
    /// This is the netCDF answer to "does the split return the right rows". A
    /// gap loses rows and an overlap repeats them, and neither raises an error,
    /// so the comparison is the only thing that would notice.
    #[tokio::test]
    async fn the_shares_of_a_file_rebuild_the_whole_read() {
        let (_, object) = gridded();
        let size = object.size;

        let read = read_share(None).await;
        let schema = read.first().expect("the fixture reads").schema();
        let whole = concat_batches(&schema, &read).expect("concatenates");
        assert!(whole.num_rows() > 0, "the fixture must return rows");

        // Five parts over 302 chunks also covers a share that lands on no chunk
        // boundary, and 1 covers the degenerate case.
        for parts in [1_u64, 2, 3, 4, 5, 8] {
            let step = size.div_ceil(parts);
            let mut shares = Vec::new();
            let mut at = 0;
            while at < size {
                let stop = (at + step).min(size);
                shares.extend(read_share(Some((at, stop))).await);
                at = stop;
            }

            let actual = concat_batches(&schema, &shares).expect("concatenates");
            assert_eq!(
                actual.num_rows(),
                whole.num_rows(),
                "parts={parts}: the shares must cover every row once"
            );
            assert_eq!(actual, whole, "parts={parts}: the shares must read the same rows");
        }
    }
}
