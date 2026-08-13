use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use beacon_nd_array::{
    arrow::{
        batch::{any_dataset_as_record_batch_stream, any_dataset_as_record_batch_stream_split},
        metrics::DatasetReadMetrics,
        nd_provider::{any_dataset_as_encoded_stream, any_dataset_as_encoded_stream_split},
        pushdown_filter::PushdownFilter,
        share::{ReadMode, SharedRead},
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
    physical_expr_adapter::{BatchAdapter, BatchAdapterFactory},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
        EmptyRecordBatchStream,
    },
};
use futures::{stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectMeta;

use std::collections::HashMap;

use super::reader::{self, FileAccess, NetcdfInput, NetcdfReaderCache};

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// Integrates the `beacon_arrow_netcdf` reader with DataFusion's file scan
/// pipeline via a [`FileOpener`].
#[derive(Debug, Clone)]
pub struct NetCDFSource {
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
    /// The shares of this scan. Cloned, not copied, so every partition of a file
    /// reaches the same one.
    partitions_shared_map:
        Arc<HashMap<object_store::path::Path, Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>>,
    nd_encoded: bool,
}

impl NetCDFSource {
    pub fn new(
        access: FileAccess,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
        nd_encoded: bool,
    ) -> Self {
        Self {
            access,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
            predicate: None,
            cache: None,
            projection: None,
            partitions_shared_map: Arc::new(HashMap::new()),
            nd_encoded,
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

    /// Whether this scan reads `path` through a share, rather than as one
    /// partition's own whole file.
    pub(crate) fn shares_file(&self, path: &object_store::path::Path) -> bool {
        self.partitions_shared_map.contains_key(path)
    }
}

impl FileSource for NetCDFSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(NetCDFOpener::new(
            self.access.clone(),
            projected_schema,
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.cache.clone(),
            self.execution_plan_metrics.clone(),
            partition,
            object_store,
            self.partitions_shared_map.clone(),
            self.nd_encoded,
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

    fn supports_repartitioning(&self) -> bool {
        matches!(self.access, FileAccess::Oxcdf)
    }

    /// Give every partition the files that are worth dividing, and one each of
    /// the files that are not.
    ///
    /// A file worth dividing goes into every partition's group and gets a cell
    /// in `partitions_shared_map`. Nothing about it is divided here: the
    /// partitions divide it as they read it, by taking subsets from the one
    /// queue behind that cell. Balance then follows completion rather than a
    /// guess made at plan time.
    ///
    /// A smaller file is left whole and dealt to one partition. Every partition
    /// opening it to take a subset or two would cost more than it returns, and
    /// the listing has already spread these across the scan.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if !self.supports_repartitioning() || output_ordering.is_some() || target_partitions <= 1 {
            // An ordered scan cannot share: a partition holding an arbitrary
            // subset of a file cannot emit its rows in file order.
            return Ok(None);
        }

        Ok(beacon_datafusion_ext::file_groups::shared_file_groups(
            &config.file_groups,
            target_partitions,
            repartition_file_min_size as u64,
        )
        .map(|deal| {
            // One cell per shared file. Every partition that holds the file
            // reaches the same cell, so the first to arrive builds the read and
            // the rest attach to what it built. A file with no cell here is one
            // partition's alone and is read whole.
            let shares = deal
                .shared
                .into_iter()
                .map(|path| (path, Arc::new(tokio::sync::OnceCell::new())))
                .collect();

            let mut config = config.clone();
            config.file_groups = deal.file_groups;
            // The openers are built from the config's source, so the map has to
            // travel with it.
            config.file_source = Arc::new(Self {
                partitions_shared_map: Arc::new(shares),
                ..self.clone()
            });
            config
        }))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "netcdf"
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

#[derive(Debug)]
struct SharedDataset {
    read: Arc<SharedRead>,
    /// `None` on the `COUNT(*)` path: there is no column to adapt, only a row
    /// count to carry. See [`NetCDFOpener::count_projection`].
    table_adapter: Option<Arc<BatchAdapter>>,
}

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es via [`any_dataset_as_record_batch_stream`].
struct NetCDFOpener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Option<NetcdfReaderCache>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    /// How this opener reaches its files, and which reader opens them.
    access: FileAccess,
    /// The store the scan lists from. The `oxcdf` reader reads through it; the
    /// netcdf-c reader ignores it and opens a resolved native path instead.
    object_store: Arc<dyn object_store::ObjectStore>,
    /// The shares of this scan, so the partitions of a file find each other.
    partition_shares:
        Arc<HashMap<object_store::path::Path, Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>>,
    nd_encoded: bool,
}

impl NetCDFOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        access: FileAccess,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<NetcdfReaderCache>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn object_store::ObjectStore>,
        partition_shares: Arc<
            HashMap<object_store::path::Path, Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>>,
        >,
        nd_encoded: bool,
    ) -> Self {
        Self {
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            cache,
            metrics,
            partition,
            access,
            object_store,
            partition_shares,
            nd_encoded,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_shared(
        shared_ref: Arc<tokio::sync::OnceCell<Arc<SharedDataset>>>,
        input: NetcdfInput,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        cache: Option<NetcdfReaderCache>,
        _metrics: Option<DatasetReadMetrics>, // TODO: use this to record the read metrics for the shared dataset, not just the partition that built it.
        _predicate: Option<Arc<dyn PhysicalExpr>>, // TODO: use this to filter the stream when creating the dataset up front on coordinate arrays.
        nd_encoded: bool,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        // The output schema is needed again after the build, which consumes the
        // copy it derives the projection from.
        let output_schema = projected_schema.clone();

        // The first partition to arrive builds the shared read and fills its queue. The rest wait for it to finish.
        let share : Arc<SharedDataset> = shared_ref
            .get_or_try_init::<datafusion::error::DataFusionError, _, _>(async move || {
                        let dataset = Self::open_dataset(input, object, cache, read_dimensions.clone()).await?;
                        let dataset_arrow_schema = Self::arrow_schema(&dataset)?;

                        let projection = Self::find_projection(&dataset_arrow_schema, &projected_schema)?;

                        if projection.is_empty() {
                            // `COUNT(*)`: no column is wanted, so the read is
                            // driven by columns of its own and only the row
                            // counts leave. It shares the queue like any other
                            // read — a file every partition holds would
                            // otherwise be counted once per partition.
                            let projection = Self::count_projection(
                                &dataset,
                                &dataset_arrow_schema,
                                &_predicate,
                            );
                            let counted =
                                Self::project_any_dataset(dataset, &dataset_arrow_schema, projection)?;
                            let read = SharedRead::build(
                                counted,
                                batch_size,
                                ReadMode::Flat(_predicate.map(PushdownFilter::new)),
                            )
                            .await?;
                            return Ok(Arc::new(SharedDataset {
                                read,
                                table_adapter: None,
                            }));
                        }

                        let source_schema = if nd_encoded {
                            Arc::new(beacon_datafusion_ext::nd::encoded_schema(
                                &dataset_arrow_schema.project(&projection)?,
                            ))
                        } else {
                            Arc::new(dataset_arrow_schema.project(&projection)?)
                        };

                        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;
                        let projected_dataset =
                            Self::project_any_dataset(dataset, &dataset_arrow_schema, projection)?;

                        let shared_read = if nd_encoded {
                            SharedRead::build(projected_dataset, batch_size, ReadMode::Encoded).await?
                        } else {
                            SharedRead::build(projected_dataset, batch_size, ReadMode::Flat(None)).await? // TODO: support a flat read with a predicate, so the shared read can prune chunks that hold nothing the query wants.
                        };

                        let shared_dataset = SharedDataset {
                            read: shared_read,
                            table_adapter: Some(Arc::new(adapter)),
                        };
                        Ok(Arc::new(shared_dataset))
            })
            .await?
            .clone();

        let adapter = share.table_adapter.clone();
        let shared_read = share.read.clone();

        let stream = shared_read
            .stream()
            .and_then(move |batch| {
                let mapped = match &adapter {
                    Some(adapter) => adapter.adapt_batch(&batch).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to adapt NetCDF batch schema: {e}"
                        ))
                    }),
                    None => Self::count_batch(&output_schema, batch.num_rows()),
                };
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }

    #[allow(clippy::too_many_arguments)]
    async fn read(
        input: NetcdfInput,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        cache: Option<NetcdfReaderCache>,
        metrics: Option<DatasetReadMetrics>, // TODO: use this to record the read metrics for the dataset on the column path too; the count path already does.
        predicate: Option<Arc<dyn PhysicalExpr>>, // TODO: use this to filter the stream when creating the dataset up front on coordinate arrays.
        nd_encoded: bool,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = Self::open_dataset(input, object, cache, read_dimensions.clone()).await?;
        let dataset_arrow_schema = Self::arrow_schema(&dataset)?;

        let projection = Self::find_projection(&dataset_arrow_schema, &projected_schema)?;

        if projection.is_empty() {
            // `COUNT(*)`: no column is wanted, so the read is driven by columns
            // of its own and only the row counts leave.
            let count_projection =
                Self::count_projection(&dataset, &dataset_arrow_schema, &predicate);
            let counted =
                Self::project_any_dataset(dataset, &dataset_arrow_schema, count_projection)?;
            let stream = any_dataset_as_record_batch_stream(
                counted,
                batch_size,
                predicate.map(PushdownFilter::new),
                metrics,
            )
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create NetCDF record batch stream: {e}"
                ))
            })
            .and_then(move |batch| {
                futures::future::ready(Self::count_batch(&projected_schema, batch.num_rows()))
            })
            .boxed();
            return Ok(stream);
        }

        let source_schema: SchemaRef = if nd_encoded {
            Arc::new(beacon_datafusion_ext::nd::encoded_schema(
                &dataset_arrow_schema.project(&projection)?,
            ))
        } else {
            Arc::new(dataset_arrow_schema.project(&projection)?)
        };

        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;
        let projected_dataset =
            Self::project_any_dataset(dataset, &dataset_arrow_schema, projection)?;

        let raw_stream = if nd_encoded {
            any_dataset_as_encoded_stream(projected_dataset, batch_size)
        } else {
            Box::pin(
                any_dataset_as_record_batch_stream(projected_dataset, batch_size, None, None)
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to create NetCDF record batch stream: {e}"
                        ))
                    }),
            )
        };

        let stream = raw_stream
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

    /// The columns a `COUNT(*)` reads, out of a file the query wants no column
    /// of.
    ///
    /// Reading no column at all would give an empty stream and a count of zero.
    /// The read is driven by the highest-dimensionality variable instead, so the
    /// row count is the full broadcast row count — a scalar attribute like
    /// `.Conventions` would give one row — plus any column the predicate names,
    /// so a pushed-down filter still applies ([`PushdownFilter`] matches by
    /// name).
    fn count_projection(
        dataset: &beacon_nd_array::dataset::AnyDataset,
        dataset_schema: &SchemaRef,
        predicate: &Option<Arc<dyn PhysicalExpr>>,
    ) -> Vec<usize> {
        let driver = dataset
            .fields()
            .keys()
            .max_by_key(|name| {
                dataset
                    .get_array(name)
                    .map(|array| array.shape().iter().product::<usize>())
                    .unwrap_or(0)
            })
            .and_then(|name| dataset_schema.index_of(name).ok())
            .unwrap_or(0);

        let mut projection = vec![driver];
        if let Some(predicate) = predicate {
            for column in datafusion::physical_expr::utils::collect_columns(predicate) {
                if let Ok(index) = dataset_schema.index_of(column.name()) {
                    projection.push(index);
                }
            }
        }
        projection.sort_unstable();
        projection.dedup();
        projection
    }

    /// One `COUNT(*)` batch: no columns, and the row count of the batch it was
    /// counted from.
    fn count_batch(schema: &SchemaRef, rows: usize) -> datafusion::error::Result<RecordBatch> {
        RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to build count batch: {e}"
            ))
        })
    }

    fn arrow_schema(
        dataset: &beacon_nd_array::dataset::AnyDataset,
    ) -> datafusion::error::Result<SchemaRef> {
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(dataset)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to derive Arrow schema from NetCDF dataset: {e}"
                ))
            })
            .map(Arc::new)
    }

    fn find_projection(
        dataset_schema: &SchemaRef,
        projected_schema: &SchemaRef,
    ) -> datafusion::error::Result<Vec<usize>> {
        let projection: Vec<usize> = dataset_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
            .map(|(i, _)| i)
            .collect();
        Ok(projection)
    }

    fn project_any_dataset(
        dataset: beacon_nd_array::dataset::AnyDataset,
        dataset_schema: &SchemaRef,
        projection: Vec<usize>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
        let dataset = if projection.len() < dataset_schema.fields().len() {
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

        Ok(dataset)
    }

    async fn open_dataset(
        input: NetcdfInput,
        object: ObjectMeta,
        cache: Option<NetcdfReaderCache>,
        read_dimensions: Option<Vec<String>>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
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
        Ok(dataset)
    }
}

impl FileOpener for NetCDFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;

        match self.partition_shares.get(&file.object_meta.location) {
            Some(shared_ref) => {
                let fut = Self::read_shared(
                    shared_ref.clone(),
                    input,
                    file.object_meta,
                    self.projected_schema.clone(),
                    self.read_dimensions.clone(),
                    self.batch_size,
                    self.cache.clone(),
                    metrics,
                    self.predicate.clone(),
                    self.nd_encoded,
                )
                .boxed();

                Ok(fut)
            }
            None => {
                let fut = Self::read(
                    input,
                    file.object_meta,
                    self.projected_schema.clone(),
                    self.read_dimensions.clone(),
                    self.batch_size,
                    self.cache.clone(),
                    metrics,
                    self.predicate.clone(),
                    self.nd_encoded,
                )
                .boxed();

                Ok(fut)
            }
        }
    }
}
