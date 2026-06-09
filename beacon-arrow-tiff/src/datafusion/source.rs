use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::{
    array::RecordBatchOptions,
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_nd_array::{
    arrow::{
        batch::{ParallelDatasetStream, any_dataset_as_row_size},
        metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
    },
    projection::DatasetProjection,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{PhysicalExpr, conjunction, projection::ProjectionExprs},
    physical_expr_adapter::BatchAdapterFactory,
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{ObjectMeta, path::Path};

use super::reader;
use crate::datafusion::stream_share::{SharedTiffStream, TiffStreamShare};

/// Number of chunks read concurrently per file, and the work-sharing channel
/// capacity. Chosen by this (TIFF) consumer; falls back to 4 when the platform
/// parallelism is unavailable.
fn default_read_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Debug, Clone)]
pub struct TiffSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// Number of chunks read concurrently per file (and channel capacity).
    read_parallelism: usize,
    /// Per-file shared parallel read, keyed by object path, so that partitions
    /// opening the same file work-share a single producer.
    stream_partition_shares: Arc<Mutex<HashMap<Path, Arc<TiffStreamShare>>>>,
}

impl TiffSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: 128 * 1024,
            predicate: None,
            projection: None,
            read_parallelism: default_read_parallelism(),
            stream_partition_shares: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for TiffSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(TiffOpener::new(
            file_schema,
            object_store,
            projected_schema,
            self.batch_size,
            self.predicate.clone(),
            self.execution_plan_metrics.clone(),
            partition,
            self.read_parallelism,
            self.stream_partition_shares.clone(),
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

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        _config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        Ok(None)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "tiff"
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

struct TiffOpener {
    table_schema: SchemaRef,
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition: usize,
    metrics: ExecutionPlanMetricsSet,
    read_parallelism: usize,
    stream_partition_shares: Arc<Mutex<HashMap<Path, Arc<TiffStreamShare>>>>,
}

impl TiffOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        table_schema: SchemaRef,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        read_parallelism: usize,
        stream_partition_shares: Arc<Mutex<HashMap<Path, Arc<TiffStreamShare>>>>,
    ) -> Self {
        Self {
            table_schema,
            object_store,
            projected_schema,
            batch_size,
            predicate,
            partition,
            metrics,
            read_parallelism,
            stream_partition_shares,
        }
    }

    /// Build (once per file) the shared read for `object`: open the dataset,
    /// resolve the projection, and either spawn a [`ParallelDatasetStream`] or,
    /// for an empty projection, capture the dataset row count.
    async fn build_shared_stream(
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        read_parallelism: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<DatasetReadMetrics>,
    ) -> datafusion::error::Result<SharedTiffStream> {
        let dataset = reader::open_dataset(object_store, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })?;

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from TIFF dataset: {e}"
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
            // Empty projection (e.g. count(*)): derive the row count once. Read
            // a single batch from the (cheap) row-size stream and keep its
            // length; each opener rebuilds the row-count batch from it.
            let mut row_size_stream = any_dataset_as_row_size(dataset).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to compute row size for empty projection on TIFF dataset: {e}"
                ))
            })?;
            let rows = match row_size_stream.next().await {
                Some(Ok(batch)) => batch.num_rows(),
                Some(Err(e)) => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read TIFF dataset with empty projection: {e}"
                    )));
                }
                None => 0,
            };
            return Ok(SharedTiffStream::RowSize { rows });
        }

        // Adapt batches (read with `projection`) onto the projected output
        // schema: reorder, cast, and null-fill columns this file lacks.
        let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
        let adapter = Arc::new(BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?);

        let dataset = if projection.len() < file_schema.fields().len() {
            let proj = DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project TIFF dataset: {e}"
                ))
            })?
        } else {
            dataset
        };

        let pushdown_filter = predicate.map(PushdownFilter::new);
        let stream =
            ParallelDatasetStream::spawn(dataset, batch_size, pushdown_filter, metrics, read_parallelism);

        Ok(SharedTiffStream::Data { stream, adapter })
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_task(
        share: Arc<TiffStreamShare>,
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        read_parallelism: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<DatasetReadMetrics>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        // First partition to reach this file opens it and spawns the parallel
        // producer; later partitions clone the same handle and work-share its
        // batches. (The producer is spawned with the initializing partition's
        // metrics.)
        let shared = share
            .get_or_try_init(|| {
                Self::build_shared_stream(
                    object,
                    object_store,
                    projected_schema,
                    batch_size,
                    read_parallelism,
                    predicate,
                    metrics,
                )
            })
            .await?
            .clone();

        match shared {
            SharedTiffStream::Data { stream, adapter } => Ok(stream
                .into_stream()
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error reading TIFF as Arrow stream: {e}"
                    ))
                })
                .and_then(move |batch| {
                    let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to adapt TIFF batch schema: {e}"
                        ))
                    });
                    futures::future::ready(mapped)
                })
                .boxed()),
            SharedTiffStream::RowSize { rows } => {
                let schema = Arc::new(Schema::empty());
                let options = RecordBatchOptions::new().with_row_count(Some(rows));
                let batch = RecordBatch::try_new_with_options(schema, vec![], &options)?;
                Ok(futures::stream::once(async move { Ok(batch) }).boxed())
            }
        }
    }
}

impl FileOpener for TiffOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));

        // Get (or create) the shared handle for this file path so that all
        // partitions opening the same file share one parallel producer.
        let share = {
            let mut shares = self
                .stream_partition_shares
                .lock()
                .expect("stream_partition_shares mutex poisoned");
            shares
                .entry(file.object_meta.location.clone())
                .or_insert_with(|| Arc::new(TiffStreamShare::new()))
                .clone()
        };

        let fut = Self::read_task(
            share,
            file.object_meta,
            self.object_store.clone(),
            self.projected_schema.clone(),
            self.batch_size,
            self.read_parallelism,
            self.predicate.clone(),
            metrics,
        )
        .boxed();

        Ok(fut)
    }
}
