//! DataFusion [`FileSource`]/[`FileOpener`] for TIFF/GeoTIFF files.
//!
//! The opener builds an [`AnyDataset`](beacon_nd_array::dataset::AnyDataset)
//! for the (projected) columns and emits `beacon.nd`-encoded batches, which the
//! `NdSourceExec`/`NdBroadcastExec` pair above the scan decodes and broadcasts.
//! This mirrors the netCDF, HDF5 and zarr sources.

use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
        nd_provider::any_dataset_as_encoded_stream, pushdown_filter::PushdownFilter,
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
    error::DataFusionError,
    physical_expr::{PhysicalExpr, conjunction, projection::ProjectionExprs},
    physical_expr_adapter::BatchAdapterFactory,
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::ObjectMeta;

use super::reader;

#[derive(Debug, Clone)]
pub struct TiffSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Explicit dimensions to read, or `None` to auto-select a default.
    read_dimensions: Option<Vec<String>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl TiffSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: 128 * 1024,
            predicate: None,
            read_dimensions: None,
            projection: None,
        }
    }

    /// Returns a copy of this source that reads only the variables belonging to
    /// `read_dimensions` (or auto-selects a default when `None`).
    pub fn with_read_dimensions(mut self, read_dimensions: Option<Vec<String>>) -> Self {
        self.read_dimensions = read_dimensions;
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

impl FileSource for TiffSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(TiffOpener::new(
            object_store,
            projected_schema,
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.execution_plan_metrics.clone(),
            partition,
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

// ─── FileOpener ──────────────────────────────────────────────────────────────

struct TiffOpener {
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TiffOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        Self {
            object_store,
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            partition,
            metrics,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_task(
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<DatasetReadMetrics>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = reader::open_dataset(object_store, object.clone())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })?;

        // Apply the dimension projection before deriving the file schema. With
        // no explicit dimensions, fall back to the dataset's auto-selected
        // default (matching `fetch_schema`). No log label here: this runs per
        // file/partition, so logging would spam.
        let read_dimensions =
            beacon_nd_array::dataset::resolve_read_dimensions(&dataset, read_dimensions, None);
        let dataset = if let Some(dims) = read_dimensions {
            dataset
                .project(&DatasetProjection::new_with_dimension_projection(dims))
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to project TIFF dataset with dimensions: {e}"
                    ))
                })?
        } else {
            dataset
        };

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
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
            // No output columns are needed (e.g. `COUNT(*)`). Reading zero
            // columns would yield an empty stream and an incorrect count of 0.
            // Drive the read with the highest-volume variable so the row count
            // equals the full broadcast row count (a scalar like `image.width`
            // would give just 1 row), plus any predicate columns so a
            // pushed-down filter still applies (PushdownFilter matches by
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
                .project(&DatasetProjection::new_with_index_projection(driver))
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to project TIFF dataset for count: {e}"
                    ))
                })?;

            let pushdown_filter = predicate.map(PushdownFilter::new);
            let count_schema = projected_schema.clone();
            let stream =
                any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter, metrics)
                    .map(move |batch| {
                        let batch = batch.map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Error reading TIFF as Arrow stream: {e}"
                            ))
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
        // encoded (struct) domain: reorder, and null-fill columns this file
        // lacks, onto the projected encoded schema.
        let source_schema: SchemaRef = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            &file_schema.project(&projection)?,
        ));
        let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

        let dataset = if projection.len() < file_schema.fields().len() {
            dataset
                .project(&DatasetProjection::new_with_index_projection(projection))
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to project TIFF dataset: {e}"))
                })?
        } else {
            dataset
        };

        // Emit nd-encoded batches (decoded/broadcast by the NdSourceExec /
        // NdBroadcastExec above the scan), adapted onto the projected encoded
        // schema.
        let _ = metrics;
        let stream = any_dataset_as_encoded_stream(dataset, batch_size)
            .and_then(move |batch| {
                let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to adapt TIFF batch schema: {e}"))
                });
                futures::future::ready(mapped)
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for TiffOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            file.object_meta,
            self.object_store.clone(),
            self.projected_schema.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            self.predicate.clone(),
            metrics,
        )
        .boxed();

        Ok(fut)
    }
}
