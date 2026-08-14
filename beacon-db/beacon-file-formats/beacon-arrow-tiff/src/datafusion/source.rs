use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::arrow::{
    metrics::SharedReadMetrics,
    morsel::{morsel_scan, MorselSource, OpenFile},
    share::SharedDataset,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs, PhysicalExpr},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{stream::BoxStream, FutureExt};
use object_store::ObjectMeta;

use super::reader;

#[derive(Debug, Clone)]
pub struct TiffSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The scan's file queue, when it is planned morsel-driven. See
    /// [`morsel_scan`].
    morsel: Option<Arc<MorselSource>>,
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
            morsel: None,
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
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(TiffOpener::new(
            object_store,
            projected_schema,
            self.batch_size,
            self.predicate.clone(),
            self.execution_plan_metrics.clone(),
            partition,
            self.morsel.clone(),
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

    /// Put the scan's rasters in one queue, and point every partition at it.
    ///
    /// A GeoTIFF is one raster per object, so nothing here divides a file: the
    /// partitions take whole rasters, whoever is free taking the next. That is
    /// worth doing even though the listing already spread them, because the
    /// listing spread them by a guess at plan time and this does not guess.
    ///
    /// An ordered scan keeps its grouping: a partition taking rasters as it
    /// finishes cannot emit them in listing order.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            return Ok(None);
        }

        let Some((morsel, file_groups)) = morsel_scan(&config.file_groups, target_partitions)
        else {
            return Ok(None);
        };

        tracing::debug!(
            "TiffSource morsel scan: {} rasters over {target_partitions} partitions",
            morsel.files()
        );
        let mut config = config.clone();
        config.file_groups = file_groups;
        // The openers are built from the config's source, so the queue has to
        // travel with it.
        config.file_source = Arc::new(Self {
            morsel: Some(morsel),
            ..self.clone()
        });
        Ok(Some(config))
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
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition: usize,
    /// This partition's counters, registered once. See [`SharedReadMetrics::new`].
    read_metrics: SharedReadMetrics,
    /// The scan's raster queue, when it is planned morsel-driven.
    morsel: Option<Arc<MorselSource>>,
    /// How one raster is opened, for the queue to call.
    rasters: Arc<dyn OpenFile>,
}

/// How one GeoTIFF becomes a planned [`SharedDataset`].
///
/// This is everything a [`MorselSource`] needs of the format.
struct TiffRasters {
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metrics: SharedReadMetrics,
}

impl std::fmt::Debug for TiffRasters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiffRasters").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl OpenFile for TiffRasters {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<Arc<SharedDataset>> {
        let object = file.object_meta.clone();
        let dataset = reader::open_dataset(self.object_store.clone(), object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })?;

        SharedDataset::plan(
            dataset,
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            Some(&self.metrics),
        )
        .await
    }
}

impl TiffOpener {
    fn new(
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        morsel: Option<Arc<MorselSource>>,
    ) -> Self {
        // Once per partition, not once per file: every call registers four
        // counters into the scan's one metrics set, behind a mutex.
        let read_metrics = SharedReadMetrics::new(&metrics, partition);
        let rasters = Arc::new(TiffRasters {
            object_store: object_store.clone(),
            projected_schema: projected_schema.clone(),
            batch_size,
            predicate: predicate.clone(),
            metrics: read_metrics.clone(),
        });

        Self {
            object_store,
            projected_schema,
            batch_size,
            predicate,
            partition,
            read_metrics,
            morsel,
            rasters,
        }
    }

    /// Read one file.
    ///
    /// TIFF has no share map: a GeoTIFF is one raster per object and the listing
    /// spreads objects across the partitions, so each file is one partition's
    /// alone. The planning below is the same one a shared file gets — see
    /// [`SharedDataset::plan`].
    async fn read(
        object: ObjectMeta,
        object_store: Arc<dyn object_store::ObjectStore>,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: SharedReadMetrics,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let dataset = reader::open_dataset(object_store, object.clone())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open TIFF dataset {}: {e}",
                    object.location,
                ))
            })?;

        let dataset = SharedDataset::plan(
            dataset,
            projected_schema,
            batch_size,
            predicate,
            Some(&metrics),
        )
        .await?;

        Ok(dataset.stream(Some(metrics)))
    }
}

impl FileOpener for TiffOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        // A morsel-driven scan hands every partition the same standing entry.
        // It is not a raster: the rasters are in the queue, and this partition
        // reads whatever it hands out until the scan is done.
        if let Some(morsel) = &self.morsel {
            let stream = morsel.stream(
                self.partition,
                Arc::clone(&self.rasters),
                Some(self.read_metrics.clone()),
            );
            return Ok(futures::future::ready(Ok(stream)).boxed());
        }

        Ok(Self::read(
            file.object_meta,
            self.object_store.clone(),
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            self.read_metrics.clone(),
        )
        .boxed())
    }
}
