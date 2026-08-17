use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        file_read::FileRead,
        metrics::ReadMetrics,
        morsel::{morsel_scan, MorselSource, OpenFile},
    },
    projection::DatasetProjection,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
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

use super::reader::{FileAccess, NetcdfInput};

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
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The scan's file queue, when it is planned morsel-driven.
    ///
    /// `Some` means every partition's group holds one standing entry and the
    /// files are all in here, so the openers read whatever the queue hands them.
    /// `None` means the groups are the file list, as DataFusion planned them.
    morsel: Option<Arc<MorselSource>>,
}

impl NetCDFSource {
    pub fn new(
        access: FileAccess,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            access,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            batch_size: usize::MAX,
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

    /// The files this scan's queue holds, when it is planned morsel-driven.
    #[cfg(test)]
    pub(crate) fn morsel_files(&self) -> Option<usize> {
        self.morsel.as_ref().map(|source| source.files())
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
            self.execution_plan_metrics.clone(),
            partition,
            object_store,
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

    fn supports_repartitioning(&self) -> bool {
        tracing::trace!(
            "NetCDFSource supports_repartitioning: access={:?}",
            self.access
        );
        matches!(self.access, FileAccess::Oxcdf)
    }

    /// Put the scan's files in one queue, and point every partition at it.
    ///
    /// Nothing is assigned here. Each partition's group holds one standing entry
    /// and the files go into a [`MorselSource`]; a partition takes the next file
    /// when it is free, and helps divide an open one when no file is left. So
    /// balance follows completion, and the plan holds one entry per partition
    /// rather than one per file.
    ///
    /// `repartition_file_min_size` is unused. It was the size a file had to
    /// reach before every partition would open it, back when balance was a guess
    /// made from file sizes at plan time. A queue does not guess, so there is no
    /// size at which it is worth declining.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        tracing::trace!(
            "NetCDFSource repartitioned: access={:?}, target_partitions={target_partitions}",
            self.access,
        );
        if !self.supports_repartitioning() || output_ordering.is_some() || target_partitions <= 1 {
            return Ok(None);
        }

        if let Some((morsel, file_groups)) = morsel_scan(&config.file_groups, target_partitions) {
            tracing::debug!(
                "NetCDFSource morsel scan: {} files over {target_partitions} partitions",
                morsel.files()
            );
            let mut config = config.clone();
            config.file_groups = file_groups;
            // The openers are built from the config's source, so the queue has
            // to travel with it.
            config.file_source = Arc::new(Self {
                morsel: Some(morsel),
                ..self.clone()
            });
            return Ok(Some(config));
        }

        // The queue declined. It only does that for a partitioned table, and
        // `NetcdfFormat::create_physical_plan` refuses those before a scan is
        // built, so this is unreachable in practice. Keeping the scan as it was
        // planned is the safe answer either way.
        Ok(None)
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

/// Opens a single NetCDF file and streams its contents as Arrow
/// [`RecordBatch`]es.
struct NetCDFOpener {
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// This partition's counters, registered once. See [`ReadMetrics::new`].
    read_metrics: ReadMetrics,
    partition: usize,
    /// How this opener reaches its files, and which reader opens them.
    access: FileAccess,
    /// The store the scan lists from. The `oxcdf` reader reads through it; the
    /// netcdf-c reader ignores it and opens a resolved native path instead.
    object_store: Arc<dyn object_store::ObjectStore>,
    /// The scan's file queue, when it is planned morsel-driven. `Some` means the
    /// entry `FileStream` hands this opener is the scan, not a file.
    morsel: Option<Arc<MorselSource>>,
    /// How one file is opened, for the queue to call.
    files: Arc<dyn OpenFile>,
}

impl NetCDFOpener {
    #[allow(clippy::too_many_arguments)]
    fn new(
        access: FileAccess,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        object_store: Arc<dyn object_store::ObjectStore>,
        morsel: Option<Arc<MorselSource>>,
    ) -> Self {
        let read_metrics = ReadMetrics::new(&metrics, partition);
        let files = Arc::new(NetCDFFiles {
            access: access.clone(),
            object_store: object_store.clone(),
            projected_schema: projected_schema.clone(),
            read_dimensions: read_dimensions.clone(),
            batch_size,
            predicate: predicate.clone(),
            metrics: read_metrics.clone(),
        });

        Self {
            morsel,
            files,
            projected_schema,
            read_dimensions,
            batch_size,
            predicate,
            read_metrics,
            partition,
            access,
            object_store,
        }
    }

    /// Read one file, through its share when it has one.
    ///
    /// A shared file is opened and planned by whichever partition arrives first;
    /// the rest attach to what it built and pull from the same queue. An
    /// unshared file is planned by the one partition that holds it. Either way
    /// the reading is the same, so there is one path below the plan.
    ///
    /// Every input to the plan has to be identical in every partition of one
    /// file, or the partitions would not be reading the same file the same way.
    /// They are: the dataset, `projected_schema`, `batch_size` and `predicate`.
    /// A scan takes all four from one place, so they are. Keep it that way.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        input: NetcdfInput,
        object: ObjectMeta,
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        batch_size: usize,
        metrics: ReadMetrics,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let planning = metrics.clone();
        let plan = async move || {
            let dataset = Self::open_dataset(input, object, read_dimensions).await?;
            FileRead::plan(
                dataset,
                projected_schema,
                batch_size,
                predicate,
                Some(&planning),
            )
            .await
        };

        // This partition's own file. Nothing is shared here: a scan that can be
        // divided goes through the queue, and one that cannot is refused before
        // it is planned.
        let dataset = plan().await?;

        Ok(dataset.stream(Some(metrics)))
    }

    async fn open_dataset(
        input: NetcdfInput,
        object: ObjectMeta,
        read_dimensions: Option<Vec<String>>,
    ) -> datafusion::error::Result<beacon_nd_array::dataset::AnyDataset> {
        let dataset = input.open().await.map_err(|e| {
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

/// How one netCDF file becomes a planned [`FileRead`].
///
/// This is everything a [`MorselSource`] needs of the format. The queue holds
/// the files; this says what opening one means.
struct NetCDFFiles {
    access: FileAccess,
    object_store: Arc<dyn object_store::ObjectStore>,
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metrics: ReadMetrics,
}

impl std::fmt::Debug for NetCDFFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetCDFFiles")
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl OpenFile for NetCDFFiles {
    async fn open(&self, file: &PartitionedFile) -> datafusion::error::Result<Arc<FileRead>> {
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;
        let dataset = NetCDFOpener::open_dataset(
            input,
            file.object_meta.clone(),
            self.read_dimensions.clone(),
        )
        .await?;

        FileRead::plan(
            dataset,
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            Some(&self.metrics),
        )
        .await
    }
}

impl FileOpener for NetCDFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        // A morsel-driven scan hands every partition the same standing entry.
        // It is not a file: the files are in the queue, and this partition reads
        // whatever it hands out until the scan is done.
        if let Some(morsel) = &self.morsel {
            tracing::trace!(
                "NetCDFOpener morsel scan: {} files, partition={}",
                morsel.files(),
                self.partition
            );
            let stream = morsel.stream(
                self.partition,
                Arc::clone(&self.files),
                Some(self.read_metrics.clone()),
            );
            return Ok(futures::future::ready(Ok(stream)).boxed());
        }

        tracing::trace!(
            "NetCDFOpener open: access={:?}, file={:?}, partition={}",
            self.access,
            file.object_meta.location,
            self.partition
        );
        let metrics = self.read_metrics.clone();
        let input = self
            .access
            .input_for(&self.object_store, &file.object_meta)?;

        Ok(Self::read(
            input,
            file.object_meta,
            self.projected_schema.clone(),
            self.read_dimensions.clone(),
            self.batch_size,
            metrics,
            self.predicate.clone(),
        )
        .boxed())
    }
}
