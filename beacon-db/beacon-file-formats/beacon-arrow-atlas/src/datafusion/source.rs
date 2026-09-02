//! The DataFusion [`FileSource`] and [`FileOpener`] for Atlas collections.
//!
//! # One dataset is one unit of work
//!
//! A plan entry is a *dataset*, not a collection: [`AtlasFormat`] lists each
//! collection at plan time and emits one [`PartitionedFile`] per dataset, with
//! the dataset's name in [`PartitionedFile::extensions`]. Every entry carries
//! the collection's own marker, so the opener knows which container to open and
//! the reader cache keys on the same object the plan did.
//!
//! Those entries go into one [`MorselSource`], and each partition holds a
//! standing entry pointing at it. A partition takes the next dataset when it is
//! free, and helps drain an open one when none is left. Balance follows
//! completion, so a collection of a million small datasets and a collection of
//! four large ones both divide over every core.
//!
//! [`AtlasFormat`]: super::AtlasFormat

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use atlas::{Atlas, DatasetView};
use beacon_nd_array::arrow::{
    file_read::FileRead,
    metrics::ReadMetrics,
    morsel::{MorselSource, OpenFile, morsel_scan},
    partition::FilePartitions,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_expr::{
        PhysicalExpr, conjunction, projection::ProjectionExprs, utils::collect_columns,
    },
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::FutureExt;
use object_store::ObjectStore;

use crate::compat;
use crate::datafusion::metrics::AtlasScanMetrics;
use crate::datafusion::pruning::{CandidateFilter, PruneCache, candidate_filter, logical_schema};
use crate::reader::{dataset_from_view, project_read_dimensions};
use crate::store::{AtlasReaderCache, get_or_open_atlas};

/// Which dataset of a collection one plan entry stands for.
///
/// Attached to [`PartitionedFile::extensions`] by
/// [`AtlasFormat::create_physical_plan`](super::AtlasFormat). The `position` is
/// the dataset's index in the `list_datasets()` call the plan made, which is
/// the row a collection-wide pruning index keys on.
#[derive(Debug, Clone)]
pub struct AtlasEntry {
    /// The dataset's name, as the collection footer states it.
    pub dataset: String,
    /// Its row in the plan-time listing.
    pub position: usize,
}

/// DataFusion [`FileSource`] for Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,
    projection: Option<ProjectionExprs>,
    /// The reader cache to consult, or `None` to open every collection afresh.
    cache: Option<AtlasReaderCache>,
    /// Whether a predicate scan drops the datasets it can rule out.
    use_pruning: bool,
    /// Each collection's pruning result, computed once for this scan and shared
    /// by every partition's opener.
    prune_cache: PruneCache,
    /// The scan's dataset queue, when it is planned morsel-driven. See
    /// [`morsel_scan`].
    morsel: Option<Arc<MorselSource>>,
}

impl AtlasSource {
    pub fn new(read_dimensions: Option<Vec<String>>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions,
            projection: None,
            cache: None,
            use_pruning: false,
            prune_cache: PruneCache::new(),
            morsel: None,
        }
    }

    /// Consult `cache` for opened collections, or open them afresh with `None`.
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Drop the datasets a predicate rules out, or read them all.
    pub fn with_pruning(mut self, use_pruning: bool) -> Self {
        self.use_pruning = use_pruning;
        self
    }

    /// Carry a projection the scan pushed down.
    ///
    /// The format rebuilds the source in `create_physical_plan`, and without
    /// this the projection pushed into the old one would be lost.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }

    /// The datasets this scan's queue holds, when it is planned morsel-driven.
    #[cfg(test)]
    pub(crate) fn morsel_datasets(&self) -> Option<usize> {
        self.morsel.as_ref().map(|source| source.files())
    }
}

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;
        let read_metrics = ReadMetrics::new(&self.execution_plan_metrics, partition);
        let scan_metrics = AtlasScanMetrics::new(&self.execution_plan_metrics, partition);

        let datasets = Arc::new(AtlasDatasets {
            object_store,
            cache: self.cache.clone(),
            // A predicate is written against the values, not the encoding the
            // scan carries them in.
            logical_schema: logical_schema(&projected_schema),
            projected_schema,
            use_pruning: self.use_pruning,
            prune_cache: self.prune_cache.clone(),
            read_dimensions: self.read_dimensions.clone(),
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            read_metrics: read_metrics.clone(),
            scan_metrics,
        });

        Ok(Arc::new(AtlasOpener {
            datasets,
            morsel: self.morsel.clone(),
            partition,
            read_metrics,
        }))
    }

    fn as_any(&self) -> &dyn Any {
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

    /// Put every dataset of the scan in one queue, and point each partition at
    /// it.
    ///
    /// Nothing is assigned here. A dataset's cost is the cells the query keeps,
    /// which no plan-time number states: two datasets of one collection differ
    /// by orders of magnitude, and a predicate prunes them unevenly. So the
    /// partitions divide the queue as they drain it.
    ///
    /// `repartition_file_min_size` is ignored, as it is for Zarr. It was the
    /// size a file had to reach before sharing it was worth the seek, and a
    /// queue makes no such bet. An atlas entry has no size of its own anyway:
    /// every dataset of a collection reports the container's.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            // A partition holding an arbitrary share of the datasets cannot
            // emit its rows in collection order.
            return Ok(None);
        }

        if let Some((morsel, file_groups)) = morsel_scan(&config.file_groups, target_partitions) {
            tracing::debug!(
                "AtlasSource morsel scan: {} datasets over {target_partitions} partitions",
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

        // The queue declined: one partition, or no datasets. Keeping the scan
        // as planned is the answer to both.
        Ok(None)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "atlas"
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        Ok(Some(Arc::new(Self {
            projection: Some(merged),
            ..self.clone()
        })))
    }

    /// Take the filters as a hint, and leave them above the scan.
    ///
    /// The scan uses a predicate twice: to skip a chunk whose coordinates
    /// cannot hold a matching row, and (once pruning lands) to skip a whole
    /// dataset whose footer statistics cannot. Neither is exact — both work in
    /// whole chunks and whole datasets — so the filter above the scan still
    /// decides each row, and `PushedDown::No` is what says so.
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
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

// ─── The opener ──────────────────────────────────────────────────────────────

/// One partition's opener.
struct AtlasOpener {
    /// How one dataset is opened, for the queue to call.
    datasets: Arc<dyn OpenFile>,
    /// The scan's queue, when it is planned morsel-driven. `Some` means the
    /// entry `FileStream` hands this opener stands for the whole scan.
    morsel: Option<Arc<MorselSource>>,
    partition: usize,
    read_metrics: ReadMetrics,
}

impl FileOpener for AtlasOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        // A morsel-driven scan hands every partition the same standing entry.
        // It is not a dataset: the datasets are in the queue, and this
        // partition reads whatever it hands out until the scan is done.
        if let Some(morsel) = &self.morsel {
            let stream = morsel.stream(
                self.partition,
                Arc::clone(&self.datasets),
                Some(self.read_metrics.clone()),
            );
            return Ok(futures::future::ready(Ok(stream)).boxed());
        }

        // One partition, or no datasets: `FileStream` walks the real entries
        // and this opener reads each one whole.
        let datasets = Arc::clone(&self.datasets);
        let metrics = self.read_metrics.clone();
        Ok(async move {
            let read = datasets.open(&file).await?;
            Ok(read.stream(Some(metrics)))
        }
        .boxed())
    }
}

/// How one Atlas dataset becomes a planned [`FileRead`].
///
/// This is everything a [`MorselSource`] needs of the format. The queue holds
/// the datasets; this says what opening one means.
struct AtlasDatasets {
    object_store: Arc<dyn ObjectStore>,
    cache: Option<AtlasReaderCache>,
    /// The scan's output schema, nd-encoded. Its field *names* are the columns
    /// to keep, and the encoding leaves names alone.
    projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped, which is what a predicate
    /// and the pruning engine are written against.
    logical_schema: SchemaRef,
    use_pruning: bool,
    prune_cache: PruneCache,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_metrics: ReadMetrics,
    scan_metrics: AtlasScanMetrics,
}

impl AtlasDatasets {
    /// Which datasets of one collection this scan's predicate can still match.
    ///
    /// Built once per collection per scan. Every partition's opener shares one
    /// memo, so the first to arrive builds the index while the rest await it;
    /// each then reads its own dataset's bit out of the result.
    ///
    /// Without a predicate, or with pruning off, nothing is ruled out and no
    /// index is built.
    async fn candidates(&self, atlas: &Arc<Atlas>, marker: &str) -> Arc<CandidateFilter> {
        let Some(predicate) = self.predicate.clone().filter(|_| self.use_pruning) else {
            return Arc::new(CandidateFilter::KeepAll);
        };
        // DataFusion offers a scan its filters even when it has none, and an
        // empty conjunction is the literal `true`. Such a predicate names no
        // column, so it can rule nothing out and is not worth a pass.
        if collect_columns(&predicate).is_empty() {
            return Arc::new(CandidateFilter::KeepAll);
        }

        let started = Instant::now();
        let atlas = Arc::clone(atlas);
        let schema = Arc::clone(&self.logical_schema);
        let metrics = self.scan_metrics.clone();
        let filter = self
            .prune_cache
            .get_or_compute(marker.to_string(), async move {
                let filter = Arc::new(candidate_filter(&atlas, &predicate, &schema).await);
                // Only an index that exists is a build. Pruning that did not
                // apply read nothing and judged nothing.
                if filter.is_index() {
                    metrics.index_builds.add(1);
                    metrics.index_rows.add(filter.rows());
                }
                filter
            })
            .await;
        self.scan_metrics.prune_time.add_elapsed(started);
        filter
    }

    /// The columns to build for one dataset, or `None` to build every one.
    ///
    /// The projection reaches the build, so an unprojected array gets no
    /// backend and an unprojected attribute is never read out of the footer.
    ///
    /// Two cases need care. A `COUNT(*)` projects nothing, and building nothing
    /// would leave the read with no grid to count; it takes the widest array of
    /// the dataset instead, which is what states the row count. And a predicate
    /// column is added to the set: the filter above the scan forces such a
    /// column into the projection today, but the chunk pruning inside the read
    /// matches columns by name and would silently stop pruning if that ever
    /// changed.
    fn projected_names(&self, view: &DatasetView) -> Option<Vec<String>> {
        if self.projected_schema.fields().is_empty() {
            return count_driver(view).map(|driver| vec![driver]);
        }

        let mut names: Vec<String> = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        if let Some(predicate) = &self.predicate {
            for column in collect_columns(predicate) {
                if !names.iter().any(|name| name == column.name()) {
                    names.push(column.name().to_string());
                }
            }
        }
        Some(names)
    }
}

/// The array a `COUNT(*)` reads to establish a dataset's row count: the widest
/// one Beacon can read.
///
/// From the footer alone, so choosing it costs no I/O and no backend. `None`
/// for a dataset with no readable array, and the caller then builds what there
/// is — an attribute-only dataset contributes the one row its scalars define.
fn count_driver(view: &DatasetView) -> Option<String> {
    view.schema()
        .arrays
        .iter()
        .filter(|(_, schema)| compat::array_dtype_to_nd(&schema.dtype).is_some())
        .max_by_key(|(_, schema)| schema.shape.iter().product::<usize>())
        .map(|(name, _)| name.clone())
}

#[async_trait::async_trait]
impl OpenFile for AtlasDatasets {
    async fn open(&self, file: &PartitionedFile) -> Result<Arc<FileRead>> {
        let entry = file
            .extensions
            .as_ref()
            .and_then(|extension| (extension.as_ref() as &dyn Any).downcast_ref::<AtlasEntry>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "the atlas scan entry at '{}' names no dataset",
                    file.object_meta.location
                ))
            })?;

        let open_start = Instant::now();
        let atlas = get_or_open_atlas(
            self.cache.as_ref(),
            Arc::clone(&self.object_store),
            &file.object_meta,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
        self.scan_metrics.open_time.add_elapsed(open_start);

        // One index per collection decides this, and the first opener to reach
        // the collection builds it. A dataset ruled out costs one pop and no
        // read at all.
        if !self
            .candidates(&atlas, file.object_meta.location.as_ref())
            .await
            .keeps(entry.position, &entry.dataset)
        {
            self.scan_metrics.datasets_pruned.add(1);
            return Ok(FileRead::skipped());
        }

        let build_start = Instant::now();
        let view = Arc::new(atlas.dataset(&entry.dataset).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to open atlas dataset '{}' of '{}': {e}",
                entry.dataset, file.object_meta.location
            ))
        })?);

        let projected = self.projected_names(&view);
        let dataset = dataset_from_view(view, projected.as_deref())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
        // Explicit dimensions, or a broadcast-compatible default. No log label:
        // this runs per dataset, and schema inference already logged the choice.
        let dataset = project_read_dimensions(dataset, self.read_dimensions.clone(), None)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

        let read = FileRead::plan(
            dataset,
            Arc::clone(&self.projected_schema),
            self.batch_size,
            self.predicate.clone(),
            // A dataset lives inside a container, not at a path, so no
            // `PARTITIONED BY` value can be read off it. The format refuses
            // such a table outright.
            FilePartitions::none(),
            Some(&self.read_metrics),
        )
        .await?;

        self.scan_metrics
            .dataset_build_time
            .add_elapsed(build_start);
        self.scan_metrics.datasets_scanned.add(1);
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;

    fn source() -> AtlasSource {
        AtlasSource::new(
            None,
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty())),
        )
    }

    fn entry(dataset: &str, position: usize) -> PartitionedFile {
        let mut file = PartitionedFile::new("obs/data.atlas", 4096);
        file.extensions = Some(Arc::new(AtlasEntry {
            dataset: dataset.to_string(),
            position,
        }));
        file
    }

    /// Every dataset of the scan goes into one queue, and each partition gets a
    /// standing entry pointing at it.
    #[test]
    fn the_datasets_go_into_one_queue() {
        const PARTITIONS: usize = 4;

        let source = source();
        let mut builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        );
        for (position, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            builder = builder.with_file(entry(name, position));
        }
        let config = builder.build();

        let planned = source
            .repartitioned(PARTITIONS, 10 * 1024 * 1024, None, &config)
            .unwrap()
            .expect("the datasets are planned across the partitions");

        assert_eq!(planned.file_groups.len(), PARTITIONS);
        for group in &planned.file_groups {
            assert_eq!(group.len(), 1, "one standing entry per partition");
        }
        let planned = planned
            .file_source()
            .as_any()
            .downcast_ref::<AtlasSource>()
            .expect("the config carries an AtlasSource");
        assert_eq!(
            planned.morsel_datasets(),
            Some(5),
            "and the queue holds every dataset"
        );
    }

    /// An ordered scan cannot share: a partition holding an arbitrary share of
    /// the datasets cannot emit its rows in collection order.
    #[test]
    fn an_ordered_scan_is_left_alone() {
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::expressions::Column;

        let source = source();
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(entry("a", 0))
        .build();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("time", 0),
        ))]);
        assert!(
            source
                .repartitioned(4, 0, ordering, &config)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn one_partition_divides_nothing() {
        let source = source();
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(entry("a", 0))
        .build();

        assert!(source.repartitioned(1, 0, None, &config).unwrap().is_none());
    }

    /// An entry that names no dataset is a bug in the planner, not bad input,
    /// and the error says which collection it came from.
    #[tokio::test]
    async fn an_entry_without_a_dataset_is_an_internal_error() {
        let datasets = AtlasDatasets {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            cache: None,
            projected_schema: Arc::new(arrow::datatypes::Schema::empty()),
            logical_schema: Arc::new(arrow::datatypes::Schema::empty()),
            use_pruning: false,
            prune_cache: PruneCache::new(),
            read_dimensions: None,
            batch_size: usize::MAX,
            predicate: None,
            read_metrics: ReadMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            scan_metrics: AtlasScanMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };

        let error = datasets
            .open(&PartitionedFile::new("obs/data.atlas", 1))
            .await
            .expect_err("an entry must name its dataset")
            .to_string();
        assert!(error.contains("names no dataset"), "{error}");
        assert!(error.contains("obs/data.atlas"), "{error}");
    }

    // ── which columns a dataset is built with ───────────────────────────

    use crate::test_support;

    /// One dataset's view. It owns what it needs, so the collection handle
    /// behind it may go.
    async fn view(dir: &std::path::Path, dataset: &str) -> DatasetView {
        test_support::open(dir)
            .await
            .dataset(dataset)
            .expect("the dataset")
    }

    fn datasets_wanting(
        projected: Vec<&str>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> AtlasDatasets {
        let fields: Vec<arrow::datatypes::Field> = projected
            .into_iter()
            .map(|name| arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Null, true))
            .collect();
        let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        AtlasDatasets {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            cache: None,
            logical_schema: Arc::clone(&projected_schema),
            projected_schema,
            use_pruning: false,
            prune_cache: PruneCache::new(),
            read_dimensions: None,
            batch_size: usize::MAX,
            predicate,
            read_metrics: ReadMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            scan_metrics: AtlasScanMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        }
    }

    #[tokio::test]
    async fn a_scan_builds_the_columns_it_projects() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let datasets = datasets_wanting(vec!["temperature"], None);
        let names = datasets
            .projected_names(&view(tmp.path(), "winter").await)
            .expect("a projection");
        assert_eq!(names, vec!["temperature".to_string()]);
    }

    /// A predicate column joins the set even when the projection leaves it out.
    /// The chunk pruning inside the read matches by name, so a missing column
    /// would silently stop it pruning.
    #[tokio::test]
    async fn a_predicate_column_is_built_too() {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("cycle", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(20)))),
        ));
        let datasets = datasets_wanting(vec!["temperature"], Some(predicate));
        let names = datasets
            .projected_names(&view(tmp.path(), "winter").await)
            .expect("a projection");
        assert_eq!(
            names,
            vec!["temperature".to_string(), "cycle".to_string()],
            "the predicate's column is kept alongside the projection"
        );
    }

    /// `COUNT(*)` projects nothing and reads one array: the row count is a
    /// property of the grid, and building every column to find it would read
    /// every attribute of the dataset for nothing.
    #[tokio::test]
    async fn a_count_reads_the_widest_array_alone() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let datasets = datasets_wanting(vec![], None);
        let names = datasets
            .projected_names(&view(tmp.path(), "grid").await)
            .expect("a driver");
        assert_eq!(names.len(), 1);
        assert!(
            names[0] == "temperature" || names[0] == "sparse",
            "either 4x6 array states the row count: {names:?}"
        );
    }
}
