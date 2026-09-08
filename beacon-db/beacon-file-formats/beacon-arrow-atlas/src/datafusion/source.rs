//! The DataFusion [`FileSource`] and [`FileOpener`] for Atlas collections.
//!
//! # One collection is one unit of work
//!
//! A plan entry is a collection: its `data.atlas` container, as the listing
//! found it. [`AtlasFormat`] dedupes the markers and deals them round-robin
//! over the target partitions, and a partition reads each collection it holds
//! from end to end. A container is never split by byte range, because a byte
//! range of one means nothing.
//!
//! # What an open does
//!
//! Opening a collection costs one footer read through the reader cache. The
//! opener then lists the datasets, prunes them in one vectorised pass over the
//! footer's statistics, and is left with the names it has to read. Those names
//! feed one stream: each dataset is built in turn, planned as a [`FileRead`],
//! and its batches are yielded before the next dataset is touched.
//!
//! So a pruned dataset costs nothing at all, and a kept one costs its build and
//! its read. Nothing is listed at plan time, and nothing is queued.
//!
//! [`AtlasFormat`]: super::AtlasFormat

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use atlas::{Atlas, DatasetView};
use beacon_nd_array::arrow::{
    file_read::FileRead, metrics::ReadMetrics, partition::FilePartitions,
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
        PhysicalExpr, conjunction,
        projection::ProjectionExprs,
        utils::{collect_columns, reassign_expr_columns},
    },
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStore;

use crate::compat;
use crate::datafusion::metrics::AtlasScanMetrics;
use crate::datafusion::pruning::{CandidateFilter, candidate_filter, logical_schema};
use crate::reader::{dataset_from_view, project_read_dimensions};
use crate::store::{AtlasReaderCache, get_or_open_atlas};

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
}

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;
        Ok(Arc::new(AtlasOpener {
            object_store,
            cache: self.cache.clone(),
            // A predicate is written against the values, not the encoding the
            // scan carries them in.
            logical_schema: logical_schema(&projected_schema),
            projected_schema,
            use_pruning: self.use_pruning,
            read_dimensions: self.read_dimensions.clone(),
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            read_metrics: ReadMetrics::new(&self.execution_plan_metrics, partition),
            scan_metrics: AtlasScanMetrics::new(&self.execution_plan_metrics, partition),
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

    /// A container is one unit. A byte range of it names nothing a reader can
    /// open, so the plan's groups stand as the format dealt them.
    fn supports_repartitioning(&self) -> bool {
        false
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
    /// The scan uses a predicate twice: to skip a whole dataset whose recorded
    /// statistics cannot hold a matching row, and to skip a chunk whose
    /// coordinates cannot. Neither is exact — both work in whole datasets and
    /// whole chunks — so the filter above the scan still decides each row, and
    /// `PushedDown::No` is what says so.
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

/// One partition's opener: a collection in, its batches out.
///
/// Every field is a handle or a clone, so the opener itself is cloned into the
/// stream it returns and outlives the call that made it.
#[derive(Clone)]
struct AtlasOpener {
    object_store: Arc<dyn ObjectStore>,
    cache: Option<AtlasReaderCache>,
    /// The scan's output schema, nd-encoded. Its field *names* are the columns
    /// to keep, and the encoding leaves names alone.
    projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped, which is what a predicate
    /// and the pruning engine are written against.
    logical_schema: SchemaRef,
    use_pruning: bool,
    read_dimensions: Option<Vec<String>>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_metrics: ReadMetrics,
    scan_metrics: AtlasScanMetrics,
}

impl FileOpener for AtlasOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let opener = self.clone();
        Ok(async move {
            let collection = file.object_meta.location.to_string();

            let open_start = Instant::now();
            let atlas = get_or_open_atlas(
                opener.cache.as_ref(),
                Arc::clone(&opener.object_store),
                &file.object_meta,
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
            opener.scan_metrics.open_time.add_elapsed(open_start);

            let listed = atlas.list_datasets();
            let total = listed.len();
            let names = opener.survivors(&atlas, listed).await;
            tracing::debug!(
                collection = %collection,
                datasets = total,
                kept = names.len(),
                "atlas scan: collection opened and pruned"
            );

            // One stream over the kept datasets. Each is built when the stream
            // reaches it, and its batches drain before the next is touched.
            let metrics = opener.read_metrics.clone();
            let batches = futures::stream::iter(names)
                .then(move |name| {
                    let opener = opener.clone();
                    let atlas = Arc::clone(&atlas);
                    let collection = collection.clone();
                    async move { opener.dataset_read(&atlas, &collection, &name).await }
                })
                .map_ok(move |read| read.stream(Some(metrics.clone())))
                .try_flatten();

            Ok(batches.boxed())
        }
        .boxed())
    }
}

impl AtlasOpener {
    /// The datasets of `atlas` this scan reads, in listing order.
    ///
    /// One pruning pass over the whole collection decides it, and a dataset
    /// ruled out never comes back: no handle, no build, no read.
    async fn survivors(&self, atlas: &Arc<Atlas>, listed: Vec<String>) -> Vec<String> {
        let filter = self.candidates(atlas).await;

        let mut kept = Vec::with_capacity(listed.len());
        let mut pruned = 0usize;
        for (position, name) in listed.into_iter().enumerate() {
            if filter.keeps(position, &name) {
                kept.push(name);
            } else {
                pruned += 1;
            }
        }
        self.scan_metrics.datasets_pruned.add(pruned);
        kept
    }

    /// Which datasets of `atlas` this scan's predicate can still match.
    ///
    /// Without a predicate, or with pruning off, nothing is ruled out and no
    /// index is built.
    async fn candidates(&self, atlas: &Arc<Atlas>) -> CandidateFilter {
        let Some(predicate) = self.predicate.clone().filter(|_| self.use_pruning) else {
            return CandidateFilter::KeepAll;
        };
        // DataFusion offers a scan its filters even when it has none, and an
        // empty conjunction is the literal `true`. Such a predicate names no
        // column, so it can rule nothing out and is not worth a pass.
        if collect_columns(&predicate).is_empty() {
            return CandidateFilter::KeepAll;
        }

        // The predicate is indexed against the table schema, the pruning
        // engine reads it against the projected one. A projection pushed down
        // after the filter leaves the two out of step, so the columns are
        // re-indexed by name here.
        let Ok(predicate) = reassign_expr_columns(predicate, &self.logical_schema) else {
            return CandidateFilter::KeepAll;
        };

        let started = Instant::now();
        let filter = candidate_filter(atlas, &predicate, &self.logical_schema).await;
        // Only an index that exists is a build. Pruning that did not apply
        // read nothing and judged nothing.
        if filter.is_index() {
            self.scan_metrics.index_builds.add(1);
            self.scan_metrics.index_rows.add(filter.rows());
        }
        self.scan_metrics.prune_time.add_elapsed(started);
        filter
    }

    /// One dataset, built and planned as a [`FileRead`].
    async fn dataset_read(
        &self,
        atlas: &Arc<Atlas>,
        collection: &str,
        name: &str,
    ) -> Result<Arc<FileRead>> {
        let build_start = Instant::now();
        let view = Arc::new(atlas.dataset(name).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to open atlas dataset '{name}' of '{collection}': {e}"
            ))
        })?);

        let projected = self.projected_names(&view).await?;
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
    async fn projected_names(&self, view: &DatasetView) -> Result<Option<Vec<String>>> {
        if self.projected_schema.fields().is_empty() {
            return Ok(count_driver(view, self.read_dimensions.as_deref())
                .await?
                .map(|driver| vec![driver]));
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
        Ok(Some(names))
    }
}

/// The array a `COUNT(*)` reads to establish a dataset's row count: the widest
/// one Beacon can read.
///
/// The footer names the candidates, and their sizes come from the segments
/// those arrays live in — one open each for the whole collection, however many
/// datasets a `COUNT(*)` walks. No array data is read, and the driver gets the
/// only backend the dataset builds.
///
/// `None` for a dataset with no readable array, and the caller then builds what
/// there is — an attribute-only dataset contributes the one row its scalars
/// define.
///
/// `read_dimensions` rules out the arrays the narrowing after the build would
/// drop. The widest array of a dataset is the one on the most dimensions, and
/// a query that asked for fewer would lose exactly that one and leave the read
/// with no grid at all.
async fn count_driver(
    view: &DatasetView,
    read_dimensions: Option<&[String]>,
) -> Result<Option<String>> {
    let readable: Vec<String> = view
        .schema()
        .iter()
        .filter(|meta| compat::array_dtype_to_nd(meta.dtype()).is_some())
        .map(|meta| meta.name().to_string())
        .collect();

    let mut widest: Option<(String, usize)> = None;
    for array in readable {
        let layout = view.array_layout(&array).await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to read the layout of atlas array '{array}' of dataset '{}': {e}",
                view.name()
            ))
        })?;
        if let Some(wanted) = read_dimensions
            && !layout
                .dimension_names()
                .iter()
                .all(|dim| wanted.iter().any(|kept| kept == dim))
        {
            continue;
        }
        let cells = layout.element_count();
        if widest.as_ref().is_none_or(|(_, held)| cells > *held) {
            widest = Some((array, cells));
        }
    }
    Ok(widest.map(|(array, _)| array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use datafusion::physical_plan::metrics::MetricsSet;

    fn source() -> AtlasSource {
        AtlasSource::new(
            None,
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty())),
        )
    }

    /// A container is one unit of work, so DataFusion may not split it into
    /// byte ranges the way it would a Parquet file.
    #[test]
    fn a_container_is_never_split() {
        assert!(!source().supports_repartitioning());
    }

    // ── opening a collection ────────────────────────────────────────────

    /// An opener over the collection in `dir`, projecting every column, with
    /// the metrics it reports.
    async fn opener(
        dir: &std::path::Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        use_pruning: bool,
    ) -> (AtlasOpener, ExecutionPlanMetricsSet) {
        use crate::reader::collection_schema;
        use beacon_datafusion_ext::type_widening::ArrowTypeWidening;

        let atlas = test_support::open(dir).await;
        let logical = collection_schema(&atlas, None, "c", &ArrowTypeWidening::default_extension())
            .await
            .unwrap();
        let projected_schema: SchemaRef =
            Arc::new(beacon_datafusion_ext::nd::encoded_schema(&logical));

        let metrics = ExecutionPlanMetricsSet::new();
        let (store, _) = test_support::store_and_marker(dir);
        let opener = AtlasOpener {
            object_store: store,
            cache: None,
            logical_schema: logical_schema(&projected_schema),
            projected_schema,
            use_pruning,
            read_dimensions: None,
            batch_size: usize::MAX,
            predicate,
            read_metrics: ReadMetrics::new(&metrics, 0),
            scan_metrics: AtlasScanMetrics::new(&metrics, 0),
        };
        (opener, metrics)
    }

    fn count_of(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        let set: MetricsSet = metrics.clone_inner();
        set.sum_by_name(name).map_or(0, |value| value.as_usize())
    }

    /// One open, then every dataset of the collection, in listing order.
    #[tokio::test]
    async fn an_open_streams_every_dataset_of_the_collection() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let (opener, metrics) = opener(tmp.path(), None, true).await;
        let (_, marker) = test_support::store_and_marker(tmp.path());

        let stream = opener
            .open(PartitionedFile::from(marker))
            .unwrap()
            .await
            .expect("the collection opens");
        let batches: Vec<_> = stream.try_collect().await.expect("every dataset reads");

        // Six datasets, one chunk each, one nd batch per chunk.
        assert_eq!(batches.len(), 6);
        assert_eq!(count_of(&metrics, "atlas_datasets_scanned"), 6);
        assert_eq!(count_of(&metrics, "atlas_datasets_pruned"), 0);
        assert_eq!(
            count_of(&metrics, "atlas_index_builds"),
            0,
            "no predicate, no index"
        );
    }

    /// A predicate prunes once for the collection, before any dataset is
    /// built. A ruled-out dataset then costs no build and no read.
    #[tokio::test]
    async fn an_open_prunes_the_collection_before_it_reads() {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        // Datasets hold 10i..=10i+3, so only d5 (50..=53) reaches past 45.
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        ));
        let (opener, metrics) = opener(tmp.path(), Some(predicate), true).await;
        let (_, marker) = test_support::store_and_marker(tmp.path());

        let stream = opener
            .open(PartitionedFile::from(marker))
            .unwrap()
            .await
            .expect("the collection opens");
        let batches: Vec<_> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1, "one dataset survives");
        assert_eq!(count_of(&metrics, "atlas_datasets_scanned"), 1);
        assert_eq!(count_of(&metrics, "atlas_datasets_pruned"), 5);
        assert_eq!(count_of(&metrics, "atlas_index_builds"), 1);
        assert_eq!(count_of(&metrics, "atlas_index_rows"), 6);
    }

    /// With pruning off, every dataset is built and read.
    ///
    /// The predicate still reaches [`FileRead::plan`], where the chunk mask over
    /// a 1-D coordinate skips chunks no row of which can match. That layer is
    /// always on and is not what the switch controls, so this pins the dataset
    /// metrics and not the batch count.
    #[tokio::test]
    async fn pruning_off_reads_every_dataset() {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        ));
        let (opener, metrics) = opener(tmp.path(), Some(predicate), false).await;
        let (_, marker) = test_support::store_and_marker(tmp.path());

        let stream = opener
            .open(PartitionedFile::from(marker))
            .unwrap()
            .await
            .unwrap();
        let _batches: Vec<_> = stream.try_collect().await.unwrap();

        assert_eq!(
            count_of(&metrics, "atlas_datasets_scanned"),
            6,
            "every dataset is built"
        );
        assert_eq!(count_of(&metrics, "atlas_datasets_pruned"), 0);
        assert_eq!(
            count_of(&metrics, "atlas_index_builds"),
            0,
            "no index is built"
        );
    }

    /// A path that is not a container fails at the open, and the error names
    /// what a collection is called.
    #[tokio::test]
    async fn a_path_that_is_not_a_container_fails_to_open() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (opener, _) = opener(tmp.path(), None, false).await;

        let error = opener
            .open(PartitionedFile::new("obs/index.json", 1))
            .unwrap()
            .await
            .err()
            .expect("only the container names a collection")
            .to_string();
        assert!(error.contains("data.atlas"), "{error}");
    }

    // ── which columns a dataset is built with ───────────────────────────

    /// One dataset's view. It owns what it needs, so the collection handle
    /// behind it may go.
    async fn view(dir: &std::path::Path, dataset: &str) -> DatasetView {
        test_support::open(dir)
            .await
            .dataset(dataset)
            .expect("the dataset")
    }

    fn opener_wanting(
        projected: Vec<&str>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> AtlasOpener {
        let fields: Vec<arrow::datatypes::Field> = projected
            .into_iter()
            .map(|name| arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Null, true))
            .collect();
        let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        AtlasOpener {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            cache: None,
            logical_schema: Arc::clone(&projected_schema),
            projected_schema,
            use_pruning: false,
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

        let opener = opener_wanting(vec!["temperature"], None);
        let names = opener
            .projected_names(&view(tmp.path(), "winter").await)
            .await
            .expect("the layouts resolve")
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
        let opener = opener_wanting(vec!["temperature"], Some(predicate));
        let names = opener
            .projected_names(&view(tmp.path(), "winter").await)
            .await
            .expect("the layouts resolve")
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

        let opener = opener_wanting(vec![], None);
        let names = opener
            .projected_names(&view(tmp.path(), "grid").await)
            .await
            .expect("the layouts resolve")
            .expect("a driver");
        assert_eq!(names.len(), 1);
        assert!(
            names[0] == "temperature" || names[0] == "sparse",
            "either 4x6 array states the row count: {names:?}"
        );
    }
}
