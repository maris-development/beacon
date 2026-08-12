//! DataFusion [`FileSource`]/[`FileOpener`] for atlas stores.
//!
//! Each `PartitionedFile` is one atlas store's metadata marker plus a slice of
//! that store's dataset names (attached as [`AtlasDatasetSlice`] in the file's
//! `extensions` by [`AtlasFormat::create_physical_plan`](super::AtlasFormat)).
//! The opener opens the store once over the query's object store and reads only
//! its assigned datasets — so a store's datasets are spread across DataFusion
//! partitions and scanned on every core in parallel. Each dataset is built with
//! just the projected columns and streamed through the shared `beacon-nd-array`
//! engine (predicate row-masking via [`PushdownFilter`]).

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter, schema::any_dataset_to_arrow_schema,
    },
    dataset::resolve_read_dimensions,
    projection::DatasetProjection,
};
use datafusion::physical_expr_adapter::BatchAdapterFactory;
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{PhysicalExpr, conjunction, projection::ProjectionExprs},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::future;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::cache::AtlasReaderCache;
use crate::datafusion::metrics::AtlasScanMetrics;
use crate::datafusion::pruning::PruneCache;

/// How many of a partition's datasets the opener reads concurrently. Overlaps
/// per-dataset I/O and decompression within a partition; cross-partition
/// parallelism comes from DataFusion running the partitions on separate cores.
const ATLAS_DATASET_CONCURRENCY: usize = 8;

/// The slice of a store's dataset names assigned to one scan partition.
///
/// Attached to each [`PartitionedFile::extensions`] by
/// [`AtlasFormat::create_physical_plan`](super::AtlasFormat). When absent (e.g.
/// a source built outside the physical plan), the opener falls back to reading
/// every dataset in the store.
#[derive(Debug, Clone)]
pub struct AtlasDatasetSlice {
    pub names: Vec<String>,
}

/// DataFusion [`FileSource`] for atlas stores.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<AtlasReaderCache>,
    /// Whether to prune non-matching datasets before reading them.
    use_pruning: bool,
    /// Per-query memo of each store's pruning result, shared across the scan's
    /// partition openers so a store is pruned once, not once per partition.
    prune_cache: PruneCache,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl AtlasSource {
    pub fn new(read_dimensions: Option<Vec<String>>, table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions,
            cache: None,
            use_pruning: false,
            prune_cache: PruneCache::new(),
            projection: None,
        }
    }

    /// Returns a copy of this source that consults `cache` (when `Some`) for
    /// opened atlas stores. The format wires in the runtime's shared cache here.
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Enable or disable dataset pruning for this scan.
    pub fn with_pruning(mut self, use_pruning: bool) -> Self {
        self.use_pruning = use_pruning;
        self
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source in
    /// `create_physical_plan`.
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
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(AtlasOpener {
            object_store,
            projected_schema,
            batch_size: self.batch_size,
            metrics: self.execution_plan_metrics.clone(),
            partition,
            read_dimensions: self.read_dimensions.clone(),
            predicate: self.predicate.clone(),
            cache: self.cache.clone(),
            use_pruning: self.use_pruning,
            prune_cache: self.prune_cache.clone(),
        }))
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

    /// Whether a scan may split one file across partitions. It may not.
    ///
    /// Atlas divides its own work, and it divides it by dataset name.
    /// `create_physical_plan` opens each store, lists its datasets, and gives
    /// every file group one slice of those names in
    /// [`PartitionedFile::extensions`]. Every slice carries the *same*
    /// `object_meta`, because they all name the same store.
    ///
    /// DataFusion's partitioner splits by byte range and copies the extensions
    /// into each share. Two shares of one slice would therefore carry the same
    /// [`AtlasDatasetSlice`], and the opener reads by name and never looks at a
    /// byte range, so each share would return the same datasets over again.
    ///
    /// Declining the split does not cost parallelism. The name slices already
    /// give one partition per share of a store's datasets.
    ///
    /// [`PartitionedFile::extensions`]: datafusion::datasource::listing::PartitionedFile::extensions
    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "atlas"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(factory),
            ..self.clone()
        }))
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
        // Merge with any projection already pushed down, then record it on a new
        // source. `FileScanConfig::projected_schema` reads this back via
        // `projection()`, and the opener's schema adapter applies it per dataset.
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

// ─── FileOpener ────────────────────────────────────────────────────────────

struct AtlasOpener {
    object_store: Arc<dyn ObjectStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    read_dimensions: Option<Vec<String>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Option<AtlasReaderCache>,
    use_pruning: bool,
    prune_cache: PruneCache,
}

impl AtlasOpener {
    #[allow(clippy::too_many_arguments)]
    async fn read_task(
        object_store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        assigned_names: Option<Vec<String>>,
        projected_schema: SchemaRef,
        batch_size: usize,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<AtlasReaderCache>,
        use_pruning: bool,
        prune_cache: PruneCache,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let scan_metrics = AtlasScanMetrics::new(&metrics, partition);
        let read_metrics = DatasetReadMetrics::new(&metrics, partition);

        let open_start = Instant::now();
        let atlas =
            crate::datafusion::cache::get_or_open_atlas(cache.as_ref(), object_store, &object_meta)
                .await?;
        scan_metrics.open_time.add_elapsed(open_start);
        let object_path = object_meta.location.clone();

        // The physical plan assigns each partition its slice of dataset names;
        // fall back to the whole store if a source was built without one.
        let names = assigned_names.unwrap_or_else(|| atlas.list_datasets());
        let assigned = names.len();

        // Skip datasets the predicate proves can't match, before opening them.
        // The store is pruned once per query (memoized by marker path) and every
        // partition's opener reuses the result; only the projected schema is
        // needed — it carries the predicate columns (a filter that stays above
        // the scan forces them in). Fails open.
        let names = match (use_pruning, &predicate) {
            (true, Some(pred)) => {
                let prune_start = Instant::now();
                let atlas = atlas.clone();
                let pred = pred.clone();
                let schema = projected_schema.clone();
                let filter = prune_cache
                    .get_or_compute(object_path.to_string(), async move {
                        Arc::new(
                            crate::datafusion::pruning::candidate_set(&atlas, &pred, &schema).await,
                        )
                    })
                    .await;
                let kept = filter.retain(names);
                scan_metrics.prune_time.add_elapsed(prune_start);
                kept
            }
            _ => names,
        };
        scan_metrics.datasets_pruned.add(assigned - names.len());
        scan_metrics.datasets_scanned.add(names.len());

        let stream = futures::stream::iter(names)
            .map(move |dataset_name| {
                let atlas = atlas.clone();
                let projected_schema = projected_schema.clone();
                let read_dimensions = read_dimensions.clone();
                let read_metrics = read_metrics.clone();
                let scan_metrics = scan_metrics.clone();
                let object_path = object_path.clone();
                let predicate = predicate.clone();
                async move {
                    read_one_dataset(
                        atlas,
                        dataset_name,
                        object_path,
                        projected_schema,
                        read_dimensions,
                        predicate,
                        batch_size,
                        Some(read_metrics),
                        scan_metrics,
                    )
                    .await
                }
            })
            .buffer_unordered(ATLAS_DATASET_CONCURRENCY)
            .try_flatten()
            .boxed();

        Ok(stream)
    }
}

/// Build and stream one atlas dataset's projected columns, adapting each batch
/// onto the query's projected output schema.
#[allow(clippy::too_many_arguments)]
async fn read_one_dataset(
    atlas: Arc<atlas::Atlas>,
    dataset_name: String,
    object_path: object_store::path::Path,
    projected_schema: SchemaRef,
    read_dimensions: Option<Vec<String>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    metrics: Option<DatasetReadMetrics>,
    scan_metrics: AtlasScanMetrics,
) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
    // Time everything up to handing back the (still-lazy) stream: opening the
    // view, reading metadata + projected attributes, wiring backends, and
    // building the schema adapter. Array *data* is read later, as the returned
    // stream is polled, and is counted by `DatasetReadMetrics` (rows/batches).
    let build_start = Instant::now();

    // Push the query's projection straight into the dataset build: only the
    // requested columns get backends, and only their attribute values are read
    // from disk. `projected_schema` is the scan's output schema, so its field
    // names are the exact column set to keep (it also carries any predicate
    // columns, since a not-fully-consumed filter stays above the scan). A
    // dataset simply omits any name it doesn't declare. An empty projection is
    // `COUNT(*)` — no column names, so nothing is built here.
    let projected_names: Vec<String> = projected_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let projected = crate::reader::dataset_from_atlas(atlas.clone(), &dataset_name, Some(&projected_names))
        .await
        .map_err(|e| {
            tracing::warn!(dataset = %dataset_name, path = %object_path, error = %e, "failed to read atlas dataset");
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to read atlas dataset '{dataset_name}' at {object_path}: {e}"
            ))
        })?;

    // Apply explicit dimensions, or narrow to a broadcast-compatible default so
    // a mix of incompatible dimension sets can't fail the scan. No log label:
    // this runs per dataset (logging happens in schema inference).
    let projected = match resolve_read_dimensions(&projected, read_dimensions, None) {
        Some(dims) => projected
            .project(&DatasetProjection::new_with_dimension_projection(dims))
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project atlas dataset '{dataset_name}' with dimensions: {e}"
                ))
            })?,
        None => projected,
    };

    let stream = if !projected.dataset().arrays.is_empty() {
        // Adapt each batch onto the scan's output schema. The dataset's arrays
        // carry their own (per-dataset) dtypes, which the merged table schema
        // may have widened — so the source schema reflects the native types and
        // the adapter casts each column up to the super-type and null-fills any
        // projected column this dataset doesn't declare, keyed by name.
        stream_adapted(projected, projected_schema, predicate, batch_size, metrics)?
    } else {
        // This dataset declares *none* of the projected columns. Its rows must
        // still appear, null-filled to the table schema (union semantics):
        // selecting a column the dataset lacks yields its rows as nulls, and
        // `COUNT(*)` (an empty projection) counts them. Establish the row count
        // from the dataset's largest readable array, then let the adapter
        // null-fill every projected column. The predicate is dropped here —
        // those columns are all null, and the not-fully-consumed filter kept
        // above the scan re-checks them, so correctness holds.
        match driver_dataset(&atlas, &dataset_name, &object_path).await? {
            Some(driver) => stream_adapted(driver, projected_schema, None, batch_size, metrics)?,
            None => {
                // No readable array either (attribute-only / bool / list
                // dataset): contribute a single broadcast row.
                let batch = null_row_batch(&projected_schema)?;
                futures::stream::once(async move { Ok(batch) }).boxed()
            }
        }
    };

    scan_metrics.dataset_build_time.add_elapsed(build_start);
    Ok(stream)
}

/// Adapt a dataset's record-batch stream onto `projected_schema`: cast each
/// column to the table's (possibly widened) type and null-fill any projected
/// column the dataset lacks, matching by name.
fn stream_adapted(
    dataset: beacon_nd_array::dataset::AnyDataset,
    projected_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    metrics: Option<DatasetReadMetrics>,
) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
    let source_schema: SchemaRef = Arc::new(any_dataset_to_arrow_schema(&dataset).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to derive Arrow schema for atlas dataset: {e}"
        ))
    })?);
    let adapter = BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

    let pushdown_filter: Option<PushdownFilter> = predicate.map(PushdownFilter::new);
    let stream = any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter, metrics)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Error reading atlas dataset as Arrow stream: {e}"
            ))
        })
        .and_then(move |batch| {
            let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to adapt atlas batch schema: {e}"
                ))
            });
            future::ready(mapped)
        })
        .boxed();

    Ok(stream)
}

/// Build a single-column dataset over `dataset_name`'s largest readable array,
/// used only to establish the dataset's row count when the query projects no
/// column it declares. `None` if the dataset has no readable array.
async fn driver_dataset(
    atlas: &Arc<atlas::Atlas>,
    dataset_name: &str,
    object_path: &object_store::path::Path,
) -> datafusion::error::Result<Option<beacon_nd_array::dataset::AnyDataset>> {
    let view = atlas.open_dataset(dataset_name).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open atlas dataset '{dataset_name}' at {object_path}: {e}"
        ))
    })?;
    let driver = view
        .schema()
        .arrays
        .iter()
        .filter(|(_, s)| crate::compat::atlas_array_dtype_to_arrow(&s.dtype).is_some())
        .max_by_key(|(_, s)| s.shape.iter().product::<usize>())
        .map(|(name, _)| name.clone());

    let Some(driver) = driver else {
        return Ok(None);
    };
    let names = [driver];
    let dataset = crate::reader::dataset_from_atlas(atlas.clone(), dataset_name, Some(&names))
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to read atlas dataset '{dataset_name}' at {object_path}: {e}"
            ))
        })?;
    Ok(Some(dataset))
}

/// A single all-null row shaped as `schema` (0 columns → a 0-column, 1-row
/// batch, the `COUNT(*)` unit for a scalar-only dataset).
fn null_row_batch(schema: &SchemaRef) -> datafusion::error::Result<RecordBatch> {
    let columns: Vec<arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_null_array(f.data_type(), 1))
        .collect();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Failed to build null row batch: {e}"))
    })
}

impl FileOpener for AtlasOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let assigned_names = file
            .extensions
            .as_ref()
            .and_then(|ext| (ext.as_ref() as &dyn Any).downcast_ref::<AtlasDatasetSlice>())
            .map(|slice| slice.names.clone());
        let fut = Self::read_task(
            self.object_store.clone(),
            file.object_meta,
            assigned_names,
            self.projected_schema.clone(),
            self.batch_size,
            self.metrics.clone(),
            self.partition,
            self.read_dimensions.clone(),
            self.predicate.clone(),
            self.cache.clone(),
            self.use_pruning,
            self.prune_cache.clone(),
        );
        Ok(Box::pin(fut))
    }
}

#[cfg(test)]
mod repartition_tests {
    //! Atlas divides its own work, by dataset name, so it must refuse
    //! DataFusion's byte-range split.

    use std::sync::Arc;

    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;

    use super::AtlasSource;

    /// A store never splits by byte range.
    ///
    /// Every name slice of a store carries the same `object_meta`, and the
    /// partitioner copies `extensions` into each share it makes. Two shares of
    /// one slice would therefore read the same datasets twice. The count would
    /// grow with `target_partitions`, silently.
    #[test]
    fn a_store_never_splits_by_byte_range() {
        let table_schema =
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
        let source = AtlasSource::new(None, table_schema);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        // Comfortably over the partitioner's minimum split size.
        .with_file(PartitionedFile::new("store.atlas", 64 * 1024 * 1024))
        .build();

        assert!(!source.supports_repartitioning());
        assert!(
            source.repartitioned(4, 1, None, &config).unwrap().is_none(),
            "an atlas store must not split by byte range"
        );
    }
}

#[cfg(test)]
mod adapter_tests {
    //! The per-dataset schema adaptation contract, exercised directly.
    //!
    //! [`stream_adapted`] leans entirely on `BatchAdapterFactory`: a dataset is
    //! read at its own native dtypes and every batch is mapped onto the merged
    //! table schema. These pin that mapping — cast up, null-fill by name — without
    //! building an atlas store or a DataFusion session, so a behaviour change in
    //! the adapter surfaces here rather than as a wrong query result.

    use std::sync::Arc;

    use arrow::array::{Array, Int16Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr_adapter::BatchAdapterFactory;

    /// Map `batch` (in `source`) onto `target`, exactly as `stream_adapted` does.
    fn adapt(
        source: Arc<Schema>,
        target: Arc<Schema>,
        batch: RecordBatch,
    ) -> datafusion::error::Result<RecordBatch> {
        BatchAdapterFactory::new(target)
            .make_adapter(&source)?
            .adapt_batch(&batch)
    }

    #[test]
    fn casts_a_narrower_dataset_dtype_up_to_the_merged_type() {
        // The widening case: one dataset stores Int16, the collection merged to Float32.
        let source = Arc::new(Schema::new(vec![Field::new("value", DataType::Int16, true)]));
        let target = Arc::new(Schema::new(vec![Field::new("value", DataType::Float32, true)]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(Int16Array::from(vec![1, 2]))],
        )
        .expect("source batch");

        let out = adapt(source, target, batch).expect("adapt");
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float32);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .expect("Float32 column");
        assert_eq!(col.values(), &[1.0, 2.0]);
    }

    #[test]
    fn null_fills_a_column_the_dataset_does_not_declare() {
        // The dataset has `value`; the table also has `flag`, which this dataset
        // lacks. Its rows must survive with `flag` null — union semantics.
        let source = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        let target = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Int32, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .expect("source batch");

        let out = adapt(source, target, batch).expect("adapt");
        assert_eq!(out.num_rows(), 2, "rows are kept, not dropped");
        let flag = out.column(out.schema().index_of("flag").expect("flag"));
        assert_eq!(flag.null_count(), 2, "missing column is entirely null");
        let value = out
            .column(out.schema().index_of("value").expect("value"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        assert_eq!(value.values(), &[10, 20], "declared column is untouched");
    }

    #[test]
    fn stringifies_a_numeric_dataset_when_the_merge_widened_to_utf8() {
        // Non-numeric conflict: atlas merges String ∪ Int64 to String, so the
        // integer dataset must be cast *into* Utf8 rather than erroring. This is
        // what makes a mixed-dtype collection readable at all.
        let source = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        let target = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            source.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("source batch");

        let out = adapt(source, target, batch).expect("Int64 -> Utf8 must be castable");
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        assert_eq!(
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>(),
            vec!["1", "2"],
        );
    }
}
