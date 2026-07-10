use std::collections::HashMap;
use std::sync::Arc;

use crate::datafusion::cache::AtlasReaderCache;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::arrow::{
    batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
    pushdown_filter::PushdownFilter,
};
use beacon_object_storage::DatasetsStore;
use datafusion::physical_expr_adapter::BatchAdapterFactory;
use datafusion::{
    common::Statistics,
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
use object_store::ObjectMeta;
use tokio::sync::Mutex;

const ATLAS_DATASET_CONCURRENCY: usize = 8;

/// Per-file payload attached to each `PartitionedFile.extensions`.
///
/// The same `atlas.json` ObjectMeta is reused across all
/// `PartitionedFile`s belonging to one atlas store; the dataset name
/// stored here selects which atlas dataset the opener should read.
#[derive(Debug, Clone)]
pub struct AtlasStreamDatasets {
    pub stream: Arc<crossbeam::queue::SegQueue<String>>,
}

impl AtlasStreamDatasets {
    pub fn new(dataset_names: Vec<String>) -> Self {
        let stream = Arc::new(crossbeam::queue::SegQueue::new());
        for name in dataset_names {
            stream.push(name);
        }
        Self { stream }
    }
}

/// DataFusion [`FileSource`] for atlas stores.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,
    /// Reader cache to consult for this scan. `None` disables caching.
    cache: Option<AtlasReaderCache>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,

    stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
}

impl AtlasSource {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions,
            cache: None,
            projection: None,
            stream_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a copy of this source that consults `cache` (when `Some`) for
    /// opened atlas stores. The format wires in the runtime's shared cache here.
    pub fn with_cache(mut self, cache: Option<AtlasReaderCache>) -> Self {
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

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(AtlasOpener {
            datasets_object_store: self.datasets_object_store.clone(),
            projected_schema,
            batch_size: self.batch_size,
            metrics: self.execution_plan_metrics.clone(),
            partition,
            read_dimensions: self.read_dimensions.clone(),
            predicate: self.predicate.clone(),
            cache: self.cache.clone(),
            stream_cache: self.stream_cache.clone(),
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

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "atlas"
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
        // Merge with any projection already pushed down, then record it on a
        // new source. `FileScanConfig::projected_schema` reads this back via
        // `projection()`, and the opener's schema adapter applies it per
        // atlas dataset.
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
    datasets_object_store: Arc<DatasetsStore>,
    projected_schema: SchemaRef,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    read_dimensions: Option<Vec<String>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Option<AtlasReaderCache>,

    stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
}

impl AtlasOpener {
    #[allow(clippy::too_many_arguments)]
    async fn read_task(
        datasets_object_store: Arc<DatasetsStore>,
        object_meta: ObjectMeta,
        stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
        projected_schema: SchemaRef,
        batch_size: usize,
        metrics: Option<DatasetReadMetrics>,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<AtlasReaderCache>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let atlas = crate::datafusion::cache::get_or_open_atlas(
            cache.as_ref(),
            datasets_object_store,
            &object_meta,
        )
        .await?;
        let object_path = object_meta.location.clone();

        let stream_datasets_handle = {
            let mut guard = stream_cache.lock().await;
            guard
                .entry(object_path.to_string())
                .or_insert_with(|| {
                    let dataset_names = atlas.list_datasets();
                    AtlasStreamDatasets::new(dataset_names)
                })
                .clone()
        };

        let queue = stream_datasets_handle.stream;
        let futures_stream = futures::stream::iter(std::iter::from_fn(move || queue.pop()));

        let stream = futures_stream
            .map(move |dataset_name| {
                let atlas = atlas.clone();
                let projected_schema = projected_schema.clone();
                let read_dimensions = read_dimensions.clone();
                let metrics = metrics.clone();
                let object_path = object_path.clone();
                {
                    let predicate_cloned = predicate.clone();
                    async move {
                        let view = atlas.open_dataset(&dataset_name).await.map_err(|e| {
                        tracing::warn!(dataset = %dataset_name, path = %object_path, error = %e, "failed to open atlas dataset");
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to open atlas dataset '{dataset_name}' at {object_path}: {e}"
                        ))
                    })?;

                        // Derive the file schema from atlas metadata alone (no backends),
                        // then ask the schema adapter which fields the query needs.
                        let file_schema: SchemaRef = crate::compat::atlas_view_arrow_schema(
                        &view,
                        read_dimensions.as_deref(),
                    )
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to derive Arrow schema for atlas dataset '{dataset_name}': {e}"
                        ))
                    })?
                    .into();

                        // Columns of this dataset that the query needs, in file
                        // order — used both to prune the read and as the source
                        // schema for the batch adapter.
                        let projection: Vec<usize> = file_schema
                            .fields()
                            .iter()
                            .enumerate()
                            .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
                            .map(|(i, _)| i)
                            .collect();

                        if projection.is_empty() {
                            return Ok::<
                                BoxStream<'static, datafusion::error::Result<RecordBatch>>,
                                datafusion::error::DataFusionError,
                            >(futures::stream::empty().boxed());
                        }

                        // Build the AnyDataset with only the queried-and-available columns.
                        let projected_names: Vec<String> = projection
                            .iter()
                            .map(|i| file_schema.field(*i).name().clone())
                            .collect();

                        // Adapt batches (read with `projection`) onto the
                        // projected output schema: reorder, cast, and null-fill
                        // columns this dataset does not provide.
                        let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
                        let adapter = BatchAdapterFactory::new(projected_schema)
                            .make_adapter(&source_schema)?;

                        let dataset = crate::reader::dataset_from_atlas(
                            atlas,
                            &dataset_name,
                            Some(&projected_names),
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Failed to load atlas dataset '{dataset_name}': {e}"
                            ))
                        })?;

                        // Atlas has no filter pushdown today.
                        let pushdown_filter: Option<PushdownFilter> =
                            predicate_cloned.map(PushdownFilter::new);
                        let dataset_stream = any_dataset_as_record_batch_stream(
                            dataset,
                            batch_size,
                            pushdown_filter,
                            metrics,
                        )
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

                        Ok(dataset_stream)
                    }
                }
            })
            .buffer_unordered(ATLAS_DATASET_CONCURRENCY)
            .try_flatten()
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for AtlasOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            self.datasets_object_store.clone(),
            file.object_meta,
            self.stream_cache.clone(),
            self.projected_schema.clone(),
            self.batch_size,
            metrics,
            self.read_dimensions.clone(),
            self.predicate.clone(),
            self.cache.clone(),
        );
        Ok(Box::pin(fut))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::AtlasFormat;
    use crate::datafusion::options::AtlasOptions;
    use crate::datafusion::test_support::{ensure_fixture, fixture_marker_object_meta, test_store};
    use arrow::array::Array;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;

    async fn build_opener_with_schema() -> (
        Arc<dyn FileOpener>,
        arrow::datatypes::SchemaRef,
        object_store::ObjectMeta,
    ) {
        ensure_fixture().await;
        let store = test_store().await;
        let format = AtlasFormat::new(store.clone(), AtlasOptions::default());
        let ctx = SessionContext::new();
        let object = fixture_marker_object_meta();

        let dummy_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());

        let table_schema = format
            .infer_schema(&ctx.state(), &dummy_store, &[object.clone()])
            .await
            .expect("infer schema");

        let source = AtlasSource::new(
            store,
            None,
            TableSchema::from_file_schema(table_schema.clone()),
        );
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();

        let opener = source
            .create_file_opener(dummy_store, &conf, 0)
            .expect("file opener");
        (opener, table_schema, object)
    }

    fn pf_for(object: object_store::ObjectMeta) -> PartitionedFile {
        PartitionedFile {
            object_meta: object,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
            ordering: None,
        }
    }

    fn collect_temperatures(
        batches: &[arrow::record_batch::RecordBatch],
        temp_idx: usize,
    ) -> Vec<f32> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch
                .column(temp_idx)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("temperature is Float32");
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    out.push(arr.value(i));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn opener_drains_queue_streaming_all_datasets() {
        // One open() call must drain the shared dataset queue and stream
        // batches from every dataset in the atlas (winter + summer).
        let (opener, schema, object) = build_opener_with_schema().await;
        let pf = pf_for(object.clone());

        let stream = opener.open(pf).expect("open").await.expect("stream");
        let batches: Vec<arrow::record_batch::RecordBatch> =
            stream.filter_map(|b| async move { b.ok() }).collect().await;
        assert!(!batches.is_empty(), "expected batches from drained queue");

        // Every batch must honor the union table schema width.
        for batch in &batches {
            assert_eq!(batch.schema().fields().len(), schema.fields().len());
        }

        // The merged stream must contain temperature values from BOTH
        // datasets: winter contributes [1, 2, 3, 4]; summer contributes
        // [20, 21, 22]. Order is unspecified (buffer_unordered).
        let temp_idx = schema.index_of("temperature").expect("temperature column");
        let mut temps = collect_temperatures(&batches, temp_idx);
        temps.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(temps, vec![1.0f32, 2.0, 3.0, 4.0, 20.0, 21.0, 22.0]);
    }

    #[tokio::test]
    async fn opener_honors_union_schema_with_nulls_for_missing_columns() {
        // Reading drains both datasets. Summer's batches must carry the
        // full union width with NULLs in `cycle` and `year`; winter's
        // batches must have real values there.
        let (opener, schema, object) = build_opener_with_schema().await;
        let pf = pf_for(object.clone());

        let stream = opener.open(pf).expect("open").await.expect("stream");
        let batches: Vec<arrow::record_batch::RecordBatch> =
            stream.filter_map(|b| async move { b.ok() }).collect().await;
        assert!(!batches.is_empty());

        let cycle_idx = schema.index_of("cycle").expect("cycle column");
        let year_idx = schema.index_of("year").expect("year column");

        let total_cycle_nulls: usize = batches
            .iter()
            .map(|b| b.column(cycle_idx).null_count())
            .sum();
        let total_year_nulls: usize = batches
            .iter()
            .map(|b| b.column(year_idx).null_count())
            .sum();
        // Summer has 3 rows and no cycle/year, so at least 3 null entries
        // must appear in each of those columns across the merged stream.
        assert!(
            total_cycle_nulls >= 3,
            "expected at least summer's 3 null cycle entries, got {total_cycle_nulls}",
        );
        assert!(
            total_year_nulls >= 3,
            "expected at least summer's 3 null year entries, got {total_year_nulls}",
        );
    }

    #[tokio::test]
    async fn opener_projection_skips_datasets_missing_the_column() {
        // Project only to `cycle` — winter has it, summer doesn't.
        // Summer's per-dataset stream short-circuits to empty (empty
        // projection); winter's batches still flow through.
        ensure_fixture().await;
        let store = test_store().await;
        let format = AtlasFormat::new(store.clone(), AtlasOptions::default());
        let ctx = SessionContext::new();
        let object = fixture_marker_object_meta();

        let dummy_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());

        let table_schema = format
            .infer_schema(&ctx.state(), &dummy_store, &[object.clone()])
            .await
            .expect("infer schema");
        let cycle_idx = table_schema.index_of("cycle").expect("cycle column");

        let source = AtlasSource::new(
            store,
            None,
            TableSchema::from_file_schema(table_schema.clone()),
        );
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_projection_indices(Some(vec![cycle_idx]))
        .unwrap()
        .build();

        let opener = source
            .create_file_opener(dummy_store, &conf, 0)
            .expect("file opener");
        let pf = pf_for(object.clone());

        let stream = opener.open(pf).expect("open").await.expect("stream");
        let batches: Vec<arrow::record_batch::RecordBatch> =
            stream.filter_map(|b| async move { b.ok() }).collect().await;

        // Winter contributes the only batches — summer's empty projection
        // produces an empty stream that flat-flattens away.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "expected winter's cycle rows");
        // Projected schema width is 1 (just `cycle`).
        for batch in &batches {
            assert_eq!(batch.schema().fields().len(), 1);
        }
    }

    #[tokio::test]
    async fn opener_opens_each_atlas_marker_once_per_source() {
        // The opener's stream_cache holds one AtlasStreamDatasets per
        // atlas marker path. After draining via open(), a second open()
        // on the same atlas finds an empty queue and yields no batches.
        let (opener, _schema, object) = build_opener_with_schema().await;

        // First open drains the queue.
        let stream = opener
            .open(pf_for(object.clone()))
            .expect("open")
            .await
            .expect("stream");
        let first_batches: Vec<_> = stream.collect().await;
        assert!(!first_batches.is_empty(), "first open must produce batches");

        // Second open sees an empty queue (same stream_cache entry) and
        // therefore produces no batches.
        let stream = opener
            .open(pf_for(object))
            .expect("open")
            .await
            .expect("stream");
        let second_batches: Vec<_> = stream.collect().await;
        assert!(
            second_batches.is_empty(),
            "second open must observe a drained queue, got {} batches",
            second_batches.len(),
        );
    }

    // ── Schema adaptation (BatchAdapterFactory) ────────────────────────
    //
    // These tests lock the contract our openers rely on after the
    // DataFusion 52/53 `SchemaAdapter` removal: every opener reads only the
    // file columns the query needs, then adapts each batch onto the
    // projected output schema with `BatchAdapterFactory`. They reproduce
    // that exact wiring against hand-built schemas (no fixtures), so the
    // behavior is verified independently of any one format reader.

    /// Build the read projection the way every opener does: the file columns
    /// present in the projected output schema, in file order.
    fn needed_file_columns(
        file_schema: &arrow::datatypes::Schema,
        projected_schema: &arrow::datatypes::Schema,
    ) -> Vec<usize> {
        file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
            .map(|(i, _)| i)
            .collect()
    }

    #[test]
    fn batch_adapter_reorders_casts_and_null_fills() {
        use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr_adapter::BatchAdapterFactory;

        // What the query wants out.
        let projected: arrow::datatypes::SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]));
        // This file provides b then a (reordered), a is Int32 (needs a cast),
        // and it has no `c` column at all (must be null-filled).
        let file = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
        ]);

        let projection = needed_file_columns(&file, &projected);
        assert_eq!(projection, vec![0, 1]);

        let source_schema: arrow::datatypes::SchemaRef =
            Arc::new(file.project(&projection).unwrap());
        let adapter = BatchAdapterFactory::new(projected.clone())
            .make_adapter(&source_schema)
            .unwrap();

        // A batch read with `projection`: columns [b, a] in projection order.
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();

        let out = adapter.adapt_batch(&batch).unwrap();
        assert_eq!(out.schema(), projected);
        let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.values(), &[1, 2], "Int32 column cast to Int64");
        let b = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b.value(0), "x");
        assert_eq!(out.column(2).null_count(), 2, "missing column null-filled");
    }

    #[test]
    fn batch_adapter_empty_projection_preserves_row_count() {
        use arrow::array::RecordBatchOptions;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr_adapter::BatchAdapterFactory;

        // `SELECT count(*)` projects to zero output columns.
        let projected: arrow::datatypes::SchemaRef = Arc::new(Schema::new(Vec::<Field>::new()));
        let file = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let projection = needed_file_columns(&file, &projected);
        assert!(projection.is_empty());

        let source_schema: arrow::datatypes::SchemaRef =
            Arc::new(file.project(&projection).unwrap());
        let adapter = BatchAdapterFactory::new(projected)
            .make_adapter(&source_schema)
            .unwrap();

        // A zero-column batch carrying a row count, as a reader would emit it.
        let batch = RecordBatch::try_new_with_options(
            source_schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();

        let out = adapter.adapt_batch(&batch).unwrap();
        assert_eq!(out.num_columns(), 0);
        assert_eq!(
            out.num_rows(),
            3,
            "row count preserved through empty projection"
        );
    }
}
