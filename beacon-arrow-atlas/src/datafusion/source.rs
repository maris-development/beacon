use std::collections::HashMap;
use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::arrow::{
    batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
    pushdown_filter::PushdownFilter,
};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_expr::{PhysicalExpr, conjunction},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::{ExecutionPlanMetricsSet, SplitMetrics},
        stream::{BatchSplitStream, RecordBatchStreamAdapter},
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
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,

    stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
}

impl AtlasSource {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        read_dimensions: Option<Vec<String>>,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter_factory: None,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions,
            stream_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        let table_schema = self
            .override_schema
            .clone()
            .unwrap_or_else(|| base_config.file_schema.clone());
        let projected_schema = base_config.projected_schema();
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let schema_adapter =
            schema_adapter_factory.create(projected_schema.clone(), table_schema.clone());

        Arc::new(AtlasOpener {
            datasets_object_store: self.datasets_object_store.clone(),
            schema_adapter: Arc::from(schema_adapter),
            batch_size: self.batch_size,
            metrics: self.execution_plan_metrics.clone(),
            projected_schema: projected_schema.clone(),
            partition,
            read_dimensions: self.read_dimensions.clone(),
            predicate: self.predicate.clone(),
            stream_cache: self.stream_cache.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(
        &self,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        if let Some(statistics) = &self.projected_statistics {
            Ok(statistics.clone())
        } else if let Some(schema) = self.override_schema.as_ref() {
            Ok(Statistics::new_unknown(schema))
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "atlas"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
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

struct AtlasOpener {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    read_dimensions: Option<Vec<String>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projected_schema: SchemaRef,

    stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
}

impl AtlasOpener {
    async fn read_task(
        datasets_object_store: Arc<DatasetsStore>,
        object_meta: ObjectMeta,
        stream_cache: Arc<Mutex<HashMap<String, AtlasStreamDatasets>>>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        batch_size: usize,
        metrics: Option<DatasetReadMetrics>,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        partition: usize,
        execution_plan_metrics: ExecutionPlanMetricsSet,
        projected_schema: SchemaRef,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let atlas =
            crate::datafusion::cache::get_or_open_atlas(datasets_object_store, &object_meta)
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
                let schema_adapter = schema_adapter.clone();
                let read_dimensions = read_dimensions.clone();
                let metrics = metrics.clone();
                let object_path = object_path.clone();
                {
                    let predicate_cloned = predicate.clone();
                    async move {
                        let view = atlas.open_dataset(&dataset_name).await.map_err(|e| {
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

                        let (schema_mapper, projection) =
                            schema_adapter.map_schema(&file_schema)?;

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
                            let mapped = schema_mapper.map_batch(batch).map_err(|e| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "Failed to map atlas batch schema: {e}"
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

        let split_metrics = SplitMetrics::new(&execution_plan_metrics, partition);

        let stream_adapter = RecordBatchStreamAdapter::new(projected_schema, stream);

        let split_stream =
            BatchSplitStream::new(Box::pin(stream_adapter), 64 * 1024, split_metrics);

        Ok(Box::pin(split_stream))
    }
}

impl FileOpener for AtlasOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: datafusion::datasource::listing::PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            self.datasets_object_store.clone(),
            file_meta.object_meta,
            self.stream_cache.clone(),
            self.schema_adapter.clone(),
            self.batch_size,
            metrics,
            self.read_dimensions.clone(),
            self.predicate.clone(),
            self.partition,
            self.metrics.clone(),
            self.projected_schema.clone(),
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
    use datafusion::datasource::physical_plan::{FileMeta, FileScanConfigBuilder, FileSource};
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

        let source = AtlasSource::new(store, None);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            table_schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();

        let opener = source.create_file_opener(dummy_store, &conf, 0);
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
        }
    }

    fn file_meta_for(object: object_store::ObjectMeta) -> FileMeta {
        FileMeta {
            object_meta: object,
            range: None,
            extensions: None,
            metadata_size_hint: None,
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
        let file_meta = file_meta_for(object);

        let stream = opener
            .open(file_meta, pf)
            .expect("open")
            .await
            .expect("stream");
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
        let file_meta = file_meta_for(object);

        let stream = opener
            .open(file_meta, pf)
            .expect("open")
            .await
            .expect("stream");
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

        let source = AtlasSource::new(store, None);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            table_schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_projection(Some(vec![cycle_idx]))
        .build();

        let opener = source.create_file_opener(dummy_store, &conf, 0);
        let pf = pf_for(object.clone());
        let file_meta = file_meta_for(object);

        let stream = opener
            .open(file_meta, pf)
            .expect("open")
            .await
            .expect("stream");
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
            .open(file_meta_for(object.clone()), pf_for(object.clone()))
            .expect("open")
            .await
            .expect("stream");
        let first_batches: Vec<_> = stream.collect().await;
        assert!(!first_batches.is_empty(), "first open must produce batches");

        // Second open sees an empty queue (same stream_cache entry) and
        // therefore produces no batches.
        let stream = opener
            .open(file_meta_for(object.clone()), pf_for(object))
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
}
