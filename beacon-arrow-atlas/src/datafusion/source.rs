use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
    },
    projection::DatasetProjection,
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
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};

use crate::datafusion::reader;

/// Per-file payload attached to each `PartitionedFile.extensions`.
///
/// The same `atlas.json` ObjectMeta is reused across all
/// `PartitionedFile`s belonging to one atlas store; the dataset name
/// stored here selects which atlas dataset the opener should read.
#[derive(Debug, Clone)]
pub struct AtlasFileInfo {
    pub dataset_name: String,
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
}

impl AtlasSource {
    pub fn new(datasets_object_store: Arc<DatasetsStore>) -> Self {
        Self {
            datasets_object_store,
            schema_adapter_factory: None,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            batch_size: usize::MAX,
            predicate: None,
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

        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema.clone());

        Arc::new(AtlasOpener {
            datasets_object_store: self.datasets_object_store.clone(),
            schema_adapter: Arc::from(schema_adapter),
            batch_size: self.batch_size,
            metrics: self.execution_plan_metrics.clone(),
            partition,
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

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> datafusion::error::Result<Option<datafusion::datasource::physical_plan::FileScanConfig>>
    {
        Ok(None)
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

// ─── FileOpener ────────────────────────────────────────────────────────────

struct AtlasOpener {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
}

impl AtlasOpener {
    async fn read_task(
        object_path: object_store::path::Path,
        dataset_name: String,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        batch_size: usize,
        metrics: Option<DatasetReadMetrics>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        let atlas = reader::open_atlas_store(datasets_object_store, &object_path).await?;

        let dataset = crate::reader::dataset_from_atlas(atlas, &dataset_name)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open atlas dataset '{dataset_name}' at {object_path}: {e}"
                ))
            })?;

        let file_schema: SchemaRef =
            beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Arrow schema from atlas dataset '{dataset_name}': {e}"
                    ))
                })?
                .into();

        let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;

        if projection.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let dataset = if projection.len() < file_schema.fields().len() {
            let proj = DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            };
            dataset.project(&proj).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to project atlas dataset '{dataset_name}': {e}"
                ))
            })?
        } else {
            dataset
        };

        // Atlas has no filter pushdown today.
        let pushdown_filter: Option<PushdownFilter> = None;
        let stream =
            any_dataset_as_record_batch_stream(dataset, batch_size, pushdown_filter, metrics)
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
                    futures::future::ready(mapped)
                })
                .boxed();

        Ok(stream)
    }
}

impl FileOpener for AtlasOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: datafusion::datasource::listing::PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let dataset_name = file
            .extensions
            .as_ref()
            .and_then(|ext| ext.downcast_ref::<AtlasFileInfo>())
            .map(|info| info.dataset_name.clone())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Atlas PartitionedFile is missing AtlasFileInfo extensions — dataset name unavailable"
                        .to_string(),
                )
            })?;

        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));
        let fut = Self::read_task(
            file_meta.object_meta.location,
            dataset_name,
            self.datasets_object_store.clone(),
            self.schema_adapter.clone(),
            self.batch_size,
            metrics,
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
    use datafusion::datasource::physical_plan::{
        FileMeta, FileScanConfigBuilder, FileSource,
    };
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;

    async fn build_opener_with_schema()
    -> (Arc<dyn FileOpener>, arrow::datatypes::SchemaRef, object_store::ObjectMeta) {
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

        let source = AtlasSource::new(store);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            table_schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();

        let opener = source.create_file_opener(dummy_store, &conf, 0);
        (opener, table_schema, object)
    }

    fn pf_for(dataset_name: &str, object: object_store::ObjectMeta) -> PartitionedFile {
        PartitionedFile {
            object_meta: object,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: Some(Arc::new(AtlasFileInfo {
                dataset_name: dataset_name.to_string(),
            })),
            metadata_size_hint: None,
        }
    }

    #[tokio::test]
    async fn opener_streams_batches_for_winter() {
        let (opener, _schema, object) = build_opener_with_schema().await;
        let pf = pf_for("winter", object.clone());

        let file_meta = FileMeta {
            object_meta: object,
            range: None,
            extensions: pf.extensions.clone(),
            metadata_size_hint: None,
        };

        let stream = opener.open(file_meta, pf).expect("open").await.expect("stream");
        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "expected at least one batch");

        let first = batches[0].as_ref().expect("batch ok");
        assert!(first.num_rows() > 0);
        assert!(first.num_columns() > 0);
    }

    #[tokio::test]
    async fn opener_missing_extensions_errors() {
        let (opener, _schema, object) = build_opener_with_schema().await;

        // PartitionedFile without AtlasFileInfo extensions.
        let pf = PartitionedFile {
            object_meta: object.clone(),
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let file_meta = FileMeta {
            object_meta: object,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let result = opener.open(file_meta, pf);
        assert!(result.is_err(), "missing AtlasFileInfo should produce error");
    }

    #[tokio::test]
    async fn opener_winter_has_temperature_column() {
        let (opener, schema, object) = build_opener_with_schema().await;
        let pf = pf_for("winter", object.clone());
        let file_meta = FileMeta {
            object_meta: object,
            range: None,
            extensions: pf.extensions.clone(),
            metadata_size_hint: None,
        };

        let stream = opener.open(file_meta, pf).expect("open").await.expect("stream");
        let batches: Vec<arrow::record_batch::RecordBatch> = stream
            .filter_map(|b| async move { b.ok() })
            .collect()
            .await;
        assert!(!batches.is_empty());

        // Verify the union schema is honored: even though winter has all
        // four columns, the projected output must match table_schema.
        for batch in &batches {
            assert_eq!(batch.schema().fields().len(), schema.fields().len());
        }

        // Concatenate temperature column values across batches.
        let temp_idx = schema.index_of("temperature").expect("temperature column");
        let mut all = Vec::new();
        for batch in &batches {
            let col = batch.column(temp_idx);
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("Float32Array");
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    all.push(arr.value(i));
                }
            }
        }
        assert_eq!(all, vec![1.0f32, 2.0, 3.0, 4.0]);
    }
}

