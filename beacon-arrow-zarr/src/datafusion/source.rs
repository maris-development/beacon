//! DataFusion [`FileSource`]/[`FileOpener`] for zarr groups.
//!
//! Each opened file is one leaf zarr group's `zarr.json`. The opener builds an
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) for the (projected)
//! columns and streams it through the shared engine, which handles predicate
//! pushdown (chunk pruning + row masking) via [`PushdownFilter`].

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream, metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter, schema::any_dataset_to_arrow_schema,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    error::DataFusionError,
    physical_expr::conjunction,
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt, future};
use object_store::ObjectStore;
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{reader::dataset_from_group, util::ZarrPath};

/// DataFusion [`FileSource`] for zarr groups.
#[derive(Clone)]
pub struct ZarrSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl Default for ZarrSource {
    fn default() -> Self {
        Self {
            schema_adapter_factory: None,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            batch_size: usize::MAX,
            predicate: None,
        }
    }
}

impl ZarrSource {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FileSource for ZarrSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
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
        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);

        Arc::new(ZarrOpener {
            object_store,
            schema_adapter: Arc::from(schema_adapter),
            predicate: self.predicate.clone(),
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

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
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
            Err(DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "zarr"
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

        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::No;
                filters.len()
            ])
            .with_updated_node(Arc::new(source)),
        )
    }
}

// ─── FileOpener ──────────────────────────────────────────────────────────────

struct ZarrOpener {
    object_store: Arc<dyn ObjectStore>,
    schema_adapter: Arc<dyn SchemaAdapter>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
}

impl FileOpener for ZarrOpener {
    fn open(
        &self,
        _file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create ZarrPath from object metadata: {e}"))
        })?;

        let object_store = self.object_store.clone();
        let schema_adapter = self.schema_adapter.clone();
        let predicate = self.predicate.clone();
        let batch_size = self.batch_size;
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));

        let fut = async move {
            let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
                as Arc<dyn AsyncReadableListableStorageTraits>;
            let group = Group::async_open(zarr_store, &zarr_path.as_zarr_path())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open Zarr group at '{}': {e}",
                        zarr_path.as_zarr_path()
                    ))
                })?;

            // Derive the file schema from the full dataset, then ask the
            // schema adapter which columns the query needs.
            let full = dataset_from_group(&group, None).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to read Zarr group as dataset: {e}"))
            })?;
            let file_schema: SchemaRef = Arc::new(any_dataset_to_arrow_schema(&full).map_err(
                |e| DataFusionError::Execution(format!("Failed to derive Zarr Arrow schema: {e}")),
            )?);

            let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;
            if projection.is_empty() {
                return Ok(futures::stream::empty().boxed());
            }

            let projected = full
                .project(&DatasetProjection::new_with_index_projection(projection))
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to project Zarr dataset: {e}"))
                })?;

            let pushdown_filter = predicate.map(PushdownFilter::new);
            let stream = any_dataset_as_record_batch_stream(
                projected,
                batch_size,
                pushdown_filter,
                metrics,
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Error reading Zarr dataset as Arrow: {e}"))
            })
            .and_then(move |batch| {
                let mapped = schema_mapper.map_batch(batch).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to map Zarr batch schema: {e}"))
                });
                future::ready(mapped)
            })
            .boxed();

            Ok(stream)
        };

        Ok(fut.boxed())
    }
}
