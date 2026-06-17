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
        batch::{any_dataset_as_record_batch_stream, default_chunk_concurrency},
        metrics::DatasetReadMetrics,
        pushdown_filter::PushdownFilter,
        schema::any_dataset_to_arrow_schema,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    error::DataFusionError,
    physical_expr::{conjunction, projection::ProjectionExprs},
    physical_expr_adapter::BatchAdapterFactory,
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
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl ZarrSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            projection: None,
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

impl FileSource for ZarrSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(ZarrOpener {
            object_store,
            projected_schema,
            predicate: self.predicate.clone(),
            batch_size: self.batch_size,
            metrics: self.execution_plan_metrics.clone(),
            partition,
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
    projected_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
}

impl FileOpener for ZarrOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create ZarrPath from object metadata: {e}"))
        })?;

        let object_store = self.object_store.clone();
        let projected_schema = self.projected_schema.clone();
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

            // Columns of this group that the query needs, in file order — used
            // both to prune the read and as the source schema for the adapter.
            let projection: Vec<usize> = file_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| projected_schema.index_of(f.name()).is_ok())
                .map(|(i, _)| i)
                .collect();
            if projection.is_empty() {
                return Ok(futures::stream::empty().boxed());
            }

            // Adapt batches (read with `projection`) onto the projected output
            // schema: reorder, cast, and null-fill columns the group lacks.
            let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

            let projected = full
                .project(&DatasetProjection::new_with_index_projection(projection))
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to project Zarr dataset: {e}"))
                })?;

            let pushdown_filter = predicate.map(PushdownFilter::new);
            let stream = any_dataset_as_record_batch_stream(
                projected,
                batch_size,
                default_chunk_concurrency(),
                pushdown_filter,
                metrics,
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Error reading Zarr dataset as Arrow: {e}"))
            })
            .and_then(move |batch| {
                let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to adapt Zarr batch schema: {e}"))
                });
                future::ready(mapped)
            })
            .boxed();

            Ok(stream)
        };

        Ok(fut.boxed())
    }
}
