//! DataFusion [`FileSource`]/[`FileOpener`] for zarr groups.
//!
//! Each opened file is one leaf zarr group's `zarr.json`. The opener builds an
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) for the (projected)
//! columns and streams it through the shared engine, which handles predicate
//! pushdown (chunk pruning + row masking) via [`PushdownFilter`].

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use beacon_nd_array::{
    arrow::{
        batch::any_dataset_as_record_batch_stream_split, metrics::DatasetReadMetrics,
        nd_provider::any_dataset_as_encoded_stream_split, pushdown_filter::PushdownFilter,
        schema::any_dataset_to_arrow_schema, split::ChunkSplit,
    },
    projection::DatasetProjection,
};
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{
            FileGroupPartitioner, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
        },
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

use crate::{
    reader::{dataset_from_group, project_read_dimensions},
    util::{ZarrPath, ZarrStorage},
};

/// The nominal size a zarr leaf group reports.
///
/// A leaf group is not one object. It is a node with a `zarr.json` and a tree of
/// chunk files under it, so no byte count describes it, which is why it used to
/// report zero. Zero has a cost: DataFusion divides a file by byte range, and it
/// declines to divide a range of zero, so a group could never be split.
///
/// The value carries no meaning of its own. It only has to leave room for one
/// range per partition. [`ZarrOpener`] reads its range as a fraction of the
/// chunk list and never as bytes, so the fractions come out exact whatever this
/// is. See [`beacon_nd_array::arrow::split`].
pub(crate) const NOMINAL_GROUP_SIZE: u64 = 1 << 20;

/// DataFusion [`FileSource`] for zarr groups.
#[derive(Clone)]
pub struct ZarrSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Explicit dimensions to read, or `None` to auto-select a default.
    read_dimensions: Option<Vec<String>>,
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// Storage to open groups over, replacing the session's object store.
    /// Set by the Icechunk reader; `None` for a listed zarr store.
    storage: Option<ZarrStorage>,
}

impl ZarrSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions: None,
            projection: None,
            storage: None,
        }
    }

    /// Returns a copy of this source that opens groups over `storage` instead of
    /// the session's object store.
    pub fn with_storage(mut self, storage: ZarrStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Returns a copy of this source that reads only the variables belonging to
    /// `read_dimensions` (or auto-selects a default when `None`).
    pub fn with_read_dimensions(mut self, read_dimensions: Option<Vec<String>>) -> Self {
        self.read_dimensions = read_dimensions;
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

impl FileSource for ZarrSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;

        Ok(Arc::new(ZarrOpener {
            storage: self
                .storage
                .clone()
                .unwrap_or_else(|| ZarrStorage::from_object_store(object_store)),
            projected_schema,
            predicate: self.predicate.clone(),
            batch_size: self.batch_size,
            read_dimensions: self.read_dimensions.clone(),
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

    /// Split each group across partitions, whatever its `zarr.json` weighs.
    ///
    /// The default implementation declines a file below
    /// `repartition_file_min_size` (10 MB), which is the right call for a format
    /// whose object *is* its data. A zarr group's object is its `zarr.json`: a
    /// metadata document of a few KB that can front terabytes of chunks. The
    /// minimum would measure the wrong thing and decline every store, so this
    /// ignores it.
    ///
    /// Over-splitting is cheap rather than wrong. The opener resolves its range
    /// against the chunk list, so a share of a group with fewer chunks than
    /// shares simply reads nothing.
    ///
    /// The byte ranges still come from `zarr.json` sizes, so a scan over several
    /// groups balances by metadata weight rather than by data volume. Within one
    /// group — the case this exists for — the shares are even.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        let repartitioned = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(0)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&config.file_groups);

        Ok(repartitioned.map(|file_groups| {
            let mut config = config.clone();
            config.file_groups = file_groups;
            config
        }))
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
    storage: ZarrStorage,
    projected_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    read_dimensions: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
}

impl FileOpener for ZarrOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create ZarrPath from object metadata: {e}"))
        })?;

        // This partition's share of the group. The range spans the group's
        // `zarr.json`, which is metadata, not data. It is never read as bytes:
        // it names a fraction of the chunk list the reader builds, and the
        // fractions of a group tile that list. See
        // [`beacon_nd_array::arrow::split`] and [`ZarrSource::repartitioned`].
        //
        // An unranged group gives `None`, which reads the whole dataset.
        let (range_start, range_end) = file.range();
        let split = ChunkSplit::from_byte_range(range_start..range_end, file.object_meta.size);

        let storage = self.storage.clone();
        let projected_schema = self.projected_schema.clone();
        let predicate = self.predicate.clone();
        let batch_size = self.batch_size;
        let read_dimensions = self.read_dimensions.clone();
        let metrics = Some(DatasetReadMetrics::new(&self.metrics, self.partition));

        let fut = async move {
            let group = Group::async_open(storage.inner(), &zarr_path.as_zarr_path())
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

            // Apply explicit dimensions, or narrow to a broadcast-compatible
            // default so `SELECT *` cannot fail when variables live on
            // incompatible dimension sets. No log label: this runs per
            // file/partition (logging happens in schema inference).
            let full = project_read_dimensions(full, read_dimensions, None)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

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
                // COUNT(*): reading zero columns yields an empty stream (count 0).
                // Drive with the highest-dimensionality variable so the row count
                // is the full broadcast count (a scalar attribute gives 1 row),
                // plus any predicate columns (PushdownFilter matches by name), and
                // emit zero-column batches carrying the row counts.
                let driver_idx = full
                    .fields()
                    .keys()
                    .max_by_key(|name| {
                        full.get_array(name)
                            .map(|a| a.shape().iter().product::<usize>())
                            .unwrap_or(0)
                    })
                    .and_then(|name| file_schema.index_of(name).ok())
                    .unwrap_or(0);
                let mut driver: Vec<usize> = vec![driver_idx];
                if let Some(pred) = &predicate {
                    for col in datafusion::physical_expr::utils::collect_columns(pred) {
                        if let Ok(idx) = file_schema.index_of(col.name()) {
                            driver.push(idx);
                        }
                    }
                }
                driver.sort_unstable();
                driver.dedup();

                let projected = full
                    .project(&DatasetProjection::new_with_index_projection(driver))
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to project Zarr dataset for count: {e}"
                        ))
                    })?;
                let pushdown_filter = predicate.map(PushdownFilter::new);
                let count_schema = projected_schema.clone();
                // The split applies here too. A count path that read the whole
                // group in every partition would return the row count once per
                // partition.
                let stream = any_dataset_as_record_batch_stream_split(
                    projected,
                    batch_size,
                    pushdown_filter,
                    metrics,
                    split,
                )
                .map(move |batch| {
                    let batch = batch.map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Error reading Zarr dataset as Arrow: {e}"
                        ))
                    })?;
                    RecordBatch::try_new_with_options(
                        count_schema.clone(),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to build count batch: {e}"))
                    })
                })
                .boxed();
                return Ok(stream);
            }

            // The opener emits nd-encoded batches, so adaptation happens in the
            // encoded (struct) domain: reorder and null-fill columns the group
            // lacks onto the projected encoded schema.
            let source_schema: SchemaRef = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
                &file_schema.project(&projection)?,
            ));
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;

            let projected = full
                .project(&DatasetProjection::new_with_index_projection(projection))
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to project Zarr dataset: {e}"))
                })?;

            // Emit nd-encoded batches (decoded/broadcast by the NdSourceExec /
            // NdBroadcastExec above the scan), adapted onto the projected
            // encoded schema.
            let _ = metrics;
            let stream = any_dataset_as_encoded_stream_split(projected, batch_size, split)
                .and_then(move |batch| {
                    let mapped = adapter.adapt_batch(&batch).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to adapt Zarr batch schema: {e}"
                        ))
                    });
                    future::ready(mapped)
                })
                .boxed();

            Ok(stream)
        };

        Ok(fut.boxed())
    }
}
