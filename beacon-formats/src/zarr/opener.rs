use std::{collections::HashMap, sync::Arc};

use arrow::{array::RecordBatch, error::ArrowError};
use beacon_arrow_zarr::{
    array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader,
};
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
        schema_adapter::SchemaAdapter,
    },
    physical_plan::PhysicalExpr,
};
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use parking_lot::Mutex;
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{
    array_step_span::NumericArrayStepSpan,
    expr_util::extract_range_from_physical_filters,
    pushdown_statistics::ZarrPushDownStatistics,
    stream_share::{PartitionedZarrStreamShare, ZarrStreamShare},
    util::{ZarrPath, path_parent},
};

pub struct ZarrFileOpener {
    pub zarr_object_store: Arc<AsyncObjectStore<Arc<dyn ObjectStore>>>,
    /// Schema adapter for mapping NetCDF schema to Arrow schema.
    pub schema_adapter: Arc<dyn SchemaAdapter>,
    /// Stream partition shares for the Zarr file.
    pub stream_partition_shares:
        Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
    /// Array Steps for slicing arrays based on step spans. This is utilized by the pruning predicate pushdown.
    pub array_steps: Arc<HashMap<String, NumericArrayStepSpan>>,
    /// Pushdown Zarr Statistics
    pub pushdown_zarr_statistics: ZarrPushDownStatistics,
    /// Pruning Predicate
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
}

async fn fetch_partitioned_streams(
    zarr_path: ZarrPath,
) -> datafusion::error::Result<Arc<[PartitionedZarrStreamShare]>> {
    todo!()
}

impl FileOpener for ZarrFileOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        // Parse file meta as ZarrFile

        let zarr_path = ZarrPath::new_from_object_meta(file_meta.object_meta).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from ObjectMeta: {}",
                e
            ))
        })?;

        let zarr_object_store = self.zarr_object_store.clone();
        let adapter = self.schema_adapter.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file_meta.object_meta.location.clone();
            tracing::debug!(
                "Getting or creating ZarrStreamShare for path: {}",
                object_path
            );
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(ZarrStreamShare::new()))
                .clone()
        };
        let pruning_predicate = self.predicate.clone();
        let array_steps = self.array_steps.clone();
        let pushdown_zarr_statistics = self.pushdown_zarr_statistics.clone();

        // Check if the pruning predicate references any arrays in the pushdown statistics
        let fut = async move {
            let (stream, schema_mapper, file_schema) = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file_meta.object_meta.location);

                    let parent_path =
                        path_parent(&file_meta.object_meta.location).ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Could not determine parent path of object: {}",
                                file_meta.object_meta.location
                            ))
                        })?;

                    // Open the zarr group
                    let group = Group::async_open(
                        zarr_object_store.clone() as Arc<dyn AsyncReadableListableStorageTraits>,
                        &format!("/{}", parent_path),
                    )
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                    let reader = AsyncArrowZarrGroupReader::new(Arc::new(group))
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                    let file_schema = reader.arrow_schema();

                    let (schema_mapper, projection) = adapter
                        .map_schema(&file_schema)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    // Generate ArraySlicePushDown based on the predicate and array steps
                    let array_slice_pushdowns = if let Some(pruning_predicate) = &pruning_predicate
                    {
                        array_steps
                            .values()
                            .filter_map(|span| {
                                generate_numeric_span_slice_pushdown(pruning_predicate, span)
                            })
                            .map(|pd| (pd.dimension().to_string(), pd))
                            .collect()
                    } else {
                        HashMap::new()
                    };

                    tracing::debug!("Array Slice Pushdowns: {:?}", array_slice_pushdowns);

                    let stream_producer = reader
                        .into_parallel_stream_composer(
                            Some(projection),
                            Some(array_slice_pushdowns),
                        )
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                    Ok::<_, datafusion::error::DataFusionError>((
                        stream_producer,
                        schema_mapper,
                        file_schema,
                    ))
                })
                .await?
                .clone();
            let producer = stream.pollable_shared_stream();

            let stream_proxy = producer
                .map(
                    move |nd_batch| -> Result<arrow::array::RecordBatch, arrow::error::ArrowError> {
                        let schema_mapper = schema_mapper.clone();
                        let arrow_batch = nd_batch
                            .map_err(|e| {
                                tracing::error!(
                                    "Error reading NdRecordBatch from Zarr stream: {:?}",
                                    e
                                );
                                ArrowError::IoError(
                                    "Error reading NdRecordBatch from Zarr stream".to_string(),
                                    std::io::Error::other(e),
                                )
                            })?
                            .to_arrow_record_batch()
                            .unwrap_or_else(|e| {
                                tracing::error!(
                                    "Error converting NdRecordBatch to Arrow RecordBatch: {:?}",
                                    e
                                );
                                RecordBatch::new_empty(file_schema.clone())
                            });
                        let mapped_batch = schema_mapper
                            .map_batch(arrow_batch)
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                        Ok(mapped_batch)
                    },
                )
                .boxed();
            Ok(stream_proxy)
        };

        Ok(fut.boxed())
    }
}

fn generate_numeric_span_slice_pushdown(
    pruning_predicate: &Arc<dyn PhysicalExpr>,
    span: &NumericArrayStepSpan,
) -> Option<ArraySlicePushDown> {
    let r =
        extract_range_from_physical_filters(std::slice::from_ref(pruning_predicate), &span.column);
    let mut min_step = None;
    let mut max_step = None;
    if let Some(min) = r.as_ref().and_then(|r| r.as_f64_min()) {
        // Calculate start index based on step
        let start_index = lower_index(span.start, span.step, min);
        min_step = Some(if start_index < 0 { 0 } else { start_index });
    }
    if let Some(max) = r.as_ref().and_then(|r| r.as_f64_max()) {
        // Calculate end index based on step
        let end_index = upper_index(span.start, span.step, max);
        max_step = Some(if end_index < 0 { 0 } else { end_index });
    }

    // If min_step > max_step, then swap them
    if let (Some(min), Some(max)) = (min_step, max_step)
        && min > max
    {
        min_step = Some(max);
        max_step = Some(min);
    }

    Some(ArraySlicePushDown::new(
        span.dimension.clone(),
        min_step.map(|v| v as usize),
        max_step.map(|v| v as usize),
    ))
}

fn lower_index(start: f64, step: f64, value: f64) -> i64 {
    let raw = (value - start) / step;
    if step >= 0.0 {
        raw.floor() as i64 // ascending: lower = floor
    } else {
        raw.ceil() as i64 // descending: lower = ceil
    }
}

fn upper_index(start: f64, step: f64, value: f64) -> i64 {
    let raw = (value - start) / step;
    if step >= 0.0 {
        raw.ceil() as i64 // ascending: upper = ceil
    } else {
        raw.floor() as i64 // descending: upper = floor
    }
}
