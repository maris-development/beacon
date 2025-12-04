use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use beacon_arrow_zarr::stream::ArrowZarrStream;
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
    opener::{
        partition::fetch_partitioned_stream,
        stream_share::{PartitionedZarrStreamShare, ZarrStreamShare},
    },
    statistics::ZarrStatisticsSelection,
    util::ZarrPath,
};

pub mod partition;
pub mod stream_share;

/// Open a Zarr object and produce a future that yields a stream proxy for its partitions.
///
/// This implements FileOpener for ZarrFileOpener and performs the following steps:
/// - Converts the provided FileMeta's ObjectMeta into a ZarrPath, returning a DataFusion
///   Execution error if parsing fails.
/// - Uses an internal, shared map of stream shares keyed by object path to obtain an
///   Arc<ZarrStreamShare>. The map access is protected by a lock; if a share for the
///   path does not exist it is created lazily. This ensures concurrent open requests
///   for the same path share initialization work and the resulting streams.
/// - Builds and returns a boxed asynchronous future. When polled, that future:
///     - Calls get_or_try_init on the ZarrStreamShare to initialize or reuse the
///       partitioned streams for the object. Initialization is performed by
///       fetch_partitioned_streams which may access the object store, apply pruning
///       using pushdown statistics/predicates, and consider array step configuration.
///     - Flattens the resulting partitioned streams into a single stream proxy suitable
///       for DataFusion consumption and returns it.
///
/// Parameters:
/// - file_meta: Metadata for the file to open; its ObjectMeta is used to construct
///   the ZarrPath and to key the stream-share cache.
/// - _file: A PartitionedFile value that is unused by this opener (present to match
///   the trait signature).
///
/// Return:
/// - On success: Ok(BoxFuture) that resolves to a stream proxy of the opened file's
///   partitions.
/// - On error: DataFusionError::Execution with a descriptive message (e.g., failed
///   ZarrPath creation or stream initialization).
///
/// Additional notes:
/// - The method clones necessary handles (object store, schema adapter, predicate,
///   array steps) so the returned future is self-contained and safe to run later.
/// - Debug tracing is emitted for path creation and file open operations to aid
///   diagnosis.
/// - IO and heavy work are deferred until the returned future is awaited/polled.
pub struct ZarrFileOpener {
    pub table_schema: SchemaRef,
    /// Underlying object store for accessing Zarr data.
    pub zarr_object_store: Arc<dyn ObjectStore>,
    /// Schema adapter for mapping Zarr schema to Arrow schema.
    pub schema_adapter: Arc<dyn SchemaAdapter>,
    /// Stream partition shares for the Zarr file.
    pub stream_partition_shares:
        Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
    /// Pruning Predicate
    pub pushdown_predicate_expr: Option<Arc<dyn PhysicalExpr>>,
    /// Statistics Selection for Pushdown
    pub pushdown_statistics: Option<Arc<ZarrStatisticsSelection>>,
}

impl FileOpener for ZarrFileOpener {
    fn open(
        &self,
        _file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_path = ZarrPath::new_from_object_meta(file.object_meta.clone()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from object metadata: {}",
                e
            ))
        })?;

        let zarr_object_store = AsyncObjectStore::new(self.zarr_object_store.clone());
        let table_schema = self.table_schema.clone();
        let adapter = self.schema_adapter.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let pushdown_statistics = self.pushdown_statistics.clone();
        let pushdown_predicate_expr = self.pushdown_predicate_expr.clone();

        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file.object_meta.location.clone();
            tracing::debug!(
                "Getting or creating ZarrStreamShare for path: {}",
                object_path
            );
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(ZarrStreamShare::new()))
                .clone()
        };

        let fut = async move {
            tracing::debug!("Opening Zarr file at path: {:?}", zarr_path);
            let partitioned_streams = stream_partition_share
                .get_or_try_init(|| async move {
                    let obj_store =
                        Arc::new(zarr_object_store) as Arc<dyn AsyncReadableListableStorageTraits>;
                    let group = Group::async_open(obj_store, &zarr_path.as_zarr_path())
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Failed to open Zarr group at path '{}': {}",
                                zarr_path.as_zarr_path(),
                                e
                            ))
                        })?;
                    let arc_group = Arc::new(group);
                    let partition = fetch_partitioned_stream(
                        arc_group,
                        table_schema,
                        adapter,
                        pushdown_predicate_expr,
                        pushdown_statistics,
                    )
                    .await?;

                    if let Some(partition) = partition {
                        Ok(Arc::from(vec![partition]))
                    } else {
                        Ok(Arc::from(vec![]))
                    }
                })
                .await?;

            let flattened_stream = flatten_partitioned_streams(partitioned_streams).await;
            Ok::<_, datafusion::error::DataFusionError>(flattened_stream)
        };

        Ok(fut.boxed())
    }
}

async fn flatten_partitioned_streams(
    partitioned_streams: Arc<[PartitionedZarrStreamShare]>,
) -> Pin<
    Box<
        dyn futures::Stream<Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>>
            + Send,
    >,
> {
    let mut all_streams = Vec::new();
    for partition in partitioned_streams.iter() {
        let stream = flatten_batch_stream(partition.stream_composer.pollable_shared_stream());
        all_streams.push(stream);
    }
    let combined_stream = futures::stream::iter(all_streams).flatten();
    combined_stream.boxed()
}

fn flatten_batch_stream(
    stream: ArrowZarrStream,
) -> Pin<
    Box<
        dyn futures::Stream<Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>>
            + Send,
    >,
> {
    (stream
        .map(|res_nd_batch| {
            let nd_batch = res_nd_batch.map_err(|e| {
                ArrowError::IoError(
                    "Error reading NdRecordBatch from Zarr stream".to_string(),
                    std::io::Error::other(e),
                )
            })?;

            let arrow_batch = nd_batch
                .to_arrow_record_batch()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            Ok(arrow_batch)
        })
        .boxed()) as _
}
