//! A shared reader over one collection: the datasets left to read, and the
//! view every partition reads them through.
//!
//! The pool for a collection is built once, on the first open, and every
//! later open of the same collection gets a handle to it. A handle is a
//! stream. It pops the next dataset off the shared queue, reads it one stored
//! chunk at a time, and yields each chunk as one nd-encoded batch. Two handles
//! that poll at once drain different datasets, so a dataset is read by one
//! partition and by no other.

use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::nd::encode_nd_record_batch;
use crossbeam::queue::ArrayQueue;
use datafusion::physical_plan::PhysicalExpr;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use parking_lot::RwLock;
use tokio::sync::OnceCell;

use crate::datafusion::metrics::AtlasScanMetrics;
use crate::datafusion::view::{AtlasView, under_fields};
use crate::store::AtlasReaderCache;

/// One cell per collection. The cell fills on the first open and never again.
type Pools = HashMap<Path, Arc<OnceCell<Arc<Level1Pool>>>>;

/// The pools of one scan, one per collection, keyed by the container's path.
#[derive(Default, Clone)]
pub struct AtlasReaderPool {
    pools: Arc<RwLock<Pools>>,
}

/// What one open of a collection needs, beyond the collection itself.
///
/// `logical_schema` is what the columns and the predicate are written
/// against. `projected_schema` is the same schema nd-encoded, and every batch
/// goes out under it.
pub(crate) struct PoolOpen<'a> {
    /// The reader cache to open through, or `None` to open afresh.
    pub cache: Option<&'a AtlasReaderCache>,
    pub logical_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    /// The predicate to prune datasets with, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The partition's metrics. The datasets this consumer reads count here.
    pub scan_metrics: AtlasScanMetrics,
}

impl Debug for AtlasReaderPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasReaderPool").finish()
    }
}

impl AtlasReaderPool {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// One consumer of the collection at `object_meta`.
    ///
    /// The first call for a collection opens it, prunes its datasets, and
    /// queues the survivors. Every call gets a stream over that queue. The
    /// open and the prune are timed on the first caller's metrics, and each
    /// consumer counts the datasets it reads on its own.
    pub(crate) async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        open: PoolOpen<'_>,
    ) -> anyhow::Result<Level1PoolStream> {
        let cell = Arc::clone(
            self.pools
                .write()
                .entry(object_meta.location.clone())
                .or_default(),
        );
        let pool = cell
            .get_or_try_init(|| async {
                let open_timer = open.scan_metrics.open_time.timer();
                let atlas_view =
                    AtlasView::new(open.cache, store, object_meta, open.logical_schema).await?;
                drop(open_timer);
                let datasets = atlas_view
                    .list_datasets(open.predicate, open.scan_metrics.clone())
                    .await?;
                // A queue has at least one slot: `ArrayQueue::new(0)` panics.
                // With no dataset to read the slot stays empty, the first
                // `pop` finds nothing, and the stream ends at once.
                let queue = ArrayQueue::new(datasets.len().max(1));
                for dataset in datasets {
                    queue
                        .push(dataset)
                        .map_err(|dataset| anyhow::anyhow!("no slot for dataset '{dataset}'"))?;
                }

                Ok::<Arc<Level1Pool>, anyhow::Error>(Arc::new(Level1Pool {
                    inner: Arc::new(InnerLevel1Pool {
                        atlas_view,
                        queue,
                        projected_schema: open.projected_schema,
                    }),
                }))
            })
            .await
            .cloned()?;

        Ok(pool.as_ref().clone().into_stream(open.scan_metrics))
    }
}

/// A handle on a collection's shared queue.
///
/// The handle holds no read state, so it is shared between threads freely.
/// [`Level1Pool::into_stream`] makes one consumer of the queue. The
/// partitions that share a collection each hold a consumer, and the queue
/// shares the work between them.
#[derive(Clone)]
pub(crate) struct Level1Pool {
    inner: Arc<InnerLevel1Pool>,
}

impl Level1Pool {
    /// One consumer of the queue. It starts on no dataset, and counts the
    /// datasets it reads on `scan_metrics`.
    pub(crate) fn into_stream(self, scan_metrics: AtlasScanMetrics) -> Level1PoolStream {
        Level1PoolStream {
            inner: self.inner,
            scan_metrics,
            current: None,
        }
    }
}

struct InnerLevel1Pool {
    atlas_view: AtlasView,
    /// The datasets left to read, in listing order.
    queue: ArrayQueue<String>,
    /// The scan's output schema, nd-encoded. Every batch goes out under it.
    projected_schema: SchemaRef,
}

/// One consumer of a collection's shared queue.
///
/// It streams the datasets it pops, one nd-encoded batch per stored chunk.
/// The dataset in hand is a boxed stream, which is `Send` and not `Sync`, so
/// the consumer lives with the partition that polls it and never in the
/// shared handle.
pub(crate) struct Level1PoolStream {
    inner: Arc<InnerLevel1Pool>,
    /// The partition's metrics. The datasets this consumer reads count here.
    scan_metrics: AtlasScanMetrics,
    /// The dataset this consumer is draining, if any.
    current: Option<BoxStream<'static, anyhow::Result<RecordBatch>>>,
}

impl Stream for Level1PoolStream {
    type Item = anyhow::Result<RecordBatch>; // Record Batches using nd encoding

    /// The next batch of the dataset in hand, or the first batch of the next
    /// dataset on the queue. The stream ends when the queue is empty and the
    /// dataset in hand is drained.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(current) = this.current.as_mut() {
                match current.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                    Poll::Ready(None) => this.current = None,
                    Poll::Pending => return Poll::Pending,
                }
            }
            match this.inner.queue.pop() {
                Some(dataset) => {
                    this.current = Some(dataset_stream(
                        Arc::clone(&this.inner),
                        this.scan_metrics.clone(),
                        dataset,
                    ));
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

/// One dataset's batches: one encoded nd batch per stored chunk, in C order.
///
/// The dataset is built when the stream is first polled, and each chunk is
/// read when the stream reaches it. A chunk goes out under the table's
/// fields, cast to the types the table declares, and under the encoded table
/// schema, so every batch of the pool has the one schema the scan expects.
fn dataset_stream(
    pool: Arc<InnerLevel1Pool>,
    scan_metrics: AtlasScanMetrics,
    dataset: String,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    futures::stream::once(async move {
        let source = pool.atlas_view.dataset(&dataset).await?;
        scan_metrics.datasets_scanned.add(1);
        let chunks = source.chunks();
        let dataset = Arc::<str>::from(dataset);
        Ok::<_, anyhow::Error>(futures::stream::iter(chunks).then(move |chunk| {
            let pool = Arc::clone(&pool);
            let source = Arc::clone(&source);
            let dataset = Arc::clone(&dataset);
            async move {
                let nd = source
                    .poll_next(chunk)
                    .await
                    .with_context(|| format!("reading a chunk of dataset '{dataset}'"))?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "dataset '{dataset}' read no batch for a chunk of its own grid"
                        )
                    })?;
                let nd = under_fields(&nd, pool.atlas_view.table_schema().fields())?;
                let batch =
                    encode_nd_record_batch(&nd)?.with_schema(Arc::clone(&pool.projected_schema))?;
                Ok(batch)
            }
        }))
    })
    .try_flatten()
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compat, test_support};
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use std::path::Path;

    /// The logical schema of a fixture, the one a scan hands the pool.
    async fn logical_schema(dir: &Path) -> SchemaRef {
        let atlas = test_support::open(dir).await;
        Arc::new(
            compat::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        )
    }

    /// One handle on the pool of a fixture.
    async fn handle(
        pool: &AtlasReaderPool,
        dir: &Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: AtlasScanMetrics,
    ) -> Level1PoolStream {
        let (store, marker) = test_support::store_and_marker(dir);
        let logical = logical_schema(dir).await;
        let projected = Arc::new(encoded_schema(&logical));
        let open = PoolOpen {
            cache: None,
            logical_schema: logical,
            projected_schema: projected,
            predicate,
            scan_metrics: metrics,
        };
        pool.open(store, marker, open).await.unwrap()
    }

    /// The scan holds the pool in a `FileSource`, which must be `Send + Sync`.
    /// A consumer is not `Sync`, so it must never sit in the shared handle.
    #[test]
    fn the_pool_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AtlasReaderPool>();
        assert_send_sync::<Level1Pool>();
    }

    fn rows(batches: &[RecordBatch]) -> Vec<usize> {
        batches
            .iter()
            .map(|batch| decode_nd_record_batch(batch).unwrap().num_rows())
            .collect()
    }

    /// One encoded batch per dataset, in listing order, each on the encoded
    /// table schema.
    #[tokio::test]
    async fn the_pool_streams_one_encoded_batch_per_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let pool = AtlasReaderPool::new();

        let batches: Vec<RecordBatch> = handle(&pool, tmp.path(), None, metrics.clone())
            .await
            .try_collect()
            .await
            .unwrap();

        let logical = logical_schema(tmp.path()).await;
        let encoded = Arc::new(encoded_schema(&logical));
        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_eq!(batch.schema(), encoded, "the table schema, encoded");
        }
        assert_eq!(rows(&batches), vec![4, 3], "winter, then summer");
        assert_eq!(metrics.datasets_scanned.value(), 2);
    }

    /// A column the dataset lacks reads as a null column, so every batch fits
    /// the one schema. `summer` declares no `cycle`.
    #[tokio::test]
    async fn a_column_the_dataset_lacks_is_all_null() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let pool = AtlasReaderPool::new();

        let batches: Vec<RecordBatch> =
            handle(&pool, tmp.path(), None, AtlasScanMetrics::new(&set, 0))
                .await
                .try_collect()
                .await
                .unwrap();

        let summer = decode_nd_record_batch(&batches[1])
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(summer.num_rows(), 3);
        assert_eq!(summer.column_by_name("cycle").unwrap().null_count(), 3);
    }

    /// Two handles on one collection share one queue. A dataset one handle
    /// reads is not read again by the other.
    #[tokio::test]
    async fn two_handles_share_one_queue() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let pool = AtlasReaderPool::new();

        let mut first = handle(&pool, tmp.path(), None, metrics.clone()).await;
        let mut second = handle(&pool, tmp.path(), None, metrics.clone()).await;

        let winter = first.try_next().await.unwrap().unwrap();
        assert_eq!(rows(&[winter]), vec![4]);
        let summer = second.try_next().await.unwrap().unwrap();
        assert_eq!(
            rows(&[summer]),
            vec![3],
            "the second handle got the next dataset"
        );
        assert!(
            first.try_next().await.unwrap().is_none(),
            "nothing left for the first"
        );
        assert!(
            second.try_next().await.unwrap().is_none(),
            "nor for the second"
        );
        assert_eq!(metrics.datasets_scanned.value(), 2);
    }

    /// A predicate that rules out every dataset leaves nothing to read. The
    /// stream ends at once, and nothing panics on the empty queue.
    #[tokio::test]
    async fn a_collection_with_nothing_to_read_streams_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let pool = AtlasReaderPool::new();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(1.0e9)))),
        ));

        let batches: Vec<RecordBatch> = handle(&pool, tmp.path(), Some(predicate), metrics.clone())
            .await
            .try_collect()
            .await
            .unwrap();

        assert!(batches.is_empty());
        assert_eq!(metrics.datasets_pruned.value(), 10);
        assert_eq!(metrics.datasets_scanned.value(), 0);
    }

    /// A predicate the statistics can judge skips the datasets it rules out
    /// before any of them is read.
    #[tokio::test]
    async fn a_predicate_prunes_datasets_before_the_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let pool = AtlasReaderPool::new();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        ));

        let batches: Vec<RecordBatch> = handle(&pool, tmp.path(), Some(predicate), metrics.clone())
            .await
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 5, "d5 to d9 reach past 45");
        assert_eq!(metrics.datasets_pruned.value(), 5);
        assert_eq!(metrics.datasets_scanned.value(), 5);
    }
}
