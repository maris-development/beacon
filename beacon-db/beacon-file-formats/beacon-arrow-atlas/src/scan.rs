//! The scan: what it reads, and the batches it yields.
//!
//! `ScanSpec` is shared by every stage. Each `DatasetStream` walks the
//! collections in one order, and pops datasets from the shared
//! `CollectionQueue` of the collection at hand, so one partition reads each
//! dataset.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use anyhow::Context as _;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::nd::{NdRecordBatch, encode_nd_record_batch, logical_schema};
use beacon_nd_array::dataset::source::DatasetSource;
use datafusion::{error::Result, physical_plan::PhysicalExpr};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use parking_lot::Mutex;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::metrics::AtlasScanMetrics;
use beacon_nd_array::dataset::UnbroadcastableDataset;

use crate::view::AtlasView;

/// What one scan reads. Shared by every stage through one handle.
#[derive(Debug)]
pub struct ScanSpec {
    /// The scan's columns, with the nd encoding unwrapped. Columns, predicates
    /// and pruning are written against it.
    pub logical_schema: SchemaRef,
    /// The dimensions the scan reads, or `None` for each dataset's default.
    pub read_dimensions: Option<Vec<String>>,
    /// The predicate to prune datasets with, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Skip a dataset whose columns fit no one grid, instead of failing.
    pub skip_unbroadcastable: bool,
    /// The query's token. When it fires, every stage stops with an error.
    pub cancel: CancellationToken,
}

impl ScanSpec {
    /// A scan of `projected_schema`. The logical schema is derived from it.
    pub fn new(
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
            logical_schema: logical_schema(&projected_schema)?,
            read_dimensions,
            predicate,
            skip_unbroadcastable: false,
            cancel,
        })
    }

    /// The same scan, skipping the datasets that cannot broadcast when `skip`.
    pub fn with_skip_unbroadcastable(mut self, skip: bool) -> Self {
        self.skip_unbroadcastable = skip;
        self
    }
}

/// The error a stage reports when the query's token fires.
///
/// An error, not an end: a short stream must not look whole.
pub(crate) fn cancelled() -> anyhow::Error {
    anyhow::anyhow!("the query was cancelled")
}

/// The walk of one scan over its collections. Shared by every partition
/// through the source; the first partition to open the scan creates it.
#[derive(Default, Clone)]
pub struct CollectionQueues {
    walk: Arc<Mutex<Option<Arc<CollectionWalk>>>>,
}

impl Debug for CollectionQueues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionQueues").finish()
    }
}

impl CollectionQueues {
    pub fn new() -> Self {
        Self::default()
    }

    /// One consumer of the walk over `collections`.
    ///
    /// The first call fixes the walk. Every later call joins it at the first
    /// collection, and finds a drained one empty.
    pub(crate) fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        collections: Arc<[ObjectMeta]>,
        spec: Arc<ScanSpec>,
        scan_metrics: AtlasScanMetrics,
    ) -> DatasetStream {
        let walk = Arc::clone(
            self.walk
                .lock()
                .get_or_insert_with(|| Arc::new(CollectionWalk::new(store, collections, spec))),
        );
        DatasetStream::new(walk, scan_metrics)
    }
}

/// Every collection of one scan, read one after another.
///
/// Every partition reads the collection at hand, so only it and the next
/// one are open. A collection is released when its queue drains.
pub(crate) struct CollectionWalk {
    store: Arc<dyn ObjectStore>,
    collections: Arc<[ObjectMeta]>,
    spec: Arc<ScanSpec>,
    /// One cell per collection. The cell fills on the first open and never again.
    cells: Vec<OnceCell<Arc<CollectionQueue>>>,
    /// Set when an early open of the collection has started.
    prefetched: Vec<AtomicBool>,
}

impl CollectionWalk {
    fn new(
        store: Arc<dyn ObjectStore>,
        collections: Arc<[ObjectMeta]>,
        spec: Arc<ScanSpec>,
    ) -> Self {
        let n = collections.len();
        Self {
            store,
            collections,
            spec,
            cells: (0..n).map(|_| OnceCell::new()).collect(),
            prefetched: (0..n).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    /// The queue of collection `index`, and an early open of the next one.
    async fn queue(
        self: Arc<Self>,
        index: usize,
        scan_metrics: AtlasScanMetrics,
    ) -> anyhow::Result<Arc<CollectionQueue>> {
        self.prefetch(index + 1, &scan_metrics);
        self.open(index, &scan_metrics).await
    }

    /// The queue of collection `index`. The first call opens it.
    async fn open(
        &self,
        index: usize,
        scan_metrics: &AtlasScanMetrics,
    ) -> anyhow::Result<Arc<CollectionQueue>> {
        self.cells[index]
            .get_or_try_init(|| {
                CollectionQueue::open(
                    Arc::clone(&self.store),
                    self.collections[index].clone(),
                    Arc::clone(&self.spec),
                    scan_metrics,
                )
            })
            .await
            .cloned()
    }

    /// Open collection `index` in the background, once, so the partitions do
    /// not wait for its footers when they reach it.
    fn prefetch(self: &Arc<Self>, index: usize, scan_metrics: &AtlasScanMetrics) {
        if index >= self.cells.len()
            || self.spec.cancel.is_cancelled()
            || self.prefetched[index].swap(true, Ordering::AcqRel)
        {
            return;
        }
        let walk = Arc::clone(self);
        let scan_metrics = scan_metrics.clone();
        tokio::spawn(async move {
            // A failed open leaves the cell empty; the partition that opens it next reports the error.
            let _ = walk.open(index, &scan_metrics).await;
        });
    }
}

/// One collection's datasets left to read, and the view they read through.
/// Holds no read state, so partitions share it freely.
pub(crate) struct CollectionQueue {
    pending: Mutex<Pending>,
}

/// What a queue still holds. The view goes with the last dataset, so a
/// drained collection is released before the scan ends.
struct Pending {
    view: Option<AtlasView>,
    /// The datasets left to read, in listing order.
    datasets: VecDeque<String>,
}

impl CollectionQueue {
    /// Open the collection, prune its datasets, and queue the survivors.
    ///
    /// A cancelled query stops the open where it stands and reports the error.
    ///
    /// The open bypasses the reader cache, so the collection's segments and
    /// block cache go with the view.
    async fn open(
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        spec: Arc<ScanSpec>,
        scan_metrics: &AtlasScanMetrics,
    ) -> anyhow::Result<Arc<Self>> {
        let cancel = spec.cancel.clone();
        let opening = async move {
            let open_timer = scan_metrics.open_time.timer();
            let view = AtlasView::new(None, store, object_meta, spec).await?;
            drop(open_timer);
            let datasets = view.list_datasets(scan_metrics).await?.into();
            let pending = Pending {
                view: Some(view),
                datasets,
            };
            Ok(Arc::new(Self {
                pending: Mutex::new(pending),
            }))
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(cancelled()),
            opened = opening => opened,
        }
    }

    /// The next dataset and the view to read it through, or `None` once the
    /// queue is drained. The pop that finds it drained releases the view.
    fn next_dataset(&self) -> Option<(String, AtlasView)> {
        let mut pending = self.pending.lock();
        let Some(dataset) = pending.datasets.pop_front() else {
            pending.view = None;
            return None;
        };
        let view = pending
            .view
            .clone()
            .expect("the view lives until the last dataset is popped");
        Some((dataset, view))
    }
}

/// One consumer of the walk. Streams one nd-encoded batch per stored chunk
/// of each dataset it pops, collection after collection.
pub(crate) struct DatasetStream {
    walk: Arc<CollectionWalk>,
    /// The collection this consumer reads, as an index into the walk.
    index: usize,
    /// The queue of that collection, once open.
    queue: Option<Arc<CollectionQueue>>,
    /// The open of that queue, while it runs.
    opening: Option<BoxFuture<'static, anyhow::Result<Arc<CollectionQueue>>>>,
    /// The partition's metrics. The datasets this consumer reads count here.
    scan_metrics: AtlasScanMetrics,
    /// The dataset this consumer is draining, if any.
    current: Option<BoxStream<'static, anyhow::Result<RecordBatch>>>,
}

impl DatasetStream {
    /// A consumer at the first collection, on no dataset yet.
    fn new(walk: Arc<CollectionWalk>, scan_metrics: AtlasScanMetrics) -> Self {
        Self {
            walk,
            index: 0,
            queue: None,
            opening: None,
            scan_metrics,
            current: None,
        }
    }
}

impl Stream for DatasetStream {
    /// One nd-encoded batch.
    type Item = anyhow::Result<RecordBatch>;

    /// The next batch of the dataset in hand, or the first batch of the next
    /// dataset on the walk. The stream ends when the last collection's queue
    /// is empty and the dataset in hand is drained, and errors when the query
    /// is cancelled.
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
            if this.walk.spec.cancel.is_cancelled() {
                return Poll::Ready(Some(Err(cancelled())));
            }
            if let Some(queue) = &this.queue {
                match queue.next_dataset() {
                    Some((dataset, view)) => {
                        this.current = Some(dataset_batches(
                            view,
                            Arc::clone(&this.walk.spec),
                            this.scan_metrics.clone(),
                            dataset,
                        ));
                    }
                    None => {
                        this.queue = None;
                        this.index += 1;
                    }
                }
                continue;
            }
            if this.index >= this.walk.collections.len() {
                return Poll::Ready(None);
            }
            let opening = this.opening.get_or_insert_with(|| {
                Arc::clone(&this.walk)
                    .queue(this.index, this.scan_metrics.clone())
                    .boxed()
            });
            match opening.poll_unpin(cx) {
                Poll::Ready(Ok(queue)) => {
                    this.opening = None;
                    this.queue = Some(queue);
                }
                Poll::Ready(Err(error)) => {
                    this.opening = None;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// One dataset's batches: one encoded nd batch per stored chunk, in C order.
/// Each chunk carries the dataset's own columns; the adapting opener maps it
/// onto the scan's schema. The view is dropped once the dataset is built.
fn dataset_batches(
    view: AtlasView,
    spec: Arc<ScanSpec>,
    scan_metrics: AtlasScanMetrics,
    dataset: String,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    futures::stream::once(async move {
        let source = match view.dataset(&dataset).await {
            Ok(source) => source,
            // The flag turns a grid the columns do not fit into a skip.
            Err(error)
                if spec.skip_unbroadcastable
                    && error.downcast_ref::<UnbroadcastableDataset>().is_some() =>
            {
                tracing::warn!("skipping dataset '{dataset}': {error:#}");
                scan_metrics.datasets_skipped.add(1);
                return Ok(futures::stream::empty().boxed());
            }
            Err(error) => return Err(error),
        };
        drop(view);
        scan_metrics.datasets_scanned.add(1);
        let chunks = source.chunks();
        let dataset = Arc::<str>::from(dataset);
        Ok::<_, anyhow::Error>(
            futures::stream::iter(chunks)
                .then(move |chunk| {
                    let spec = Arc::clone(&spec);
                    let source = Arc::clone(&source);
                    let dataset = Arc::clone(&dataset);
                    async move {
                        if spec.cancel.is_cancelled() {
                            return Err(cancelled());
                        }
                        // The batch carries the dataset's own columns and types. The
                        // adapting opener above maps it onto the scan's schema.
                        let nd = read_chunk(&source, chunk, &dataset).await?;
                        Ok(encode_nd_record_batch(&nd)?)
                    }
                })
                .boxed(),
        )
    })
    .try_flatten()
    .boxed()
}

/// One chunk of `source`, read.
async fn read_chunk(
    source: &Arc<dyn DatasetSource>,
    chunk: Arc<dyn Any + Send + Sync>,
    dataset: &str,
) -> anyhow::Result<NdRecordBatch> {
    source
        .poll_next(chunk)
        .await
        .with_context(|| format!("reading a chunk of dataset '{dataset}'"))?
        .ok_or_else(|| {
            anyhow::anyhow!("dataset '{dataset}' read no batch for a chunk of its own grid")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema, test_support};
    use arrow::datatypes::SchemaRef;
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use object_store::ObjectStoreExt;
    use std::path::Path;

    /// The logical schema of a fixture, the one a scan is built on.
    async fn logical_schema(dir: &Path) -> SchemaRef {
        let atlas = test_support::open(dir).await;
        Arc::new(
            schema::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        )
    }

    /// One stream over the queue of a fixture.
    async fn stream(
        queues: &CollectionQueues,
        dir: &Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: AtlasScanMetrics,
    ) -> DatasetStream {
        open_with(queues, dir, predicate, metrics, CancellationToken::new()).await
    }

    /// [`stream`], on a query `cancel` stops.
    async fn open_with(
        queues: &CollectionQueues,
        dir: &Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: AtlasScanMetrics,
        cancel: CancellationToken,
    ) -> DatasetStream {
        let (store, marker) = test_support::store_and_marker(dir);
        let projected = Arc::new(encoded_schema(logical_schema(dir).await.as_ref()));
        let spec = ScanSpec::new(projected, None, predicate, cancel).unwrap();
        queues.open(store, Arc::from(vec![marker]), Arc::new(spec), metrics)
    }

    /// Collections `names` under `root`, each with the two datasets of
    /// `two_datasets`, with one store over `root` and their markers in walk order.
    async fn collections(
        root: &Path,
        names: &[&str],
    ) -> (Arc<dyn ObjectStore>, Arc<[ObjectMeta]>, SchemaRef) {
        for name in names {
            test_support::two_datasets(&root.join(format!("{name}.atlas"))).await;
        }
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
        let mut markers = Vec::new();
        for name in names {
            let path = object_store::path::Path::from(format!(
                "{name}.atlas/{}",
                crate::discover::ATLAS_MARKER
            ));
            markers.push(store.head(&path).await.unwrap());
        }
        let schema = logical_schema(&root.join(format!("{}.atlas", names[0]))).await;
        (store, markers.into(), schema)
    }

    /// The walk of `queues`, once a stream has created it.
    fn walk(queues: &CollectionQueues) -> Arc<CollectionWalk> {
        Arc::clone(
            queues
                .walk
                .lock()
                .as_ref()
                .expect("a stream created the walk"),
        )
    }

    /// Whether collection `index` of the walk has released its view.
    fn released(walk: &CollectionWalk, index: usize) -> bool {
        walk.cells[index]
            .get()
            .expect("the collection was opened")
            .pending
            .lock()
            .view
            .is_none()
    }

    /// A token that fires between two datasets ends the stream with an error,
    /// not with an end: a short answer must never look whole.
    #[tokio::test]
    async fn a_cancelled_stream_errors_instead_of_ending() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let queues = CollectionQueues::new();
        let cancel = CancellationToken::new();
        let mut stream = open_with(
            &queues,
            tmp.path(),
            None,
            AtlasScanMetrics::new(&set, 0),
            cancel.clone(),
        )
        .await;

        let winter = stream.try_next().await.unwrap().unwrap();
        assert_eq!(rows(&[winter]), vec![4], "the first dataset reads whole");
        cancel.cancel();
        let error = stream
            .try_next()
            .await
            .expect_err("the second dataset is never read")
            .to_string();

        assert!(error.contains("cancelled"), "{error}");
    }

    /// A token that has fired refuses the open itself, before the footer is
    /// read.
    #[tokio::test]
    async fn a_cancelled_query_does_not_open_the_collection() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let queues = CollectionQueues::new();

        let mut stream = open_with(&queues, tmp.path(), None, metrics.clone(), cancel).await;

        let error = stream
            .try_next()
            .await
            .expect_err("nothing opens for a cancelled query")
            .to_string();
        assert!(error.contains("cancelled"), "{error}");
        assert!(walk(&queues).cells[0].get().is_none(), "nothing was opened");
        assert_eq!(metrics.datasets_scanned.value(), 0);
    }

    /// One stream reads every collection of the walk in order, and each
    /// collection is released once its queue drains.
    #[tokio::test]
    async fn the_walk_reads_collections_in_order_and_releases_each() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, markers, schema) = collections(tmp.path(), &["a", "b", "c"]).await;
        let spec = ScanSpec::new(
            Arc::new(encoded_schema(&schema)),
            None,
            None,
            CancellationToken::new(),
        )
        .unwrap();
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let queues = CollectionQueues::new();
        let mut stream = queues.open(store, markers, Arc::new(spec), metrics.clone());

        let mut read = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            read.push(batch);
            // Datasets of a later collection are only read once the earlier one is drained.
            let walk = walk(&queues);
            if read.len() == 3 {
                assert!(released(&walk, 0), "collection a is drained and released");
            }
        }

        assert_eq!(rows(&read), vec![4, 3, 4, 3, 4, 3], "a, then b, then c");
        let walk = walk(&queues);
        for index in 0..3 {
            assert!(released(&walk, index), "collection {index} was released");
        }
        assert_eq!(metrics.datasets_scanned.value(), 6);
    }

    /// The open of a collection starts the open of the next one, so the
    /// partitions do not wait for it when they arrive.
    #[tokio::test]
    async fn the_next_collection_opens_early() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, markers, schema) = collections(tmp.path(), &["a", "b", "c"]).await;
        let spec = ScanSpec::new(
            Arc::new(encoded_schema(&schema)),
            None,
            None,
            CancellationToken::new(),
        )
        .unwrap();
        let set = ExecutionPlanMetricsSet::new();
        let queues = CollectionQueues::new();
        let mut stream = queues.open(
            store,
            markers,
            Arc::new(spec),
            AtlasScanMetrics::new(&set, 0),
        );

        stream
            .try_next()
            .await
            .unwrap()
            .expect("a first batch of a");
        let walk = walk(&queues);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while walk.cells[1].get().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("b opens while a is read");

        assert!(walk.cells[2].get().is_none(), "c waits until b is reached");
    }

    /// Two streams on one walk read each dataset of every collection once.
    #[tokio::test]
    async fn two_streams_on_one_walk_read_each_dataset_once() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, markers, schema) = collections(tmp.path(), &["a", "b"]).await;
        let spec = Arc::new(
            ScanSpec::new(
                Arc::new(encoded_schema(&schema)),
                None,
                None,
                CancellationToken::new(),
            )
            .unwrap(),
        );
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let queues = CollectionQueues::new();
        let first = queues.open(
            Arc::clone(&store),
            Arc::clone(&markers),
            Arc::clone(&spec),
            metrics.clone(),
        );
        let second = queues.open(store, markers, spec, metrics.clone());

        let (a, b) = tokio::join!(
            first.try_collect::<Vec<_>>(),
            second.try_collect::<Vec<_>>()
        );

        let mut all = rows(&a.unwrap());
        all.extend(rows(&b.unwrap()));
        all.sort_unstable();
        assert_eq!(all, vec![3, 3, 4, 4]);
        assert_eq!(metrics.datasets_scanned.value(), 4);
    }

    /// A stream over a fixture, with `skip_unbroadcastable` set as given.
    async fn stream_skipping(
        dir: &Path,
        skip_unbroadcastable: bool,
        metrics: AtlasScanMetrics,
    ) -> DatasetStream {
        let (store, marker) = test_support::store_and_marker(dir);
        let projected = Arc::new(encoded_schema(logical_schema(dir).await.as_ref()));
        let spec = ScanSpec::new(projected, None, None, CancellationToken::new())
            .unwrap()
            .with_skip_unbroadcastable(skip_unbroadcastable);
        CollectionQueues::new().open(store, Arc::from(vec![marker]), Arc::new(spec), metrics)
    }

    /// `mixed` holds columns on two grids and no list says which one to read.
    /// Without the flag, it fails the scan at its open.
    #[tokio::test]
    async fn a_dataset_that_cannot_broadcast_fails_the_scan() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids_then_plain(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);

        let error = stream_skipping(tmp.path(), false, metrics)
            .await
            .try_collect::<Vec<_>>()
            .await
            .expect_err("two grids, no list")
            .to_string();

        assert!(error.contains("mixed"), "{error}");
        assert!(error.contains("more than one grid"), "{error}");
    }

    /// With the flag, `mixed` is skipped and counted, and the scan reads
    /// `plain` after it.
    #[tokio::test]
    async fn the_flag_skips_a_dataset_that_cannot_broadcast() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids_then_plain(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);

        let batches: Vec<RecordBatch> = stream_skipping(tmp.path(), true, metrics.clone())
            .await
            .try_collect()
            .await
            .unwrap();

        assert_eq!(rows(&batches), vec![4], "`plain` alone is read");
        assert_eq!(metrics.datasets_skipped.value(), 1);
        assert_eq!(metrics.datasets_scanned.value(), 1);
    }

    /// The scan holds the queues in a `FileSource`, which must be `Send + Sync`.
    /// A stream is not `Sync`, so it must never sit in the shared queue.
    #[test]
    fn the_queues_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CollectionQueues>();
        assert_send_sync::<CollectionWalk>();
        assert_send_sync::<CollectionQueue>();
    }

    fn rows(batches: &[RecordBatch]) -> Vec<usize> {
        batches
            .iter()
            .map(|batch| decode_nd_record_batch(batch).unwrap().num_rows())
            .collect()
    }

    /// One encoded batch per dataset, in listing order, each under the
    /// dataset's own columns. `summer` declares no `cycle`, so its batch has
    /// none; the adapting opener above the queue fills it.
    #[tokio::test]
    async fn the_queue_streams_one_encoded_batch_per_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let queues = CollectionQueues::new();

        let batches: Vec<RecordBatch> = stream(&queues, tmp.path(), None, metrics.clone())
            .await
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(rows(&batches), vec![4, 3], "winter, then summer");
        assert!(batches[0].schema().column_with_name("cycle").is_some());
        assert!(batches[1].schema().column_with_name("cycle").is_none());
        assert_eq!(metrics.datasets_scanned.value(), 2);
    }

    /// Two streams over one collection share one queue. A dataset one stream
    /// reads is not read again by the other.
    #[tokio::test]
    async fn two_streams_share_one_queue() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let queues = CollectionQueues::new();

        let mut first = stream(&queues, tmp.path(), None, metrics.clone()).await;
        let mut second = stream(&queues, tmp.path(), None, metrics.clone()).await;

        let winter = first.try_next().await.unwrap().unwrap();
        assert_eq!(rows(&[winter]), vec![4]);
        let summer = second.try_next().await.unwrap().unwrap();
        assert_eq!(
            rows(&[summer]),
            vec![3],
            "the second stream got the next dataset"
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

        // The drained queue released its collection. A late stream finds
        // nothing to read and reopens nothing.
        assert!(released(&walk(&queues), 0));
        let mut third = stream(&queues, tmp.path(), None, metrics.clone()).await;
        assert!(third.try_next().await.unwrap().is_none());
        assert_eq!(metrics.datasets_scanned.value(), 2, "nothing was reread");
    }

    /// A predicate that rules out every dataset leaves nothing to read. The
    /// stream ends at once, and nothing panics on the empty queue.
    #[tokio::test]
    async fn a_collection_with_nothing_to_read_streams_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let set = ExecutionPlanMetricsSet::new();
        let metrics = AtlasScanMetrics::new(&set, 0);
        let queues = CollectionQueues::new();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(1.0e9)))),
        ));

        let batches: Vec<RecordBatch> =
            stream(&queues, tmp.path(), Some(predicate), metrics.clone())
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
        let queues = CollectionQueues::new();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        ));

        let batches: Vec<RecordBatch> =
            stream(&queues, tmp.path(), Some(predicate), metrics.clone())
                .await
                .try_collect()
                .await
                .unwrap();

        assert_eq!(batches.len(), 5, "d5 to d9 reach past 45");
        assert_eq!(metrics.datasets_pruned.value(), 5);
        assert_eq!(metrics.datasets_scanned.value(), 5);
    }
}
