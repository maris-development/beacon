//! The scan: what it reads, and the batches it yields.
//!
//! `ScanSpec` is shared by every stage. Each `DatasetStream` pops datasets
//! from a shared `CollectionQueue`, so one partition reads each dataset.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::nd::{NdRecordBatch, encode_nd_record_batch, logical_schema};
use beacon_nd_array::dataset::source::DatasetSource;
use datafusion::{error::Result, physical_plan::PhysicalExpr};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use parking_lot::{Mutex, RwLock};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::metrics::AtlasScanMetrics;
use crate::open::AtlasReaderCache;
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

/// One cell per collection. The cell fills on the first open and never again.
type Cells = HashMap<Path, Arc<OnceCell<Arc<CollectionQueue>>>>;

/// The queues of one scan, one per collection, keyed by the container's path.
/// Shared by every partition through the source.
#[derive(Default, Clone)]
pub struct CollectionQueues {
    cells: Arc<RwLock<Cells>>,
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

    /// One stream over the collection at `object_meta`.
    /// The first call opens it and queues its datasets; every call gets a
    /// stream over the shared queue.
    pub(crate) async fn open(
        &self,
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        spec: Arc<ScanSpec>,
        scan_metrics: AtlasScanMetrics,
    ) -> anyhow::Result<DatasetStream> {
        let cell = Arc::clone(
            self.cells
                .write()
                .entry(object_meta.location.clone())
                .or_default(),
        );
        let queue = cell
            .get_or_try_init(|| {
                CollectionQueue::open(cache, store, object_meta, spec, &scan_metrics)
            })
            .await
            .cloned()?;
        Ok(queue.stream(scan_metrics))
    }
}

/// One collection's datasets left to read, and the view they read through.
/// Holds no read state, so partitions share it freely.
pub(crate) struct CollectionQueue {
    spec: Arc<ScanSpec>,
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
    async fn open(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        spec: Arc<ScanSpec>,
        scan_metrics: &AtlasScanMetrics,
    ) -> anyhow::Result<Arc<Self>> {
        let cancel = spec.cancel.clone();
        let opening = async move {
            let open_timer = scan_metrics.open_time.timer();
            let view = AtlasView::new(cache, store, object_meta, Arc::clone(&spec)).await?;
            drop(open_timer);
            let datasets = view.list_datasets(scan_metrics).await?.into();
            let pending = Pending {
                view: Some(view),
                datasets,
            };
            Ok(Arc::new(Self {
                spec,
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

    /// One consumer of the queue. It starts on no dataset, and counts the
    /// datasets it reads on `scan_metrics`.
    fn stream(self: Arc<Self>, scan_metrics: AtlasScanMetrics) -> DatasetStream {
        DatasetStream {
            queue: self,
            scan_metrics,
            current: None,
        }
    }
}

/// One consumer of a collection's shared queue. Streams one nd-encoded
/// batch per stored chunk of each dataset it pops.
pub(crate) struct DatasetStream {
    queue: Arc<CollectionQueue>,
    /// The partition's metrics. The datasets this consumer reads count here.
    scan_metrics: AtlasScanMetrics,
    /// The dataset this consumer is draining, if any.
    current: Option<BoxStream<'static, anyhow::Result<RecordBatch>>>,
}

impl Stream for DatasetStream {
    /// One nd-encoded batch.
    type Item = anyhow::Result<RecordBatch>;

    /// The next batch of the dataset in hand, or the first batch of the next
    /// dataset on the queue. The stream ends when the queue is empty and the
    /// dataset in hand is drained, and errors when the query is cancelled.
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
            if this.queue.spec.cancel.is_cancelled() {
                return Poll::Ready(Some(Err(cancelled())));
            }
            match this.queue.next_dataset() {
                Some((dataset, view)) => {
                    this.current = Some(dataset_batches(
                        view,
                        Arc::clone(&this.queue.spec),
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
        open_with(queues, dir, predicate, metrics, CancellationToken::new())
            .await
            .unwrap()
    }

    /// [`stream`], on a query `cancel` stops, and without the unwrap.
    async fn open_with(
        queues: &CollectionQueues,
        dir: &Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: AtlasScanMetrics,
        cancel: CancellationToken,
    ) -> anyhow::Result<DatasetStream> {
        let (store, marker) = test_support::store_and_marker(dir);
        let projected = Arc::new(encoded_schema(logical_schema(dir).await.as_ref()));
        let spec = ScanSpec::new(projected, None, predicate, cancel).unwrap();
        queues
            .open(None, store, marker, Arc::new(spec), metrics)
            .await
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
        .await
        .unwrap();

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

        let opened = open_with(
            &CollectionQueues::new(),
            tmp.path(),
            None,
            metrics.clone(),
            cancel,
        )
        .await;

        let error = match opened {
            Ok(_) => panic!("nothing opens for a cancelled query"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("cancelled"), "{error}");
        assert_eq!(metrics.datasets_scanned.value(), 0);
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
        CollectionQueues::new()
            .open(None, store, marker, Arc::new(spec), metrics)
            .await
            .unwrap()
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
        let (_, marker) = test_support::store_and_marker(tmp.path());
        let cell = Arc::clone(queues.cells.read().get(&marker.location).unwrap());
        assert!(cell.get().unwrap().pending.lock().view.is_none());
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
