//! The scan: what it reads, the datasets of each collection left to read, and
//! the batches they become.
//!
//! [`ScanSpec`] is decided once per partition and shared by every stage. A
//! collection's `CollectionQueue` is built once, on the first open, and every
//! later open of the same collection gets a `DatasetStream` over it. The
//! stream pops the next dataset off the shared queue, reads it one stored
//! chunk at a time, and yields each chunk as one nd-encoded batch under the
//! scan's fields. Two streams that poll at once drain different datasets, so a
//! dataset is read by one partition and by no other.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use arrow::{
    array::{ArrayRef, RecordBatch, new_null_array},
    compute::cast,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
};
use beacon_datafusion_ext::nd::{
    Dimensions, NdArrowArray, NdRecordBatch, encode_nd_record_batch, logical_schema,
};
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use beacon_nd_array::dataset::source::DatasetSource;
use crossbeam::queue::ArrayQueue;
use datafusion::{common::plan_err, error::Result, physical_plan::PhysicalExpr};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use parking_lot::RwLock;
use tokio::sync::OnceCell;

use crate::metrics::AtlasScanMetrics;
use crate::open::AtlasReaderCache;
use crate::view::AtlasView;

/// What one scan reads.
///
/// The source builds one per partition, and the opener, the queue and the
/// view of every collection the partition opens read through the same handle.
/// Nothing here changes after planning.
#[derive(Debug)]
pub struct ScanSpec {
    /// The scan's output schema, nd-encoded. Every batch goes out under it. Its
    /// field *names* are the columns to keep, and the encoding leaves names
    /// alone.
    pub projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped. The columns, the predicate
    /// and the pruning engine are written against it.
    pub logical_schema: SchemaRef,
    /// The dimensions the scan reads, or `None` for each dataset's default.
    pub read_dimensions: Option<Vec<String>>,
    /// The predicate to prune datasets with, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The rule that merged the table schema. It decides which casts read null.
    pub type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ScanSpec {
    /// A scan of `projected_schema`. The logical schema is derived from it.
    ///
    /// Refuses a schema of no column, see `require_projection`.
    pub fn new(
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Result<Self> {
        require_projection(&projected_schema)?;
        Ok(Self {
            logical_schema: logical_schema(&projected_schema)?,
            projected_schema,
            read_dimensions,
            predicate,
            type_widening,
        })
    }
}

/// Refuse a scan that projects no column.
///
/// A dataset's row count follows the dimensions of the columns it reads. With
/// no column there is no dimension set, so the count of a dataset that holds
/// arrays on different grids has no one answer. The scan refuses rather than
/// pick one. `COUNT(*)` reaches here; `COUNT(column)` projects a column and
/// does not.
pub(crate) fn require_projection(projected_schema: &Schema) -> Result<()> {
    if projected_schema.fields().is_empty() {
        return plan_err!(
            "an atlas scan must project at least one column: a dataset's row count \
             follows the dimensions of the columns it reads, and no column names none. \
             Use COUNT(column) instead of COUNT(*)"
        );
    }
    Ok(())
}

/// One cell per collection. The cell fills on the first open and never again.
type Cells = HashMap<Path, Arc<OnceCell<Arc<CollectionQueue>>>>;

/// The queues of one scan, one per collection, keyed by the container's path.
///
/// Shared by every partition of the scan through the source.
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
    ///
    /// The first call for a collection opens it, through `cache` when given,
    /// prunes its datasets, and queues the survivors. Every call gets a stream
    /// over that queue. The open and the prune are timed on the first caller's
    /// metrics, and each stream counts the datasets it reads on its own.
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

/// One collection's view, and the datasets of it left to read.
///
/// The queue holds no read state, so the partitions that share a collection
/// share it freely. [`CollectionQueue::stream`] makes one consumer of it.
pub(crate) struct CollectionQueue {
    view: AtlasView,
    /// The datasets left to read, in listing order.
    queue: ArrayQueue<String>,
}

impl CollectionQueue {
    /// Open the collection, prune its datasets, and queue the survivors.
    async fn open(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        spec: Arc<ScanSpec>,
        scan_metrics: &AtlasScanMetrics,
    ) -> anyhow::Result<Arc<Self>> {
        let open_timer = scan_metrics.open_time.timer();
        let view = AtlasView::new(cache, store, object_meta, spec).await?;
        drop(open_timer);
        let datasets = view.list_datasets(scan_metrics).await?;

        // A queue has at least one slot: `ArrayQueue::new(0)` panics. With no
        // dataset to read the slot stays empty, the first `pop` finds nothing,
        // and the stream ends at once.
        let queue = ArrayQueue::new(datasets.len().max(1));
        for dataset in datasets {
            queue
                .push(dataset)
                .map_err(|dataset| anyhow::anyhow!("no slot for dataset '{dataset}'"))?;
        }
        Ok(Arc::new(Self { view, queue }))
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

/// One consumer of a collection's shared queue.
///
/// It streams the datasets it pops, one nd-encoded batch per stored chunk.
/// The dataset in hand is a boxed stream, which is `Send` and not `Sync`, so
/// the consumer lives with the partition that polls it and never in the
/// shared queue.
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
            match this.queue.queue.pop() {
                Some(dataset) => {
                    this.current = Some(dataset_batches(
                        Arc::clone(&this.queue),
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
/// schema, so every batch of the queue has the one schema the scan expects.
fn dataset_batches(
    queue: Arc<CollectionQueue>,
    scan_metrics: AtlasScanMetrics,
    dataset: String,
) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
    futures::stream::once(async move {
        let source = queue.view.dataset(&dataset).await?;
        scan_metrics.datasets_scanned.add(1);
        let chunks = source.chunks();
        let dataset = Arc::<str>::from(dataset);
        Ok::<_, anyhow::Error>(futures::stream::iter(chunks).then(move |chunk| {
            let queue = Arc::clone(&queue);
            let source = Arc::clone(&source);
            let dataset = Arc::clone(&dataset);
            async move {
                let spec = queue.view.spec();
                let nd = read_chunk(&source, chunk, &dataset).await?;
                let nd = under_fields(
                    &nd,
                    spec.logical_schema.fields(),
                    spec.type_widening.as_ref(),
                )?;
                let batch =
                    encode_nd_record_batch(&nd)?.with_schema(Arc::clone(&spec.projected_schema))?;
                Ok(batch)
            }
        }))
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

/// `nd` under `fields`: every field in order, on the same target grid.
///
/// A column comes out under the array's own type, and the table may declare a
/// wider one: that is a cast. A field the dataset lacks is a rank-0 null,
/// which broadcasts to an all-null column. The decoder makes the same of a
/// null struct row, so the scan sees one thing either way. `type_widening` is
/// the rule that merged the table schema, and it decides which casts read null.
pub(crate) fn under_fields(
    nd: &NdRecordBatch,
    fields: &[FieldRef],
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<NdRecordBatch> {
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let column = match nd.schema().column_with_name(field.name()) {
            Some((index, _)) => {
                let column = nd.column(index);
                match as_field_type(Arc::clone(column.values()), field, type_widening)? {
                    Some(values) => NdArrowArray::try_new(values, column.dims().clone())?,
                    None => null_scalar(field),
                }
            }
            None => null_scalar(field),
        };
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(fields.to_vec()));
    Ok(NdRecordBatch::try_new(
        schema,
        columns,
        nd.target().clone(),
    )?)
}

/// A rank-0 null. It broadcasts to an all-null column of the target grid.
fn null_scalar(field: &Field) -> NdArrowArray {
    NdArrowArray::try_new(new_null_array(field.data_type(), 1), Dimensions::scalar())
        .expect("one element on no axis")
}

/// `values` in the type the table declares for `field`, or `None` for values
/// the table cannot hold.
///
/// A dataset may store a column narrower than the merged type, and the merge
/// widened it: that is a cast. A dataset of another family than the table
/// column reached the scan through `TypeConflict::KeepFirst` alone, and the
/// rule that merged the schema says so. Such a dataset reads as null.
fn as_field_type(
    values: ArrayRef,
    field: &Field,
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<Option<ArrayRef>> {
    if values.data_type() == field.data_type() {
        return Ok(Some(values));
    }
    match cast(&values, field.data_type()) {
        Ok(values) => Ok(Some(values)),
        Err(_) if type_widening.casts_leniently(values.data_type(), field.data_type()) => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("casting column '{}' to {}", field.name(), field.data_type())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema, test_support};
    use arrow::datatypes::SchemaRef;
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};
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
        let (store, marker) = test_support::store_and_marker(dir);
        let projected = Arc::new(encoded_schema(logical_schema(dir).await.as_ref()));
        let spec = ScanSpec::new(
            projected,
            None,
            predicate,
            Arc::new(DefaultArrowTypeWidening::new()),
        )
        .unwrap();
        queues
            .open(None, store, marker, Arc::new(spec), metrics)
            .await
            .unwrap()
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

    /// One encoded batch per dataset, in listing order, each on the encoded
    /// table schema.
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

        let encoded = Arc::new(encoded_schema(logical_schema(tmp.path()).await.as_ref()));
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
        let queues = CollectionQueues::new();

        let batches: Vec<RecordBatch> =
            stream(&queues, tmp.path(), None, AtlasScanMetrics::new(&set, 0))
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
