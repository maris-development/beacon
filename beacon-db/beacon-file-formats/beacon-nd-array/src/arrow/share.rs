//! One file's chunk stream, shared by the partitions that read it.
//!
//! [`crate::arrow::split`] divides a file before it is read: each partition is
//! given a run of the chunk list and reads exactly that. It balances by
//! position, which is a guess about cost, and it rests on every partition
//! building an identical chunk list.
//!
//! A share divides the file as it is read instead. One driver walks the chunks
//! and pushes them into a bounded [`flume`] channel; every partition holds a
//! clone of the receiver. The channel is MPMC, so a chunk reaches exactly one
//! partition, and a partition that finishes early simply takes the next one.
//! Balance follows completion rather than position, and no partition needs to
//! agree with any other about anything.
//!
//! # Admission
//!
//! A share belongs to a physical plan, and a plan can be executed more than
//! once. The second execution must not attach to the first one's drained
//! channel, or it reads nothing at all.
//!
//! [`NdStreamShare::join`] therefore admits exactly `consumers` callers per
//! generation. The first builds the stream and the rest attach to it; the caller
//! after that starts a new generation with a new stream. `consumers` is the
//! number of partitions the file was handed to, so one execution fills exactly
//! one generation.
//!
//! That rests on every partition holding the file actually executing it. That is
//! what DataFusion does: a plan executes each of its partitions once. A
//! partition that is never executed would leave a generation part-filled, and
//! the next execution would inherit it.
//!
//! # Early exit
//!
//! The driver stops as soon as a send fails, which happens once every receiver
//! is dropped. A `LIMIT` that satisfies itself after two chunks therefore stops
//! the read, rather than leaving a detached task to walk the rest of the file.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

use crate::arrow::nd_provider::any_dataset_as_encoded_stream_concurrent;
use crate::dataset::AnyDataset;

/// How many chunk reads a driver keeps in flight, and how many finished chunks
/// the channel holds.
///
/// The channel is the memory bound: a chunk of a wide grid is not small, and
/// this many sit in it while the partitions drain them. It is also the read
/// concurrency, since a driver reads ahead only as far as the channel allows.
pub const CHUNKS_IN_FLIGHT: usize = 4;

/// The stream of one file, shared by the partitions reading it.
#[derive(Debug)]
pub struct NdStreamShare {
    /// How many partitions this file was handed to, and so how many callers
    /// make up one generation.
    consumers: usize,
    state: Mutex<Generation>,
}

#[derive(Debug)]
struct Generation {
    /// Callers admitted to this generation so far.
    admitted: usize,
    /// The stream they share. The first caller fills it; the rest await it.
    stream: Arc<OnceCell<flume::Receiver<Result<RecordBatch>>>>,
}

impl Generation {
    fn new() -> Self {
        Self {
            admitted: 0,
            stream: Arc::new(OnceCell::new()),
        }
    }
}

impl NdStreamShare {
    /// A share for a file handed to `consumers` partitions.
    pub fn new(consumers: usize) -> Self {
        Self {
            consumers: consumers.max(1),
            state: Mutex::new(Generation::new()),
        }
    }

    /// Join the share, and return a receiver on this generation's stream.
    ///
    /// The first caller of a generation runs `build`; the rest wait on its
    /// result and never run their own. A `build` that fails is not remembered,
    /// so the next caller tries again.
    pub async fn join<F, Fut>(&self, build: F) -> Result<flume::Receiver<Result<RecordBatch>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<flume::Receiver<Result<RecordBatch>>>>,
    {
        // The lock is held only to pick the generation, never across the build.
        let stream = {
            let mut state = self.state.lock();
            if state.admitted >= self.consumers {
                // The last generation is full, so this caller belongs to a new
                // execution of the same plan and needs a stream of its own.
                *state = Generation::new();
            }
            state.admitted += 1;
            state.stream.clone()
        };

        stream.get_or_try_init(build).await.cloned()
    }
}

/// Read `dataset` into a bounded channel, and return the receiving end.
///
/// The driver runs until the dataset is exhausted or every receiver is dropped.
/// It holds the dataset, so the file stays open for as long as any partition is
/// still pulling from it.
pub fn spawn_encoded_producer(
    dataset: AnyDataset,
    batch_size: usize,
) -> flume::Receiver<Result<RecordBatch>> {
    spawn_stream_producer(any_dataset_as_encoded_stream_concurrent(
        dataset,
        batch_size,
        CHUNKS_IN_FLIGHT,
    ))
}

/// Drive `stream` into a bounded channel, and return the receiving end.
///
/// The driver runs until the stream ends or every receiver is dropped. It owns
/// the stream, so whatever the stream holds — the open file, the dataset — stays
/// alive exactly as long as some partition is still pulling.
pub(crate) fn spawn_stream_producer(
    mut stream: futures::stream::BoxStream<'static, Result<RecordBatch>>,
) -> flume::Receiver<Result<RecordBatch>> {
    let (sender, receiver) = flume::bounded(CHUNKS_IN_FLIGHT);

    tokio::spawn(async move {
        while let Some(batch) = stream.next().await {
            // A send fails only when every receiver is gone, which means the
            // scan is over -- a `LIMIT` was satisfied, or the query was
            // cancelled. Stop rather than read the rest of the file for nobody.
            if sender.send_async(batch).await.is_err() {
                break;
            }
        }
    });

    receiver
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::TryStreamExt;
    use indexmap::IndexMap;

    use super::*;
    use crate::dataset::Dataset;
    use crate::{NdArray, NdArrayD};

    /// A dataset of `rows` values on one dimension, cut into chunks by the
    /// batch size the caller passes.
    async fn dataset(rows: usize) -> AnyDataset {
        let values = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..rows as i64).collect(),
            vec![rows],
            vec!["row".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("value".to_string(), Arc::new(values));
        AnyDataset::Regular(Dataset::new("shared".to_string(), arrays).await)
    }

    /// Drain a receiver into the rows it carries.
    async fn drain(receiver: flume::Receiver<Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = receiver.into_stream().try_collect().await.unwrap();
        batches
            .iter()
            .map(|batch| {
                beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// The partitions of one file read every row once between them.
    ///
    /// This is the property the whole design rests on. A chunk handed to two
    /// partitions is a row returned twice, and a chunk handed to none is a row
    /// lost, and neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_read_every_row_once() {
        const ROWS: usize = 10_000;

        for consumers in [1_usize, 2, 3, 8] {
            let share = Arc::new(NdStreamShare::new(consumers));
            let source = dataset(ROWS).await;

            let mut set = tokio::task::JoinSet::new();
            for _ in 0..consumers {
                let share = share.clone();
                let source = source.clone();
                set.spawn(async move {
                    let receiver = share
                        .join(|| async move { Ok(spawn_encoded_producer(source, 512)) })
                        .await
                        .expect("the share admits");
                    drain(receiver).await
                });
            }

            let mut total = 0;
            while let Some(rows) = set.join_next().await {
                total += rows.expect("a partition finishes");
            }
            assert_eq!(
                total, ROWS,
                "consumers={consumers}: every row is read exactly once"
            );
        }
    }

    /// Only one partition builds the stream, however many join.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_partition_builds_the_stream_for_all_of_them() {
        const CONSUMERS: usize = 8;

        let share = Arc::new(NdStreamShare::new(CONSUMERS));
        let builds = Arc::new(AtomicUsize::new(0));
        let source = dataset(4_000).await;

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..CONSUMERS {
            let share = share.clone();
            let builds = builds.clone();
            let source = source.clone();
            set.spawn(async move {
                let receiver = share
                    .join(|| async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        Ok(spawn_encoded_producer(source, 512))
                    })
                    .await
                    .expect("the share admits");
                drain(receiver).await
            });
        }
        while set.join_next().await.is_some() {}

        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "the dataset is walked once, not once per partition"
        );
    }

    /// A second execution of the same plan gets a stream of its own.
    ///
    /// The share belongs to the plan, not to one run of it. Without a new
    /// generation the second execution would attach to the first one's drained
    /// channel and return nothing, which is the failure this admission rule
    /// exists to prevent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_second_execution_gets_a_new_stream() {
        const ROWS: usize = 4_000;
        const CONSUMERS: usize = 4;

        let share = Arc::new(NdStreamShare::new(CONSUMERS));

        for execution in 1..=3 {
            let source = dataset(ROWS).await;
            let mut set = tokio::task::JoinSet::new();
            for _ in 0..CONSUMERS {
                let share = share.clone();
                let source = source.clone();
                set.spawn(async move {
                    let receiver = share
                        .join(|| async move { Ok(spawn_encoded_producer(source, 512)) })
                        .await
                        .expect("the share admits");
                    drain(receiver).await
                });
            }

            let mut total = 0;
            while let Some(rows) = set.join_next().await {
                total += rows.expect("a partition finishes");
            }
            assert_eq!(total, ROWS, "execution {execution} reads the whole dataset");
        }
    }

    /// A build that fails is not remembered.
    #[tokio::test]
    async fn a_failed_build_is_retried() {
        let share = NdStreamShare::new(4);

        let first = share
            .join(|| async {
                Err(datafusion::error::DataFusionError::Execution(
                    "the file will not open".to_string(),
                ))
            })
            .await;
        assert!(first.is_err(), "the failure reaches the caller");

        let source = dataset(1_000).await;
        let second = share
            .join(|| async move { Ok(spawn_encoded_producer(source, 512)) })
            .await
            .expect("the next caller builds again");
        assert_eq!(drain(second).await, 1_000);
    }

    /// The driver stops once every partition drops its receiver.
    ///
    /// A `LIMIT` satisfied after one chunk leaves nobody to read the rest. The
    /// driver has to notice, or it walks the whole file for no one -- which on a
    /// large grid is a long read that nothing is waiting for.
    ///
    /// The count is of items the driver pulled, so it measures the read rather
    /// than the send. It settles a little above [`CHUNKS_IN_FLIGHT`], because
    /// the driver reads ahead as far as the channel allows before it blocks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_stops_when_every_partition_leaves() {
        const ITEMS: usize = 100_000;

        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = pulled.clone();
        let stream = futures::stream::iter(0..ITEMS)
            .map(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(RecordBatch::new_empty(Arc::new(
                    arrow::datatypes::Schema::empty(),
                )))
            })
            .boxed();

        let receiver = spawn_stream_producer(stream);

        // Take one item, the way a satisfied `LIMIT` would, then leave.
        let _first = receiver.recv_async().await.expect("one item arrives");
        drop(receiver);

        // Let the driver reach its next send and notice.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let stopped_at = pulled.load(Ordering::SeqCst);

        assert!(
            stopped_at < ITEMS,
            "the driver must stop early, but it pulled all {ITEMS} items"
        );
        assert!(
            stopped_at <= CHUNKS_IN_FLIGHT + 4,
            "the driver must stop within its read-ahead, but it pulled {stopped_at}"
        );
    }
}
