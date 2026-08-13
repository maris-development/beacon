//! One file's work, shared by the partitions that read it.
//!
//! [`crate::arrow::split`] divides a file before it is read: each partition is
//! given a run of the chunk list and reads exactly that. It balances by
//! position, which is a guess about cost, and it rests on every partition
//! building an identical chunk list.
//!
//! A share divides the file as it is read instead. The first partition to arrive
//! opens the dataset and fills a queue with the subsets to read. Every partition
//! then pops a subset, reads it, and yields a batch, until the queue is empty.
//! Balance follows completion rather than position: a partition that draws a
//! cheap subset comes back for another one sooner. No partition needs to agree
//! with any other about anything, because no subset is in the queue twice.
//!
//! A partition drives its own reads. There is no driver task and no channel, so
//! nothing reads on once the partitions stop asking, and a `LIMIT` that drops a
//! stream mid-file simply stops popping. What that partition never popped is
//! still in the queue for the others.
//!
//! # Admission
//!
//! A share belongs to a physical plan, and a plan can be executed more than
//! once. A second execution must not inherit the first one's drained queue, or
//! it reads nothing at all.
//!
//! [`NdFileShare::join`] therefore admits exactly `consumers` callers per
//! generation. The first fills the queue and the rest attach to it; the caller
//! after that starts a new generation with a new queue. `consumers` is the
//! number of partitions the file was handed to, so one execution fills exactly
//! one generation.
//!
//! That rests on every partition holding the file actually executing it. That is
//! what DataFusion does: a plan executes each of its partitions once. A
//! partition that is never executed would leave a generation part-filled, and
//! the next execution would inherit it.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use futures::stream::BoxStream;
use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

use crate::NdArrayD;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, compute_predicate_masks,
    plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// The work of one file, shared by the partitions reading it.
#[derive(Debug)]
pub struct NdFileShare {
    /// How many partitions this file was handed to, and so how many callers
    /// make up one generation.
    consumers: usize,
    state: Mutex<Generation>,
}

#[derive(Debug)]
struct Generation {
    /// Callers admitted to this generation so far.
    admitted: usize,
    /// The work they share. The first caller fills it; the rest await it.
    shared: Arc<OnceCell<Arc<SharedRead>>>,
}

impl Generation {
    fn new() -> Self {
        Self {
            admitted: 0,
            shared: Arc::new(OnceCell::new()),
        }
    }
}

impl NdFileShare {
    /// A share for a file handed to `consumers` partitions.
    pub fn new(consumers: usize) -> Self {
        Self {
            consumers: consumers.max(1),
            state: Mutex::new(Generation::new()),
        }
    }

    /// Join the share, and return this generation's work.
    ///
    /// The first caller of a generation runs `build`; the rest wait on its
    /// result and never run their own. A `build` that fails is not remembered,
    /// so the next caller tries again.
    pub async fn join<F, Fut>(&self, build: F) -> Result<Arc<SharedRead>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<SharedRead>>>,
    {
        // The lock is held only to pick the generation, never across the build.
        let shared = {
            let mut state = self.state.lock();
            if state.admitted >= self.consumers {
                // The last generation is drained, so this caller belongs to a
                // new execution of the same plan and needs work of its own.
                *state = Generation::new();
            }
            state.admitted += 1;
            state.shared.clone()
        };

        shared.get_or_try_init(build).await.cloned()
    }
}

/// Which batches a shared read produces.
///
/// A scan is one or the other throughout, so the mode is fixed when the queue is
/// filled. The first partition to arrive chooses it, and every partition of a
/// scan would choose the same, so whichever arrives first is the right one.
#[derive(Debug, Clone)]
pub enum ReadMode {
    /// `beacon.nd`-encoded batches, which an `NdSourceExec` decodes above the
    /// scan. This is what a column read produces.
    Encoded,
    /// Flat, broadcast batches. This is what the `COUNT(*)` path reads, and the
    /// only path that prunes chunks, so it carries the predicate that prunes
    /// them.
    Flat(Option<PushdownFilter>),
}

/// One unit of work: what a partition reads for one pop.
///
/// The two dataset shapes divide differently, so the queue carries whichever
/// unit its file is made of. Both are read once and by one partition, which is
/// all the queue needs of them.
#[derive(Debug)]
enum Work {
    /// One hyperslab of a regular dataset's chunk grid.
    Grid(ArraySubset),
    /// One batch of a ragged dataset's plan, as a range of passing casts.
    ///
    /// A ragged dataset has no chunk grid. Its batches are cut where the cast
    /// boundaries fall, which takes the cumulative offsets and the predicate
    /// masks to work out, so the plan is built once by whichever partition
    /// arrives first. After that a range reads on its own, exactly as a chunk
    /// does.
    Ragged(Range<usize>),
}

/// A file opened once, and the subsets left to read from it.
#[derive(Debug)]
pub struct SharedRead {
    queue: ArrayQueue<Work>,
    read: ReadKind,
    mode: ReadMode,
    /// Per-dimension keep masks, computed once for the file rather than once per
    /// partition. Empty unless the mode is [`ReadMode::Flat`] with a predicate.
    dim_masks: Arc<Vec<(String, Vec<bool>)>>,
}

#[derive(Debug)]
enum ReadKind {
    Grid {
        arrays: Arc<IndexMap<String, Arc<dyn NdArrayD>>>,
        dims: Arc<Vec<String>>,
        schema: Arc<Schema>,
    },
    Ragged {
        plan: Arc<RaggedPlan>,
    },
}

impl SharedRead {
    /// Open `dataset` for sharing, and fill the queue with its subsets.
    ///
    /// A regular dataset is cut on its chunk grid, which is the same grid an
    /// unshared read walks. A ragged one is cut on its batch plan, which is the
    /// same plan an unshared read builds.
    pub async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        mode: ReadMode,
    ) -> Result<Arc<Self>> {
        let predicate = match &mode {
            ReadMode::Flat(predicate) => predicate.clone(),
            ReadMode::Encoded => None,
        };

        let regular = match dataset {
            AnyDataset::Regular(regular) => regular,
            AnyDataset::Ragged { ragged, .. } => {
                let plan = plan_ragged_read(ragged, batch_size, predicate)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let queue = ArrayQueue::new(plan.ranges.len().max(1));
                for range in plan.ranges.clone() {
                    // The capacity is the range count, so this cannot fail.
                    let _ = queue.push(Work::Ragged(range));
                }
                return Ok(Arc::new(Self {
                    queue,
                    read: ReadKind::Ragged {
                        plan: Arc::new(plan),
                    },
                    mode,
                    dim_masks: Arc::new(Vec::new()),
                }));
            }
        };

        let ChunkGrid { dims, chunks } =
            chunk_grid(&regular, batch_size).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // A dataset with no chunks still needs a queue, and `ArrayQueue` will
        // not take a capacity of zero.
        let queue = ArrayQueue::new(chunks.len().max(1));
        for subset in chunks {
            // The capacity is the chunk count, so this cannot fail.
            let _ = queue.push(Work::Grid(subset));
        }

        let arrays = Arc::new(regular.arrays);
        let schema = build_dataset_schema(&arrays);

        // Computed once for the file. An unshared read computes these per
        // partition, which reads the coordinate arrays once per partition.
        let dim_masks = compute_predicate_masks(&arrays, predicate)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        Ok(Arc::new(Self {
            queue,
            read: ReadKind::Grid {
                arrays,
                dims: Arc::new(dims),
                schema,
            },
            mode,
            dim_masks: Arc::new(dim_masks),
        }))
    }

    /// How many subsets are left. For tests and diagnostics.
    pub fn remaining(&self) -> usize {
        self.queue.len()
    }

    /// One partition's stream: pop, read, yield, until the queue is empty.
    ///
    /// Every partition of the file calls this and they all pull from the same
    /// queue, so the batches divide between them as fast as each can take them.
    /// A partition that drops its stream stops popping, and whatever it had not
    /// taken is left for the others.
    pub fn stream(self: Arc<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold(self, |shared| async move {
            let work = shared.queue.pop()?;
            let batches = shared.read(work);
            Some((batches, shared))
        })
        .flatten()
        .boxed()
    }

    /// The batches one unit of work produces.
    fn read(&self, work: Work) -> BoxStream<'static, Result<RecordBatch>> {
        match (work, &self.read) {
            (
                Work::Grid(subset),
                ReadKind::Grid {
                    arrays,
                    dims,
                    schema,
                },
            ) => {
                let arrays = arrays.clone();
                let dims = dims.clone();
                let schema = schema.clone();
                let masks = self.dim_masks.clone();
                let flat = matches!(self.mode, ReadMode::Flat(_));
                futures::stream::once(async move {
                    if flat {
                        // The flat path prunes: a chunk whose mask is all false
                        // holds nothing the query wants, and is skipped.
                        return read_chunk(&arrays, subset, schema, &dims, &masks)
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()));
                    }
                    let nd = read_nd_chunk(&arrays, &dims, schema, subset).await?;
                    beacon_datafusion_ext::nd::encode_nd_record_batch(&nd).map(Some)
                })
                .filter_map(|batch| futures::future::ready(batch.transpose()))
                .boxed()
            }
            (Work::Ragged(range), ReadKind::Ragged { plan }) => {
                let plan = plan.clone();
                // A ragged read is flat already, and its plan applied the
                // predicate when it chose which casts survive, so there is
                // nothing left to prune here.
                let encode = matches!(self.mode, ReadMode::Encoded);
                futures::stream::once(async move {
                    let flat = read_ragged_range(&plan, range)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    if encode {
                        beacon_datafusion_ext::nd::encode_flat_batch_as_nd(&flat)
                    } else {
                        Ok(flat)
                    }
                })
                .boxed()
            }
            // `build` pairs the unit with the kind, so a mismatch would be a bug
            // in this file rather than bad input.
            _ => futures::stream::once(async {
                Err(DataFusionError::Internal(
                    "nd share: work unit does not match the dataset it came from".to_string(),
                ))
            })
            .boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::TryStreamExt;

    use super::*;
    use crate::NdArray;
    use crate::dataset::Dataset;

    /// A dataset of `rows` values on one dimension.
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

    /// Drain one partition's stream into the rows it read.
    async fn drain(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .map(|batch| {
                beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// Run `consumers` partitions over one share, and return the rows each read.
    async fn read_in_partitions(
        share: Arc<NdFileShare>,
        source: AnyDataset,
        consumers: usize,
        batch_size: usize,
    ) -> Vec<usize> {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..consumers {
            let share = share.clone();
            let source = source.clone();
            set.spawn(async move {
                let shared = share
                    .join(|| async move { SharedRead::build(source, batch_size, ReadMode::Encoded).await })
                    .await
                    .expect("the share admits");
                drain(shared.stream()).await
            });
        }

        let mut rows = Vec::new();
        while let Some(read) = set.join_next().await {
            rows.push(read.expect("a partition finishes"));
        }
        rows
    }

/// A CF contiguous ragged-array dataset of `casts` casts, each holding one
    /// more observation than the last.
    ///
    /// Uneven on purpose: a plan over equal casts would divide the same way
    /// whatever the batch size, and hide a mistake in the mapping back to the
    /// dataset's own indices.
    async fn ragged_dataset(casts: usize) -> (AnyDataset, usize) {
        let sizes: Vec<i32> = (1..=casts as i32).collect();
        let observations: usize = sizes.iter().map(|size| *size as usize).sum();

        let row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            sizes,
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        // The attribute that marks the dataset as ragged and names the
        // observation dimension `row_size` counts into.
        let sample_dimension = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["obs".to_string()],
            vec![],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let station = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..casts).map(|cast| cast as f64).collect(),
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..observations).map(|obs| obs as f64 * 0.5).collect(),
            vec![observations],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("row_size".to_string(), Arc::new(row_size));
        arrays.insert(
            "row_size.sample_dimension".to_string(),
            Arc::new(sample_dimension),
        );
        arrays.insert("station".to_string(), Arc::new(station));
        arrays.insert("temperature".to_string(), Arc::new(temperature));

        let dataset = Dataset::new("ragged".to_string(), arrays).await;
        let any = AnyDataset::try_from_dataset(dataset).await.unwrap();
        assert!(matches!(any, AnyDataset::Ragged { .. }), "fixture is ragged");
        (any, observations)
    }

    /// The partitions of a ragged file read every observation once between them.
    ///
    /// A ragged dataset has no chunk grid, so its queue holds ranges of its
    /// batch plan instead. The property is the same and so is the risk: a range
    /// popped twice is an observation returned twice, and one popped by nobody
    /// is an observation lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_ragged_share_read_every_row_once() {
        const CASTS: usize = 60;

        // A batch size well under the file, so the plan holds many ranges to
        // divide, and one over it, so it holds one.
        for batch_size in [8_usize, 64, usize::MAX] {
            for consumers in [1_usize, 2, 3, 8] {
                let share = Arc::new(NdFileShare::new(consumers));
                let (source, observations) = ragged_dataset(CASTS).await;
                let rows = read_in_partitions(share, source, consumers, batch_size).await;

                assert_eq!(
                    rows.iter().sum::<usize>(),
                    observations,
                    "batch_size={batch_size} consumers={consumers}: every row once"
                );
            }
        }
    }

    /// A ragged file divides across partitions rather than falling to one.
    ///
    /// The queue used to hold a ragged file as a single unit, so one partition
    /// read it and the rest found the queue empty. This is the assertion that
    /// says it no longer does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_ragged_file_divides_across_partitions() {
        const CASTS: usize = 60;
        const CONSUMERS: usize = 4;

        let share = Arc::new(NdFileShare::new(CONSUMERS));
        let (source, observations) = ragged_dataset(CASTS).await;

        // Build the plan first, so the test can say how many ranges there were
        // to divide before the partitions take them.
        let shared = SharedRead::build(source.clone(), 8, ReadMode::Encoded)
            .await
            .expect("the plan builds");
        let ranges = shared.remaining();
        drop(shared);
        assert!(
            ranges >= CONSUMERS,
            "the fixture must hold at least one range per partition, got {ranges}"
        );

        let rows = read_in_partitions(share, source, CONSUMERS, 8).await;
        assert_eq!(rows.iter().sum::<usize>(), observations);
        assert!(
            rows.iter().filter(|read| **read > 0).count() > 1,
            "more than one partition must read: {rows:?}"
        );
    }

/// Drain a flat stream into the rows it read.
    ///
    /// A flat batch is already broadcast, so it needs no decoding. This is what
    /// the `COUNT(*)` path counts.
    async fn drain_flat(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// The partitions of a file count its rows once between them.
    ///
    /// `COUNT(*)` reads through the flat mode, which never decodes an nd array
    /// and so cannot inherit the division from the decode. A file every
    /// partition holds would be counted once per partition if this mode did not
    /// take its work from the same queue: the answer would grow with
    /// `target_partitions`, silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_count_its_rows_once() {
        const ROWS: usize = 10_000;

        for consumers in [1_usize, 2, 4, 8] {
            let share = Arc::new(NdFileShare::new(consumers));
            let source = dataset(ROWS).await;

            let mut set = tokio::task::JoinSet::new();
            for _ in 0..consumers {
                let share = share.clone();
                let source = source.clone();
                set.spawn(async move {
                    let shared = share
                        .join(|| async move {
                            SharedRead::build(source, 512, ReadMode::Flat(None)).await
                        })
                        .await
                        .expect("the share admits");
                    drain_flat(shared.stream()).await
                });
            }

            let mut counted = 0;
            while let Some(rows) = set.join_next().await {
                counted += rows.expect("a partition finishes");
            }
            assert_eq!(
                counted, ROWS,
                "consumers={consumers}: every row counted exactly once"
            );
        }
    }

    /// A ragged file counts its rows once too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_ragged_share_count_its_rows_once() {
        const CASTS: usize = 60;
        const CONSUMERS: usize = 4;

        let share = Arc::new(NdFileShare::new(CONSUMERS));
        let (source, observations) = ragged_dataset(CASTS).await;

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..CONSUMERS {
            let share = share.clone();
            let source = source.clone();
            set.spawn(async move {
                let shared = share
                    .join(
                        || async move { SharedRead::build(source, 8, ReadMode::Flat(None)).await },
                    )
                    .await
                    .expect("the share admits");
                drain_flat(shared.stream()).await
            });
        }

        let mut counted = 0;
        while let Some(rows) = set.join_next().await {
            counted += rows.expect("a partition finishes");
        }
        assert_eq!(counted, observations, "every observation counted once");
    }

    /// The partitions of one file read every row once between them.
    ///
    /// This is the property the whole design rests on. A subset popped by two
    /// partitions is a row returned twice, and one popped by none is a row lost,
    /// and neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_read_every_row_once() {
        const ROWS: usize = 10_000;

        for consumers in [1_usize, 2, 3, 8] {
            let share = Arc::new(NdFileShare::new(consumers));
            let source = dataset(ROWS).await;
            let rows = read_in_partitions(share, source, consumers, 512).await;

            assert_eq!(
                rows.iter().sum::<usize>(),
                ROWS,
                "consumers={consumers}: every row is read exactly once"
            );
        }
    }

    /// More partitions than subsets is not an error. The surplus find the queue
    /// empty and finish at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partition_with_nothing_left_to_pop_just_finishes() {
        const ROWS: usize = 100;

        // One chunk, eight partitions.
        let share = Arc::new(NdFileShare::new(8));
        let source = dataset(ROWS).await;
        let rows = read_in_partitions(share, source, 8, usize::MAX).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS);
        assert_eq!(
            rows.iter().filter(|read| **read > 0).count(),
            1,
            "one chunk is read by one partition, and the rest read nothing"
        );
    }

    /// The file is opened once, however many partitions read it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_partition_opens_the_file_for_all_of_them() {
        const CONSUMERS: usize = 8;

        let share = Arc::new(NdFileShare::new(CONSUMERS));
        let builds = Arc::new(AtomicUsize::new(0));
        let source = dataset(4_000).await;

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..CONSUMERS {
            let share = share.clone();
            let builds = builds.clone();
            let source = source.clone();
            set.spawn(async move {
                let shared = share
                    .join(|| async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        SharedRead::build(source, 512, ReadMode::Encoded).await
                    })
                    .await
                    .expect("the share admits");
                drain(shared.stream()).await
            });
        }
        while set.join_next().await.is_some() {}

        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "the file is opened once, not once per partition"
        );
    }

    /// A second execution of the same plan gets a queue of its own.
    ///
    /// The share belongs to the plan, not to one run of it. Without a new
    /// generation the second execution would inherit the first one's drained
    /// queue and return nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_second_execution_gets_a_new_queue() {
        const ROWS: usize = 4_000;
        const CONSUMERS: usize = 4;

        let share = Arc::new(NdFileShare::new(CONSUMERS));

        for execution in 1..=3 {
            let source = dataset(ROWS).await;
            let rows = read_in_partitions(share.clone(), source, CONSUMERS, 512).await;
            assert_eq!(
                rows.iter().sum::<usize>(),
                ROWS,
                "execution {execution} reads the whole dataset"
            );
        }
    }

    /// A partition that leaves early leaves its work for the others.
    ///
    /// Nothing is reserved up front, so a dropped stream costs only the subsets
    /// it had already popped. A `LIMIT` that stops one partition mid-file must
    /// not take rows away from the rest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn work_a_partition_does_not_take_is_left_for_the_others() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 500;

        let share = Arc::new(NdFileShare::new(2));
        let source = dataset(ROWS).await;

        let shared = share
            .join(|| {
                let source = source.clone();
                async move { SharedRead::build(source, BATCH, ReadMode::Encoded).await }
            })
            .await
            .expect("the share admits");
        let leaver = share
            .join(|| async { unreachable!("the first caller built it") })
            .await
            .expect("the share admits");

        // One partition takes a single batch and leaves.
        let mut early = leaver.stream();
        let first = early.next().await.expect("one batch").expect("it reads");
        let taken = beacon_datafusion_ext::nd::decode_nd_record_batch(&first)
            .unwrap()
            .num_rows();
        drop(early);

        // The other reads what is left, and between them that is the file.
        let rest = drain(shared.stream()).await;
        assert_eq!(
            taken + rest,
            ROWS,
            "the subsets the leaver never popped are still there"
        );
    }

    /// A build that fails is not remembered.
    #[tokio::test]
    async fn a_failed_build_is_retried() {
        let share = NdFileShare::new(4);

        let first = share
            .join(|| async {
                Err(DataFusionError::Execution(
                    "the file will not open".to_string(),
                ))
            })
            .await;
        assert!(first.is_err(), "the failure reaches the caller");

        let source = dataset(1_000).await;
        let second = share
            .join(|| async move { SharedRead::build(source, 512, ReadMode::Encoded).await })
            .await
            .expect("the next caller builds again");
        assert_eq!(drain(second.stream()).await, 1_000);
    }
}
