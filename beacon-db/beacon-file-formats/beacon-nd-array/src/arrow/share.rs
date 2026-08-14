use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crossbeam::queue::ArrayQueue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{BatchAdapter, BatchAdapterFactory};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use tokio::sync::OnceCell;

use crate::NdArrayD;
use crate::projection::DatasetProjection;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, chunk_is_pruned,
    compute_predicate_masks, plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::metrics::SharedReadMetrics;
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// What the scan wants out of a file, and so what a batch off the queue becomes.
///
/// A scan is one or the other throughout: the first partition to arrive decides,
/// and every partition of a scan would decide the same. The decision reaches
/// down to the read itself, because the two want different batches — a column
/// read wants `beacon.nd`-encoded chunks for the `NdSourceExec` above it to
/// decode, and `COUNT(*)` wants flat ones it can take a row count off.
#[derive(Debug)]
enum Output {
    /// Columns: nd-encoded batches, reordered and null-filled onto the
    /// projected schema.
    Columns(Arc<BatchAdapter>),
    /// `COUNT(*)`: flat batches, of which only the row count leaves, under the
    /// (empty) projected schema. See [`count_projection`].
    Rows(SchemaRef),
}

impl Output {
    /// Whether the read encodes its chunks, rather than broadcasting them flat.
    fn encoded(&self) -> bool {
        matches!(self, Output::Columns(_))
    }
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
///
/// The queue holds only what the query needs: [`SharedRead::build`] applies the
/// predicate as it fills it, so every unit in here is a read that will produce
/// rows.
#[derive(Debug)]
pub struct SharedRead {
    queue: ArrayQueue<Work>,
    read: ReadKind,
    /// Whether a chunk leaves nd-encoded. See [`Output`].
    encoded: bool,
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
    /// Open `dataset` for sharing, and fill the queue with the subsets that are
    /// worth reading.
    ///
    /// A regular dataset is cut on its chunk grid, which is the same grid an
    /// unshared read walks. A ragged one is cut on its batch plan, which is the
    /// same plan an unshared read builds.
    ///
    /// # The predicate is applied here, not later
    ///
    /// The coordinate arrays are read once, before the queue is filled, and a
    /// chunk no row of which can meet the predicate never enters it. So the
    /// queue holds the work the query actually needs, and the partitions divide
    /// *that*.
    ///
    /// Filtering as each chunk is popped would divide the file evenly and the
    /// work unevenly: one partition can draw a run of chunks that are all
    /// excluded and finish having read nothing, while another reads everything
    /// the query wanted. It would also leave `remaining` counting work that does
    /// not exist.
    ///
    /// A ragged dataset already works this way: `plan_ragged_read` applies the
    /// predicate when it chooses which casts survive, so its plan holds only the
    /// batches that have something in them.
    pub(crate) async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        predicate: Option<PushdownFilter>,
        encoded: bool,
        metrics: Option<&SharedReadMetrics>,
    ) -> Result<Arc<Self>> {
        let regular = match dataset {
            AnyDataset::Regular(regular) => regular,
            AnyDataset::Ragged { ragged, .. } => {
                let plan = plan_ragged_read(ragged, batch_size, predicate)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                // Casts the predicate excluded before the plan existed. No batch
                // can account for them, so they are counted here.
                if let (Some(metrics), Some((casts, rows))) = (metrics, plan.pruned) {
                    metrics.chunks_pruned.add(casts);
                    metrics.rows_pruned.add(rows);
                }

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
                    encoded,
                }));
            }
        };

        let ChunkGrid { dims, chunks } = chunk_grid(&regular, batch_size)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let arrays = Arc::new(regular.arrays);
        let schema = build_dataset_schema(&arrays);

        // Read once for the file, before anything is queued. An unshared read
        // computes these per partition, which reads the coordinate arrays once
        // per partition.
        let dim_masks = compute_predicate_masks(&arrays, predicate)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Only the chunks that can hold a row the query wants. The predicate is
        // applied again above the scan, so dropping one here only drops rows
        // that would have been dropped there.
        let (wanted, pruned): (Vec<ArraySubset>, Vec<ArraySubset>) = chunks
            .into_iter()
            .partition(|subset| !chunk_is_pruned(&dim_masks, &dims, subset));

        if let Some(metrics) = metrics {
            metrics.chunks_pruned.add(pruned.len());
            metrics
                .rows_pruned
                .add(pruned.iter().map(|subset| subset.rows()).sum::<usize>());
        }

        // A dataset with no chunks left still needs a queue, and `ArrayQueue`
        // will not take a capacity of zero.
        let queue = ArrayQueue::new(wanted.len().max(1));
        for subset in wanted {
            // The capacity is the chunk count, so this cannot fail.
            let _ = queue.push(Work::Grid(subset));
        }

        Ok(Arc::new(Self {
            queue,
            read: ReadKind::Grid {
                arrays,
                dims: Arc::new(dims),
                schema,
            },
            encoded,
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
    pub(crate) fn stream(
        self: Arc<Self>,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold((self, metrics), |(shared, metrics)| async move {
            let work = shared.queue.pop()?;
            let batches = shared.read(work, metrics.clone());
            Some((batches, (shared, metrics)))
        })
        .flatten()
        .boxed()
    }

    /// The batches one unit of work produces.
    fn read(
        &self,
        work: Work,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
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
                let flat = !self.encoded;
                // The rows this chunk holds, as the scan will broadcast them.
                // An encoded batch carries the lot in one row, so counting the
                // batch would say nothing.
                if let Some(metrics) = &metrics {
                    metrics.chunks_read.add(1);
                    metrics.rows_read.add(subset.rows());
                }
                futures::stream::once(async move {
                    // No masks here: `build` applied them when it filled the
                    // queue, so this chunk is one the query wants. Passing them
                    // again would rebuild the same chunk mask per read and
                    // always come to the same answer.
                    if flat {
                        return read_chunk(&arrays, subset, schema, &dims, &[])
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
                let encode = self.encoded;
                futures::stream::once(async move {
                    let flat = read_ragged_range(&plan, range)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    if let Some(metrics) = &metrics {
                        metrics.chunks_read.add(1);
                        metrics.rows_read.add(flat.num_rows());
                    }
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

/// One file's share: the dataset behind it, and what a partition does when it
/// arrives to find another partition already opening it.
///
/// Opening an nd file is not cheap — the file itself, its coordinate arrays, and
/// the predicate masks over them — and until it is done there is no queue to
/// draw from. So an arriving partition either waits for it or moves on, and
/// which of those is right depends on whether it has anything else to do.
#[derive(Debug)]
pub struct FileShare {
    dataset: OnceCell<Arc<SharedDataset>>,
    /// Taken by the partition that opens the file. Only [`Mode::Claim`] reads it.
    opening: std::sync::atomic::AtomicBool,
    mode: Mode,
}

/// What a partition does when another one is already opening the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Wait, then draw from the same queue.
    ///
    /// For a scan with too few files to fill its partitions: waiting costs
    /// nothing there, because the waiting partition has nothing else it could be
    /// doing, and dividing the queue afterwards is the only way to use it.
    Divide,
    /// Move on to the next file, leaving this one to whoever claimed it.
    ///
    /// For the tail pool of a scan that has files to spare. Waiting is what made
    /// an earlier version of that pool cost more than it returned — the partition
    /// has its own work and a whole pool ahead of it, so the one thing it must
    /// not do is block on somebody else's open.
    Claim,
}

impl FileShare {
    pub fn new(mode: Mode) -> Self {
        Self {
            dataset: OnceCell::new(),
            opening: std::sync::atomic::AtomicBool::new(false),
            mode,
        }
    }

    /// The dataset behind this share, opening it with `plan` if nobody has.
    ///
    /// `None` means another partition holds this file and this one should move
    /// on. Only [`Mode::Claim`] returns it.
    ///
    /// A claim is settled by one atomic swap, so the partition that loses is
    /// gone in nanoseconds rather than waiting out an open it gains nothing from.
    /// The winner reads the file to the end of its queue, so nothing is dropped
    /// by leaving: a partition that skips a file skips work that is being done.
    pub async fn open<F, Fut>(&self, plan: F) -> Result<Option<Arc<SharedDataset>>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<SharedDataset>>>,
    {
        if self.mode == Mode::Divide {
            return self
                .dataset
                .get_or_try_init(plan)
                .await
                .map(|dataset| Some(dataset.clone()));
        }

        // Already open: draw from its queue, whatever is left in it.
        if let Some(dataset) = self.dataset.get() {
            return Ok(Some(dataset.clone()));
        }
        // Not open, and somebody else got to it first.
        if self
            .opening
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Ok(None);
        }
        self.dataset
            .get_or_try_init(plan)
            .await
            .map(|dataset| Some(dataset.clone()))
    }
}

/// The shares of one scan, keyed by object path.
///
/// A file in here is in *every* partition's file group, so it is read through
/// its share and no other way: reading it whole would return every row once per
/// partition. [`share_files`] builds the map and the groups together, so the two
/// cannot disagree.
pub type FileShares =
    Arc<std::collections::HashMap<object_store::path::Path, Arc<FileShare>>>;

/// How a scan's files are dealt to its partitions, and which of them are shared.
pub struct SharedScan {
    /// One group per partition, in partition order.
    pub file_groups: Vec<FileGroup>,
    /// The share of each file that landed in every group.
    pub shares: FileShares,
}

/// Deal the scan's files to its partitions, and leave the largest in a pool
/// every partition can draw from.
///
/// A partition gets a group of its own files, smallest first, and then the pool.
/// A pooled file is in *every* group and is read through the one queue behind
/// its share, so whichever partitions reach it divide it between them; a file
/// dealt whole belongs to the partition holding it. Either way a file's chunks
/// are read exactly once.
///
/// # Why a deal alone does not balance
///
/// The deal is a guess made at plan time, and the only thing it can guess from
/// is file size. File size is a poor guide to what an nd file costs: the work
/// follows the uncompressed cells the query reads, and a netCDF collection holds
/// those at compression ratios that differ from file to file. One CORA year is
/// 19 GB of grid in 2.4 GB on disk. A deal that balances the 2.4 GB perfectly
/// can still leave one partition with twice the cells of another, and the query
/// then waits at the end on whichever partition guessed heavy.
///
/// The pool is work no partition owns, so a partition that runs out of its own
/// takes from the pool instead of stopping. Balance follows completion, which is
/// the one thing plan time cannot predict.
///
/// # Every partition starts the pool at a different file
///
/// This is what makes the pool pay, and the reason is measured. A pool walked in
/// the same order by every partition made one CORA year **1.7x slower** — the
/// same 22,898 chunks read, but `time_elapsed_opening` rose from 59 s to 251 s.
/// All 24 partitions finished their own files at about the same time, arrived at
/// the same pooled file, and [`OnceCell`] blocked 23 of them while the first
/// opened it and computed its predicate masks. Four times the opening cost, to
/// divide the six chunks an average CORA file holds.
///
/// So partition `i` starts the pool a fraction `i / partitions` of the way into
/// it and wraps around. The partitions spread over the pool instead of queueing
/// at the front of it: each opens the files in its own stretch, alone, and only
/// meets another partition once it has run out and walked into a stretch someone
/// else is still working. Reaching a file that is already drained costs a
/// resolved cell and an empty pop, so walking is nearly free — it is *waiting on
/// someone else's open* that was expensive, and a rotation is what stops that
/// from being the common case.
///
/// The pool goes last in every group, after the files the partition owns. That
/// leaves the *divisible* work for the end, where the imbalance shows up.
///
/// # And every partition walks it twice
///
/// A rotation stops the partitions colliding; it does not stop them colliding
/// altogether. When two do meet, the one that loses the race to open the file is
/// turned away and has walked past it by the time it is ready — so a single pass
/// would hand that file to the winner alone, however many chunks it held.
///
/// The second pass is what makes "claim it or move on" safe. By then the file is
/// open, so nobody is turned away, and the partitions that were come back for
/// whatever chunks the winner has not reached. On a file with six chunks the
/// winner has finished and the pass costs an empty pop; on a file with six
/// hundred it is the difference between one partition reading it and all of
/// them.
///
/// # `min_share_size`
///
/// It decides the pool for a scan too small to deal — fewer large files than
/// partitions, where dealing cannot fill them and sharing is the only thing that
/// can. `Some(size)` declines a file at or under it: one small file opened by
/// every partition for a chunk or two costs more than it returns.
///
/// `None` shares every such file. That is for a format whose object is not its
/// data: a zarr group's object is a `zarr.json` of a few KB that can front
/// terabytes of chunks, so any size test on it measures the wrong thing.
///
/// A scan with files to spare pools by *rank* instead, up to
/// [`TAIL_POOL_BYTE_SHARE`] of its bytes and at most
/// [`TAIL_POOL_FILES_PER_PARTITION`] per partition.
///
/// Returns `None` when this would change nothing: no pool, and a grouping that
/// already covers the partitions.
pub fn share_files(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_share_size: Option<u64>,
) -> Option<SharedScan> {
    if target_partitions <= 1 {
        return None;
    }

    let mut files: Vec<PartitionedFile> = file_groups
        .iter()
        .flat_map(FileGroup::iter)
        .cloned()
        .collect();
    if files.is_empty() {
        return None;
    }

    // Largest first. The pool takes a prefix of this, and the deal wants the
    // same order anyway.
    files.sort_by_key(|file| std::cmp::Reverse(file.effective_size()));

    let shareable = match min_share_size {
        None => files.len(),
        Some(min) => files
            .iter()
            .take_while(|file| file.effective_size() > min)
            .count(),
    };
    let (pool_len, mode) = if files.len() < target_partitions {
        // Too few files to give every partition one of its own. Dividing a file's
        // queue is the only thing that can fill them, and the size test says
        // which files are worth dividing.
        (shareable, Mode::Divide)
    } else {
        // Files to spare. The pool is a tail to steal from, so a partition that
        // finds one taken moves on rather than waiting.
        (tail_pool_len(&files, target_partitions), Mode::Claim)
    };

    let pool = &files[..pool_len];
    let owned = &files[pool_len..];

    // With no pool this is a re-deal, which is worth a rewritten scan only when
    // it spreads the files wider than the grouping already does.
    if pool.is_empty()
        && (file_groups.len() >= target_partitions || owned.len() <= file_groups.len())
    {
        return None;
    }

    // How many times a partition walks the pool.
    //
    // A claimed file turns away whoever loses the race to open it, and that
    // partition has moved past it by the time the file is ready. One pass would
    // leave it there: the winner would read a file every other partition had
    // been turned away from, however many chunks it held. So the pool is walked
    // twice. On the second pass the file is open, so nobody is turned away and
    // whatever chunks the winner has not reached yet are taken by the partitions
    // that come back for them.
    //
    // A divided file needs one pass. Nobody was turned away from it.
    let passes = match mode {
        Mode::Divide => 1,
        Mode::Claim => 2,
    };

    let mut groups = deal_by_size(owned, target_partitions);
    for (partition, group) in groups.iter_mut().enumerate() {
        // `deal_by_size` builds each group largest first; the partition reads it
        // the other way round, so its cheapest files are behind it soonest and
        // the first rows of the scan appear sooner. Measured on one CORA year:
        // 5.4 s to the first byte before, 2.9 s after.
        group.reverse();

        // The pool, rotated to this partition's own starting point.
        let start = pool_start(pool.len(), partition, target_partitions);
        for _ in 0..passes {
            group.extend(pool[start..].iter().cloned());
            group.extend(pool[..start].iter().cloned());
        }
    }

    let shares = pool
        .iter()
        .map(|file| {
            (
                file.object_meta.location.clone(),
                Arc::new(FileShare::new(mode)),
            )
        })
        .collect();

    Some(SharedScan {
        file_groups: groups.into_iter().map(FileGroup::new).collect(),
        shares: Arc::new(shares),
    })
}

/// Where partition `partition` of `target_partitions` starts reading a pool of
/// `pool_len` files.
///
/// Evenly spaced around the pool, so the partitions spread over it rather than
/// queueing at its first file. A pool smaller than the partition count gives
/// several partitions the same start, which is unavoidable — there is not a file
/// each — and harmless, because a pool that small is one the partitions are
/// meant to divide.
fn pool_start(pool_len: usize, partition: usize, target_partitions: usize) -> usize {
    if pool_len == 0 {
        return 0;
    }
    partition.saturating_mul(pool_len) / target_partitions.max(1)
}

/// The fraction of a scan's bytes the tail pool may hold, as a divisor.
///
/// Three: a third pooled, two thirds dealt. Enough work left unowned to absorb
/// what a deal of the rest gets wrong, and little enough that most of the scan
/// still runs on files a partition has to itself — which is cheaper, because a
/// dealt file is one plan entry and a pooled one is an entry per partition.
const TAIL_POOL_BYTE_SHARE: u64 = 3;

/// The most files the tail pool may hold, per partition.
///
/// A pooled file goes into every group, so the pool costs `files x partitions`
/// plan entries. This is the bound that keeps a collection of a hundred thousand
/// small files from planning millions of them.
const TAIL_POOL_FILES_PER_PARTITION: usize = 64;

/// How many of `files` — sorted largest first — belong in the tail pool.
///
/// Takes from the front until the pool holds its byte share, then stops. Never
/// more than half the files: there is always a dealt scan under the pool.
fn tail_pool_len(files: &[PartitionedFile], target_partitions: usize) -> usize {
    let total: u64 = files.iter().map(|file| file.effective_size()).sum();
    let want = total / TAIL_POOL_BYTE_SHARE;
    let cap = target_partitions
        .saturating_mul(TAIL_POOL_FILES_PER_PARTITION)
        .min(files.len() / 2);

    let mut bytes = 0u64;
    let mut len = 0usize;
    while len < cap && bytes < want {
        bytes = bytes.saturating_add(files[len].effective_size());
        len += 1;
    }
    len
}

/// Deal `files` — sorted largest first — one group per partition, by bytes.
///
/// Each file goes to the group holding the fewest bytes so far. That is the
/// standard greedy fit, and it beats dealing by count on a listing whose files
/// differ in size, which nd files do. Largest first matters: the small files at
/// the end are what evens out whatever the large ones left uneven.
///
/// A group can come back empty, when there are fewer files than partitions.
/// That partition reads only the pool, which is what it would have had anyway.
fn deal_by_size(files: &[PartitionedFile], target_partitions: usize) -> Vec<Vec<PartitionedFile>> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); target_partitions];
    // Least-loaded first, and the index breaks ties so the deal is reproducible.
    let mut loads: BinaryHeap<Reverse<(u64, usize)>> = (0..target_partitions)
        .map(|index| Reverse((0u64, index)))
        .collect();

    for file in files {
        let Reverse((load, index)) = loads
            .pop()
            .expect("the heap holds one entry per partition and each pop pushes one back");
        let size = file.effective_size();
        groups[index].push(file.clone());
        loads.push(Reverse((load.saturating_add(size), index)));
    }
    groups
}

/// Read a whole dataset as flat, broadcast batches, in file order.
///
/// This is the [`SharedRead`] a `COUNT(*)` builds, minus the counting: one
/// consumer, the whole queue, and the predicate pruning chunks it cannot use.
/// It is the only way to get flat batches out of a dataset, and it exists for
/// the readers' own tests — a scan goes through [`SharedDataset::plan`], which
/// resolves a projection and encodes.
pub async fn flat_stream(
    dataset: AnyDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    Ok(
        SharedRead::build(dataset, batch_size, predicate, false, None)
            .await?
            .stream(None),
    )
}

/// One file, opened and planned once: the queue its partitions draw from, and
/// what a batch off that queue becomes.
///
/// This is what a file-format opener puts behind its share. A shared file builds
/// one of these for all of its partitions; an unshared file builds one for the
/// single partition that holds it, which is the same work without the sharing.
///
/// Every format that reads through the nd pipeline plans a file the same way, so
/// the planning lives here rather than three times over. What differs between
/// them — how a file is opened, which dimensions it reads on — happens before
/// this and is handed in as an [`AnyDataset`].
#[derive(Debug)]
pub struct SharedDataset {
    read: Arc<SharedRead>,
    output: Output,
}

impl SharedDataset {
    /// Plan `dataset` for a scan that wants `projected_schema`.
    ///
    /// Resolves the projection, fills the queue, and decides what a batch off it
    /// becomes. `predicate` is a hint: it prunes chunks that cannot hold a row
    /// the query wants, and the scan is expected to apply it again above.
    ///
    /// `metrics` belong to the partition that plans. They take the counts made
    /// here rather than per chunk: a chunk the predicate excluded is dropped
    /// before the queue exists, so no reader of the queue can account for it.
    pub async fn plan(
        dataset: AnyDataset,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: Option<&SharedReadMetrics>,
    ) -> Result<Arc<Self>> {
        let dataset_schema: SchemaRef = Arc::new(
            crate::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to derive an Arrow schema from the dataset: {e}"
                ))
            })?,
        );

        // The columns of this file the query needs, in file order.
        let projection: Vec<usize> = dataset_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| projected_schema.index_of(field.name()).is_ok())
            .map(|(index, _)| index)
            .collect();

        let pushdown = predicate.clone().map(PushdownFilter::new);

        // No column is wanted, so this is `COUNT(*)`: the read is driven by
        // columns of its own and only the row counts leave.
        let (output, projection) = if projection.is_empty() {
            let counted = count_projection(&dataset, &dataset_schema, &predicate);
            (Output::Rows(projected_schema), counted)
        } else {
            // The scan carries nd columns, so adaptation happens in the encoded
            // (struct) domain: reorder and null-fill onto the projected schema.
            let source_schema: SchemaRef = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
                &dataset_schema.project(&projection)?,
            ));
            let adapter =
                BatchAdapterFactory::new(projected_schema).make_adapter(&source_schema)?;
            (Output::Columns(Arc::new(adapter)), projection)
        };

        let dataset = project(dataset, &dataset_schema, projection)?;
        let read =
            SharedRead::build(dataset, batch_size, pushdown, output.encoded(), metrics).await?;

        Ok(Arc::new(Self { read, output }))
    }

    /// How much of the file is left to read. For tests and diagnostics.
    pub fn remaining(&self) -> usize {
        self.read.remaining()
    }

    /// One partition's stream over the file.
    ///
    /// Every partition of a shared file calls this on the same `SharedDataset`,
    /// and they draw from the one queue behind it. `metrics` are this
    /// partition's, so what each one read is what it reports.
    pub fn stream(
        &self,
        metrics: Option<SharedReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let batches = self.read.clone().stream(metrics);
        match &self.output {
            Output::Columns(adapter) => {
                let adapter = adapter.clone();
                batches
                    .and_then(move |batch| {
                        let adapted = adapter.adapt_batch(&batch).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to adapt the batch onto the scan's schema: {e}"
                            ))
                        });
                        futures::future::ready(adapted)
                    })
                    .boxed()
            }
            Output::Rows(schema) => {
                let schema = schema.clone();
                batches
                    .and_then(move |batch| {
                        let counted = RecordBatch::try_new_with_options(
                            schema.clone(),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to build a count batch: {e}"
                            ))
                        });
                        futures::future::ready(counted)
                    })
                    .boxed()
            }
        }
    }
}

/// The columns a `COUNT(*)` reads, out of a file the query wants no column of.
///
/// Reading no column at all would give an empty stream and a count of zero. The
/// read is driven by the widest variable instead, so the row count is the full
/// broadcast row count — a scalar attribute like `.Conventions` would give one
/// row — plus any column the predicate names, so a pushed-down filter still
/// applies ([`PushdownFilter`] matches by name).
fn count_projection(
    dataset: &AnyDataset,
    dataset_schema: &SchemaRef,
    predicate: &Option<Arc<dyn PhysicalExpr>>,
) -> Vec<usize> {
    let driver = dataset
        .fields()
        .keys()
        .max_by_key(|name| {
            dataset
                .get_array(name)
                .map(|array| array.shape().iter().product::<usize>())
                .unwrap_or(0)
        })
        .and_then(|name| dataset_schema.index_of(name).ok())
        .unwrap_or(0);

    let mut projection = vec![driver];
    if let Some(predicate) = predicate {
        for column in datafusion::physical_expr::utils::collect_columns(predicate) {
            if let Ok(index) = dataset_schema.index_of(column.name()) {
                projection.push(index);
            }
        }
    }
    projection.sort_unstable();
    projection.dedup();
    projection
}

/// Keep only `projection` of `dataset`, or all of it when that is everything.
fn project(
    dataset: AnyDataset,
    dataset_schema: &SchemaRef,
    projection: Vec<usize>,
) -> Result<AnyDataset> {
    if projection.len() == dataset_schema.fields().len() {
        return Ok(dataset);
    }
    dataset
        .project(&DatasetProjection {
            dimension_projection: None,
            index_projection: Some(projection),
        })
        .map_err(|e| DataFusionError::Execution(format!("Failed to project the dataset: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;

    use super::*;
    use crate::NdArray;
    use crate::dataset::Dataset;

    /// A projection that wants no column, so a plan takes the `COUNT(*)` path.
    ///
    /// These tests are about who opens the file, not what comes out of it, and
    /// this is the shortest way to a real [`SharedDataset`] with a filled queue.
    fn no_columns() -> SchemaRef {
        Arc::new(Schema::empty())
    }

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
        assert!(
            matches!(any, AnyDataset::Ragged { .. }),
            "fixture is ragged"
        );
        (any, observations)
    }

    /// Drain one partition's encoded stream into the rows it read.
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

    /// Drain a flat stream into the rows it read.
    ///
    /// A flat batch is already broadcast, so it needs no decoding. This is what
    /// the `COUNT(*)` path counts.
    async fn drain_flat(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Run `partitions` partitions over one read, and return the rows each one
    /// took. This is the shape the opener runs: every partition of a file holds
    /// the same `Arc` and streams from it.
    async fn read_in_partitions(
        shared: Arc<SharedRead>,
        partitions: usize,
        encoded: bool,
    ) -> Vec<usize> {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..partitions {
            let shared = shared.clone();
            set.spawn(async move {
                if encoded {
                    drain(shared.stream(None)).await
                } else {
                    drain_flat(shared.stream(None)).await
                }
            });
        }

        let mut rows = Vec::new();
        while let Some(read) = set.join_next().await {
            rows.push(read.expect("a partition finishes"));
        }
        rows
    }

    // ── pruning on the predicate ───────────────────────────────────────

    /// `value > threshold`, as the scan would push it down.
    fn greater_than(column: &str, threshold: i64) -> PushdownFilter {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        PushdownFilter::new(Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column, 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
        )) as Arc<dyn PhysicalExpr>)
    }

    /// Every value an encoded read returned, decoded and broadcast.
    async fn values_read(stream: BoxStream<'static, Result<RecordBatch>>) -> Vec<i64> {
        use arrow::array::Int64Array;

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            let flat = beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                .unwrap()
                .materialize()
                .unwrap();
            let column = flat
                .column_by_name("value")
                .expect("the fixture has one column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column")
                .clone();
            values.extend(column.iter().flatten());
        }
        values
    }

    /// The queue holds only the chunks the predicate keeps, and keeps every row
    /// the query wants.
    ///
    /// Both halves matter and neither implies the other. Reading everything is
    /// correct but pointless; reading less is pointless if it drops a row the
    /// query asked for, and nothing about that raises an error — the chunk is
    /// simply never fetched.
    ///
    /// The queue length is the first assertion because it is where the saving
    /// is. A queue still holding the excluded chunks would give a partition a
    /// run of work that turns out to be nothing, while another partition reads
    /// everything the query wanted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_queue_holds_only_the_chunks_the_predicate_keeps() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let whole = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");
        let chunks = whole.remaining();
        let all = values_read(whole.stream(None)).await;
        assert_eq!(all.len(), ROWS, "the unfiltered read returns the file");

        let pruned = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        // The fixture counts up, so a chunk below the threshold holds nothing
        // the query wants, and it is left out before any partition can draw it.
        let queued = pruned.remaining();
        assert!(
            queued > 0 && queued < chunks,
            "the queue should hold some of the {chunks} chunks, it holds {queued}"
        );

        // The rows read come from the queued chunks and no others. The last
        // chunk of the file is short, so this is a bound rather than a product.
        let kept = values_read(pruned.stream(None)).await;
        assert!(
            kept.len() <= queued * BATCH && kept.len() > (queued - 1) * BATCH,
            "{} rows off {queued} chunks of at most {BATCH}",
            kept.len()
        );

        // Nothing the predicate keeps may go missing. The read is allowed to
        // return more than that — it skips whole chunks, not rows — and the
        // scan applies the predicate again above.
        let wanted: Vec<i64> = all.iter().copied().filter(|v| *v > THRESHOLD).collect();
        let returned: std::collections::HashSet<i64> = kept.iter().copied().collect();
        assert!(
            wanted.iter().all(|value| returned.contains(value)),
            "the read dropped a row the predicate keeps"
        );
    }

    /// A predicate no row can meet leaves an empty queue.
    ///
    /// The partitions then find nothing to do, which is the point: the file is
    /// never opened for reading at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_predicate_nothing_meets_queues_no_work() {
        const ROWS: usize = 10_000;

        let shared = SharedRead::build(
            dataset(ROWS).await,
            512,
            Some(greater_than("value", ROWS as i64 * 10)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        assert_eq!(shared.remaining(), 0, "no chunk can hold a matching row");
        assert_eq!(drain(shared.stream(None)).await, 0);
    }

    /// The flat read and the nd read skip the same chunks.
    ///
    /// `COUNT(*)` goes one way and a column read the other. They must agree
    /// about which chunks hold nothing, or a count stops matching its own rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn both_modes_skip_the_same_chunks() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let nd = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
            None,
        )
        .await
        .expect("the read builds");
        let nd_rows = drain(nd.stream(None)).await;

        let flat = SharedRead::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            false,
            None,
        )
        .await
        .expect("the read builds");
        let flat_rows = drain_flat(flat.stream(None)).await;

        assert_eq!(nd_rows, flat_rows, "the two modes read the same chunks");
        assert!(
            nd_rows > 0 && nd_rows < ROWS,
            "and they pruned some of them"
        );
    }

    /// A predicate on a column the file does not bound prunes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unrelated_predicate_reads_the_whole_file() {
        const ROWS: usize = 4_000;

        let shared = SharedRead::build(
            dataset(ROWS).await,
            512,
            Some(greater_than("no_such_column", 10)),
            true,
            None,
        )
        .await
        .expect("the read builds");

        assert_eq!(drain(shared.stream(None)).await, ROWS);
    }

    /// The partitions of one file read every row once between them.
    ///
    /// This is the property the whole design rests on. A subset popped by two
    /// partitions is a row returned twice, and one popped by none is a row lost,
    /// and neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_share_read_every_row_once() {
        const ROWS: usize = 10_000;

        for partitions in [1_usize, 2, 3, 8] {
            let shared = SharedRead::build(dataset(ROWS).await, 512, None, true, None)
                .await
                .expect("the read builds");
            let rows = read_in_partitions(shared, partitions, true).await;

            assert_eq!(
                rows.iter().sum::<usize>(),
                ROWS,
                "partitions={partitions}: every row is read exactly once"
            );
        }
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

        for partitions in [1_usize, 2, 4, 8] {
            let shared = SharedRead::build(dataset(ROWS).await, 512, None, false, None)
                .await
                .expect("the read builds");
            let counted = read_in_partitions(shared, partitions, false).await;

            assert_eq!(
                counted.iter().sum::<usize>(),
                ROWS,
                "partitions={partitions}: every row counted exactly once"
            );
        }
    }

    /// Several partitions draw from one file's queue, and each gets its own
    /// work.
    ///
    /// Every partition takes a batch before any of them drains, so what is
    /// asserted is the division and not the scheduling. Letting them race and
    /// counting afterwards says nothing: a queue this small is often emptied by
    /// whichever partition is polled first, and that is correct behaviour.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_shared_file_divides_across_partitions() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const PARTITIONS: usize = 4;

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");
        let chunks = shared.remaining();
        assert!(
            chunks >= PARTITIONS,
            "the fixture must hold at least one chunk per partition, it holds {chunks}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS)
            .map(|_| shared.clone().stream(None))
            .collect();
        let mut taken = 0;
        for (partition, stream) in streams.iter_mut().enumerate() {
            let batch = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("partition {partition} gets a chunk of its own"))
                .expect("it reads");
            taken += beacon_datafusion_ext::nd::decode_nd_record_batch(&batch)
                .unwrap()
                .num_rows();
        }
        assert_eq!(
            shared.remaining(),
            chunks - PARTITIONS,
            "each partition took one chunk, and no chunk went to two of them"
        );

        // The rest divides between them, and between them they read the file.
        let mut rest = 0;
        for stream in streams {
            rest += drain(stream).await;
        }
        assert_eq!(taken + rest, ROWS, "every row is read exactly once");
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
            for partitions in [1_usize, 2, 3, 8] {
                let (source, observations) = ragged_dataset(CASTS).await;
                let shared = SharedRead::build(source, batch_size, None, true, None)
                    .await
                    .expect("the plan builds");
                let rows = read_in_partitions(shared, partitions, true).await;

                assert_eq!(
                    rows.iter().sum::<usize>(),
                    observations,
                    "batch_size={batch_size} partitions={partitions}: every row once"
                );
            }
        }
    }

    /// A ragged file counts its rows once too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_partitions_of_a_ragged_share_count_its_rows_once() {
        const CASTS: usize = 60;
        const PARTITIONS: usize = 4;

        let (source, observations) = ragged_dataset(CASTS).await;
        let shared = SharedRead::build(source, 8, None, false, None)
            .await
            .expect("the plan builds");
        let counted = read_in_partitions(shared, PARTITIONS, false).await;

        assert_eq!(
            counted.iter().sum::<usize>(),
            observations,
            "every observation counted once"
        );
    }

    /// A ragged file divides across partitions rather than falling to one.
    ///
    /// The queue used to hold a ragged file as a single unit, so one partition
    /// read it and the rest found the queue empty. This is the assertion that
    /// says it no longer does — and, as above, it takes a batch per partition
    /// up front so that it asserts the division rather than the scheduling.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_ragged_file_divides_across_partitions() {
        const CASTS: usize = 60;
        const PARTITIONS: usize = 4;

        let (source, observations) = ragged_dataset(CASTS).await;
        let shared = SharedRead::build(source, 8, None, true, None)
            .await
            .expect("the plan builds");
        let ranges = shared.remaining();
        assert!(
            ranges >= PARTITIONS,
            "the fixture must hold at least one range per partition, got {ranges}"
        );

        let mut streams: Vec<_> = (0..PARTITIONS)
            .map(|_| shared.clone().stream(None))
            .collect();
        let mut taken = 0;
        for (partition, stream) in streams.iter_mut().enumerate() {
            let batch = stream
                .next()
                .await
                .unwrap_or_else(|| panic!("partition {partition} gets a range of its own"))
                .expect("it reads");
            taken += beacon_datafusion_ext::nd::decode_nd_record_batch(&batch)
                .unwrap()
                .num_rows();
        }
        assert_eq!(
            shared.remaining(),
            ranges - PARTITIONS,
            "each partition took one range, and no range went to two of them"
        );

        let mut rest = 0;
        for stream in streams {
            rest += drain(stream).await;
        }
        assert_eq!(taken + rest, observations, "every row is read exactly once");
    }

    /// A claimed file goes to one partition; the rest are turned away at once.
    ///
    /// The whole point of [`Mode::Claim`]: the loser does not wait out an open it
    /// gains nothing from. Nothing is dropped by leaving, because the winner
    /// reads the file to the end of its queue.
    ///
    /// The open holds a barrier the other nine wait at, so they are guaranteed
    /// to arrive while it is in flight. Without it the winner can finish the
    /// whole open before the others are scheduled — which is not a failure, it is
    /// a late arrival, and every one of them then legitimately gets the dataset.
    /// This test is about the arrivals that are *not* late.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_claimed_file_turns_away_every_partition_but_one() {
        let share = Arc::new(FileShare::new(Mode::Claim));

        // Ten partitions reach the same file at once. `plan` counts how many of
        // them actually opened it, and holds until all ten have tried.
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let arrived = Arc::new(tokio::sync::Barrier::new(10));
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let share = share.clone();
            let opens = opens.clone();
            let arrived = arrived.clone();
            tasks.push(tokio::spawn(async move {
                let taken = share
                    .open(async || {
                        opens.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        // Every other partition reaches the share before this
                        // open finishes, so each one meets it in flight.
                        arrived.wait().await;
                        SharedDataset::plan(dataset(64).await, no_columns(), 16, None, None)
                            .await
                    })
                    .await
                    .expect("planning succeeds")
                    .is_some();
                // A partition turned away never entered the closure, so it has
                // to release the barrier from here or the opener would hang.
                if !taken {
                    arrived.wait().await;
                }
                taken
            }));
        }

        let mut got = 0;
        for task in tasks {
            if task.await.expect("the task finishes") {
                got += 1;
            }
        }

        assert_eq!(
            opens.load(std::sync::atomic::Ordering::Acquire),
            1,
            "the file is opened once"
        );
        assert!(got >= 1, "somebody reads it");
        assert!(
            got < 10,
            "and the partitions that lost the race were turned away, not queued: {got} of 10 got it"
        );
    }

    /// A partition that comes back for a file reads what is left of it, and
    /// nothing twice.
    ///
    /// This is what the pool's second pass rests on. A partition turned away
    /// while another was opening the file walks the pool again; by then the file
    /// is open, and the queue behind the share hands out the chunks the first
    /// partition has not reached. If a repeat entry re-read the file instead, the
    /// scan would return rows twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coming_back_to_a_file_reads_the_rest_of_it_and_no_more() {
        const ROWS: usize = 640;
        const BATCH: usize = 16;

        let share = FileShare::new(Mode::Claim);
        let plan = async || {
            SharedDataset::plan(dataset(ROWS).await, no_columns(), BATCH, None, None).await
        };

        let dataset = share
            .open(plan)
            .await
            .expect("planning succeeds")
            .expect("the first partition opens it");
        let chunks = dataset.remaining();
        assert!(chunks > 2, "a queue worth dividing: {chunks}");

        // One partition takes a single chunk and stops there, as it would if the
        // file it is on runs out of interest before the queue does.
        let mut first = dataset.stream(None);
        first.next().await.expect("a chunk").expect("it reads");

        // The partition that was turned away comes back on the second pass.
        let second = share
            .open(plan)
            .await
            .expect("planning succeeds")
            .expect("the file is open now, so nobody is turned away");
        let taken_back = drain_flat(second.stream(None)).await;

        assert_eq!(
            dataset.remaining(),
            0,
            "the second pass finished what the first left"
        );
        let left_on_the_first = drain_flat(first).await;
        assert_eq!(
            taken_back + left_on_the_first + BATCH,
            ROWS,
            "and between them every row was read exactly once"
        );
    }

    /// A partition arriving after the open still gets the file.
    ///
    /// Turning it away would be wrong: the queue may still hold chunks, and
    /// taking them is exactly the stealing the pool exists for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claimed_file_is_still_open_to_a_late_arrival() {
        let share = FileShare::new(Mode::Claim);
        let plan =
            async || SharedDataset::plan(dataset(64).await, no_columns(), 16, None, None).await;

        assert!(share.open(plan).await.unwrap().is_some(), "the first opens it");
        assert!(
            share.open(plan).await.unwrap().is_some(),
            "and a later arrival draws from the same queue"
        );
    }

    /// A divided file makes every partition wait, and hands them all the queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_divided_file_is_handed_to_every_partition() {
        let share = Arc::new(FileShare::new(Mode::Divide));
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..10 {
            let share = share.clone();
            let opens = opens.clone();
            tasks.push(tokio::spawn(async move {
                share
                    .open(async || {
                        opens.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        SharedDataset::plan(dataset(64).await, no_columns(), 16, None, None)
                            .await
                    })
                    .await
                    .expect("planning succeeds")
                    .is_some()
            }));
        }

        for task in tasks {
            assert!(
                task.await.expect("the task finishes"),
                "every partition gets the dataset"
            );
        }
        assert_eq!(
            opens.load(std::sync::atomic::Ordering::Acquire),
            1,
            "and it is still opened only once"
        );
    }

    /// More partitions than subsets is not an error. The surplus find the queue
    /// empty and finish at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partition_with_nothing_left_to_pop_just_finishes() {
        const ROWS: usize = 100;

        // One chunk, eight partitions.
        let shared = SharedRead::build(dataset(ROWS).await, usize::MAX, None, true, None)
            .await
            .expect("the read builds");
        let rows = read_in_partitions(shared, 8, true).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS);
        assert_eq!(
            rows.iter().filter(|read| **read > 0).count(),
            1,
            "one chunk is read by one partition, and the rest read nothing"
        );
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

        let shared = SharedRead::build(dataset(ROWS).await, BATCH, None, true, None)
            .await
            .expect("the read builds");

        // One partition takes a single batch and leaves.
        let mut early = shared.clone().stream(None);
        let first = early.next().await.expect("one batch").expect("it reads");
        let taken = beacon_datafusion_ext::nd::decode_nd_record_batch(&first)
            .unwrap()
            .num_rows();
        drop(early);

        // The other reads what is left, and between them that is the file.
        let rest = drain(shared.stream(None)).await;
        assert_eq!(
            taken + rest,
            ROWS,
            "the subsets the leaver never popped are still there"
        );
    }
}

#[cfg(test)]
mod deal_tests {
    use datafusion::datasource::listing::PartitionedFile;
    use object_store::path::Path;

    use super::*;

    const MIN: u64 = 8 * 1024 * 1024;

    /// A file worth sharing lands in every partition, and gets a share.
    #[test]
    fn a_shared_file_lands_in_every_partition() {
        const PARTITIONS: usize = 4;

        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        let scan = share_files(&groups, PARTITIONS, Some(MIN)).expect("a large file is shared");

        assert_eq!(scan.file_groups.len(), PARTITIONS);
        for group in &scan.file_groups {
            assert_eq!(group.len(), 1, "each partition holds the file once");
            assert!(
                group.iter().next().unwrap().range.is_none(),
                "a shared file is not divided"
            );
        }
        assert!(
            scan.shares.contains_key(&Path::from("large.nc")),
            "the file the partitions share must have a share"
        );
        assert_eq!(scan.shares.len(), 1);
    }

    /// Small files are dealt one per partition, undivided and unshared.
    #[test]
    fn small_files_are_dealt_whole_and_unshared() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..4).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));

        let scan =
            share_files(&[FileGroup::new(files)], 4, Some(MIN)).expect("the large file is shared");

        let small: Vec<_> = scan
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .filter(|file| file.object_meta.location.as_ref().starts_with("small-"))
            .collect();

        assert_eq!(small.len(), 4, "each small file appears once in the scan");
        for file in small {
            assert!(file.range.is_none(), "a whole file is not divided");
        }
        assert_eq!(scan.shares.len(), 1, "only the large file is shared");
    }

    /// Every partition reads its own files before it reaches a shared one.
    ///
    /// A partition works through what it owns — which nobody else can take — and
    /// only then draws on work that is still divisible. Opening the shared file
    /// first would drain it while every partition still had its own files to go,
    /// and the end of the query would again be one partition on a file of its
    /// own.
    #[test]
    fn a_shared_file_comes_last_in_every_group() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..8).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));

        let scan =
            share_files(&[FileGroup::new(files)], 4, Some(MIN)).expect("the large file is shared");

        for group in &scan.file_groups {
            let last = group.iter().last().expect("a group holds the shared file");
            assert_eq!(
                last.object_meta.location,
                Path::from("large.nc"),
                "the shared file is opened last"
            );
        }
    }

    /// A partition reads its own files smallest first.
    ///
    /// Its total is the same whichever order it takes them in, so this buys no
    /// balance. It buys the first rows of the scan sooner.
    #[test]
    fn a_partition_reads_its_own_files_smallest_first() {
        // One partition, so every owned file lands in the same group.
        let files: Vec<_> = [4_000u64, 1_000, 3_000, 2_000]
            .iter()
            .map(|size| PartitionedFile::new(format!("f-{size}.nc"), *size))
            .collect();

        let scan = share_files(&[FileGroup::new(files)], 2, Some(MIN))
            .expect("four files over two partitions are dealt");

        for group in &scan.file_groups {
            let owned: Vec<u64> = group
                .iter()
                .filter(|file| !scan.shares.contains_key(&file.object_meta.location))
                .map(|file| file.object_meta.size)
                .collect();
            assert!(
                owned.windows(2).all(|pair| pair[0] <= pair[1]),
                "owned files ascend by size: {owned:?}"
            );
        }
    }

    /// A scan with nothing worth sharing and nowhere left to spread is left alone.
    ///
    /// This is where the size test still decides. Four files cannot fill eight
    /// partitions by dealing, so sharing is the only thing that could — and a
    /// share pays to open a file, so a file has to earn it. None of these does,
    /// and the listing has already put each in a group of its own, so there is
    /// nothing to re-deal either.
    #[test]
    fn too_few_small_files_to_deal_are_left_alone() {
        let groups: Vec<_> = (0..4)
            .map(|i| FileGroup::new(vec![PartitionedFile::new(format!("small-{i}.nc"), 1024 * 1024)]))
            .collect();

        assert!(
            share_files(&groups, 8, Some(MIN)).is_none(),
            "four 1 MB files under the minimum earn no share, and are already spread"
        );
    }

    /// The same four files bunched into one group are still spread out.
    ///
    /// Nothing here earns a share, but four partitions reading one file each
    /// beats one partition reading four.
    #[test]
    fn too_few_small_files_in_one_group_are_still_dealt() {
        let files: Vec<_> = (0..4)
            .map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024 * 1024))
            .collect();

        let scan = share_files(&[FileGroup::new(files)], 8, Some(MIN))
            .expect("four files in one group over eight partitions are dealt");
        let (owned, pooled) = check_scan(&scan, 4, 8);

        assert!(pooled.is_empty(), "nothing under the minimum is shared");
        assert_eq!(owned.len(), 4);
        assert_eq!(
            scan.file_groups.iter().filter(|g| !g.is_empty()).count(),
            4,
            "four files reach four partitions"
        );
    }

    /// A scan with files to spare is both dealt and pooled.
    ///
    /// Every file is read exactly once either way: a dealt file by the one
    /// partition holding it, a pooled file by whichever partitions reach its
    /// queue. What changes is who is allowed to take it.
    #[test]
    fn a_scan_that_can_fill_its_partitions_is_dealt_and_pooled() {
        const PARTITIONS: usize = 8;

        for count in [PARTITIONS, PARTITIONS + 1, 5_000] {
            let group = FileGroup::new(
                (0..count)
                    .map(|i| PartitionedFile::new(format!("f-{i}.nc"), 1024 * (i as u64 + 1)))
                    .collect(),
            );

            let scan = share_files(&[group], PARTITIONS, Some(MIN))
                .unwrap_or_else(|| panic!("{count} files are dealt and pooled"));
            let (owned, pooled) = check_scan(&scan, count, PARTITIONS);

            assert!(!pooled.is_empty(), "{count} files leave a pool");
            assert!(
                pooled.len() <= count / 2,
                "{count} files keep at least half dealt: {} pooled",
                pooled.len()
            );
            assert!(!owned.is_empty(), "{count} files leave something dealt");
        }
    }

    /// The pool is the largest files, and it stops at a third of the bytes.
    ///
    /// Largest, because that is where a guess made from file sizes is most wrong
    /// in absolute terms, and because a large file holds the most chunks to
    /// divide between the partitions that reach it.
    #[test]
    fn the_pool_takes_the_largest_files_up_to_a_third_of_the_scan() {
        const PARTITIONS: usize = 4;

        // 4 x 1000 + 16 x 125 = 6000; a third is 2000, which two large files
        // meet.
        let mut files: Vec<_> = (0..4)
            .map(|i| PartitionedFile::new(format!("large-{i}.nc"), 1000))
            .collect();
        files.extend((0..16).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 125)));

        let scan = share_files(&[FileGroup::new(files)], PARTITIONS, Some(MIN))
            .expect("twenty files over four partitions are dealt and pooled");
        let (_, pooled) = check_scan(&scan, 20, PARTITIONS);

        assert_eq!(pooled.len(), 2, "the pool stops once it holds a third");
        assert!(
            pooled.iter().all(|name| name.starts_with("large-")),
            "the pool is the largest files: {pooled:?}"
        );
    }

    /// The pool cannot grow without bound on a collection of small files.
    ///
    /// A pooled file costs an entry in every group, so an unbounded pool would
    /// plan `files x partitions` entries.
    #[test]
    fn the_pool_is_bounded_per_partition() {
        const PARTITIONS: usize = 8;
        const FILES: usize = 20_000;

        // All the same size, so only the bound can stop the pool: a third of the
        // bytes would otherwise be a third of the files.
        let group = FileGroup::new(
            (0..FILES)
                .map(|i| PartitionedFile::new(format!("f-{i}.nc"), 1024 * 1024))
                .collect(),
        );

        let scan = share_files(&[group], PARTITIONS, Some(MIN)).expect("20k files are pooled");
        let (_, pooled) = check_scan(&scan, FILES, PARTITIONS);

        assert_eq!(
            pooled.len(),
            PARTITIONS * 64,
            "the pool stops at sixty-four files per partition"
        );
    }

    /// Every partition starts the pool at a different file.
    ///
    /// This is the difference between a pool that pays and one that does not. A
    /// pool walked front-to-back by everybody put all 24 partitions on the same
    /// file at the same moment, and `OnceCell` blocked 23 of them while the first
    /// opened it: measured on one CORA year, `time_elapsed_opening` went from
    /// 59 s to 251 s and the query got 1.7x slower. Spreading the starts is what
    /// keeps each partition opening its own files.
    #[test]
    fn every_partition_starts_the_pool_at_a_different_file() {
        const PARTITIONS: usize = 8;

        // Twenty-four equal files: a third of the bytes is eight of them, which
        // is exactly one start per partition.
        let group = FileGroup::new(
            (0..24)
                .map(|i| PartitionedFile::new(format!("f-{i:02}.nc"), 1024 * 1024))
                .collect(),
        );

        let scan = share_files(&[group], PARTITIONS, Some(MIN)).expect("the largest are pooled");
        let (_, pooled) = check_scan(&scan, 24, PARTITIONS);
        assert_eq!(pooled.len(), PARTITIONS, "a third of twenty-four files");

        // The first pooled file each partition reaches, in group order.
        let starts: Vec<String> = scan
            .file_groups
            .iter()
            .map(|group| {
                group
                    .iter()
                    .find(|file| scan.shares.contains_key(&file.object_meta.location))
                    .expect("every group holds the pool")
                    .object_meta
                    .location
                    .to_string()
            })
            .collect();

        let distinct: std::collections::HashSet<&String> = starts.iter().collect();
        assert_eq!(
            distinct.len(),
            PARTITIONS,
            "no two partitions open the same pooled file first: {starts:?}"
        );
    }

    /// A rotated group holds the whole pool, walked twice from its own start.
    ///
    /// The rotation is a starting offset, not a subset: a partition that works
    /// through its stretch keeps going into everyone else's, which is what makes
    /// the pool absorb a slow partition rather than merely spread the fast ones.
    ///
    /// The second pass is what lets a partition read a file it was turned away
    /// from while another was opening it. Both passes start at the same offset,
    /// so the partition sweeps the pool the same way twice.
    #[test]
    fn a_rotation_drops_nothing_and_comes_back_once() {
        const PARTITIONS: usize = 6;

        let group = FileGroup::new(
            (0..30)
                .map(|i| PartitionedFile::new(format!("f-{i:02}.nc"), 1024 * (30 - i as u64)))
                .collect(),
        );

        let scan = share_files(&[group], PARTITIONS, Some(MIN)).expect("thirty files are pooled");
        let (_, pooled) = check_scan(&scan, 30, PARTITIONS);

        for group in &scan.file_groups {
            let seen: Vec<String> = group
                .iter()
                .filter(|file| scan.shares.contains_key(&file.object_meta.location))
                .map(|file| file.object_meta.location.to_string())
                .collect();

            assert_eq!(
                seen.len(),
                pooled.len() * 2,
                "the whole pool, twice over: {seen:?}"
            );
            let (first, second) = seen.split_at(pooled.len());
            assert_eq!(first, second, "and the second pass repeats the first");

            let distinct: std::collections::HashSet<&String> = first.iter().collect();
            assert_eq!(
                distinct.len(),
                pooled.len(),
                "no file twice within a pass: {first:?}"
            );
        }
    }

    /// The dealt files balance by bytes, not by count.
    ///
    /// One large file among many small ones is the shape a listing produces all
    /// the time, and a deal by count would leave the partition holding the large
    /// one running long after the others finished.
    #[test]
    fn the_deal_balances_by_size() {
        const PARTITIONS: usize = 4;

        // 4 x 400 + 24 x 100 = 4000. A third is 1333, so the pool takes the four
        // 400s and leaves 2400 in twenty-four equal files to deal four ways. A
        // deal by count would not divide those evenly.
        let mut files: Vec<_> = (0..4)
            .map(|i| PartitionedFile::new(format!("large-{i}.nc"), 400))
            .collect();
        files.extend((0..24).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 100)));

        let scan = share_files(&[FileGroup::new(files)], PARTITIONS, Some(MIN))
            .expect("twenty-eight files over four partitions are dealt and pooled");
        let (owned, pooled) = check_scan(&scan, 28, PARTITIONS);
        assert_eq!(pooled.len(), 4, "the pool is the four large files");
        assert_eq!(owned.len(), 24);

        let loads: Vec<u64> = scan
            .file_groups
            .iter()
            .map(|group| {
                group
                    .iter()
                    .filter(|file| !scan.shares.contains_key(&file.object_meta.location))
                    .map(|file| file.object_meta.size)
                    .sum()
            })
            .collect();

        assert_eq!(loads.iter().sum::<u64>(), 2_400, "everything unpooled is dealt");
        assert_eq!(loads, vec![600; PARTITIONS], "and dealt evenly: {loads:?}");
    }

    /// Pruning that empties groups still leaves every partition something to do.
    ///
    /// This is the bug the deal exists for. The listing splits its files into one
    /// contiguous group per partition, and pruning then drops the files a
    /// predicate rules out and discards the groups it empties. A listing that
    /// sorts by time and a predicate that selects a time range empty a
    /// *contiguous* run of those groups, so a year taken out of a decade leaves
    /// a handful of groups on a machine with twenty-four partitions, and the
    /// rest of the machine reads nothing.
    #[test]
    fn pruning_that_empties_groups_still_fills_every_partition() {
        const PARTITIONS: usize = 24;
        const SURVIVORS: usize = 2_900;

        // What pruning leaves: every survivor, but in four groups.
        let per_group = SURVIVORS.div_ceil(4);
        let pruned: Vec<_> = (0..4)
            .map(|g| {
                FileGroup::new(
                    (g * per_group..((g + 1) * per_group).min(SURVIVORS))
                        .map(|i| PartitionedFile::new(format!("kept-{i}.nc"), 64 * 1024 * 1024))
                        .collect(),
                )
            })
            .collect();

        let scan = share_files(&pruned, PARTITIONS, Some(MIN))
            .expect("four groups over twenty-four partitions are re-dealt");
        check_scan(&scan, SURVIVORS, PARTITIONS);

        for group in &scan.file_groups {
            assert!(!group.is_empty(), "no partition is left with nothing to do");
        }
    }

    /// A pooled file is claimed, not divided.
    ///
    /// The partition that loses the race has its own files and the rest of the
    /// pool ahead of it, so it must not wait.
    #[test]
    fn a_pooled_share_is_claimed() {
        let group = FileGroup::new(
            (0..40)
                .map(|i| PartitionedFile::new(format!("f-{i:02}.nc"), 1024 * (40 - i as u64)))
                .collect(),
        );

        let scan = share_files(&[group], 8, Some(MIN)).expect("forty files are pooled");
        assert!(!scan.shares.is_empty());
        for share in scan.shares.values() {
            assert_eq!(share.mode, Mode::Claim);
        }
    }

    /// A file shared because the scan cannot fill its partitions is divided.
    ///
    /// Waiting costs nothing there: the waiting partition has nothing else it
    /// could be doing, and dividing the queue is the only way to use it.
    #[test]
    fn a_share_that_fills_the_partitions_is_divided() {
        let group = FileGroup::new(vec![
            PartitionedFile::new("large-0.nc", 64 * 1024 * 1024),
            PartitionedFile::new("large-1.nc", 64 * 1024 * 1024),
        ]);

        let scan = share_files(&[group], 8, Some(MIN)).expect("two large files are shared");
        assert_eq!(scan.shares.len(), 2);
        for share in scan.shares.values() {
            assert_eq!(share.mode, Mode::Divide);
        }
    }

    /// The structural invariant of a dealt scan.
    ///
    /// Every file is either dealt to exactly one partition or shared into all of
    /// them, the shared files are exactly the ones with a share, and between them
    /// they account for the whole scan. A file in more than one group without a
    /// share would be read once per partition, which is the failure this guards.
    ///
    /// A shared file appears once per group per pass — twice over for a claimed
    /// pool, which every partition walks a second time to pick up the files it
    /// was turned away from. Repeating an entry costs nothing: the queue behind
    /// the share hands out each chunk once however many times it is asked.
    fn check_scan(
        scan: &SharedScan,
        expected_files: usize,
        partitions: usize,
    ) -> (std::collections::HashSet<String>, std::collections::HashSet<String>) {
        use std::collections::{HashMap, HashSet};

        assert_eq!(scan.file_groups.len(), partitions, "one group per partition");

        let passes = match scan.shares.values().next().map(|share| share.mode) {
            Some(Mode::Claim) => 2,
            _ => 1,
        };

        let mut seen: HashMap<String, usize> = HashMap::new();
        for group in &scan.file_groups {
            for file in group.iter() {
                *seen
                    .entry(file.object_meta.location.to_string())
                    .or_default() += 1;
            }
        }

        let mut owned = HashSet::new();
        let mut pooled = HashSet::new();
        for (name, count) in &seen {
            let shared = scan.shares.contains_key(&Path::from(name.as_str()));
            if shared {
                assert_eq!(
                    *count,
                    partitions * passes,
                    "{name} is shared, so every group holds it once per pass"
                );
                pooled.insert(name.clone());
            } else {
                assert_eq!(*count, 1, "{name} has no share, so one partition owns it");
                owned.insert(name.clone());
            }
        }

        assert_eq!(pooled.len(), scan.shares.len(), "a share per shared file");
        assert_eq!(
            owned.len() + pooled.len(),
            expected_files,
            "every file is accounted for"
        );
        (owned, pooled)
    }

    /// A single-partition scan shares nothing: there is nobody to share with.
    #[test]
    fn one_partition_is_left_alone() {
        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        assert!(share_files(&groups, 1, Some(MIN)).is_none());
    }

    /// Several large files are each shared by every partition.
    #[test]
    fn every_large_file_is_shared_by_every_partition() {
        const PARTITIONS: usize = 3;

        let files: Vec<_> = (0..2)
            .map(|i| PartitionedFile::new(format!("large-{i}.nc"), 64 * 1024 * 1024))
            .collect();

        let scan = share_files(&[FileGroup::new(files)], PARTITIONS, Some(MIN))
            .expect("both files are shared");

        assert_eq!(scan.shares.len(), 2);
        for group in &scan.file_groups {
            assert_eq!(group.len(), 2, "every partition holds both files");
        }
    }
}
