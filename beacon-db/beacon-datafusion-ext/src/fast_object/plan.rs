//! What a scan decides to read, and the pruning that decides it.
//!
//! One [`Ready`] per execution, built once behind [`Shared::ready`] and shared
//! by every partition. See the [module docs](super) for why it is shared.

use std::fmt::{self, Formatter};
use std::ops::Range;
use std::sync::Arc;

use beacon_file_stats::{FileId, FileStatsStore};
use crossbeam::queue::SegQueue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::FileRange;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::Count;
use object_store::ObjectMeta;
use tokio::sync::OnceCell;

/// One unit of work: a file, or the part of one a split assigned.
///
/// The index is into the scan's shared listing, which is what keeps this cheap.
/// A `PartitionedFile` is ~280 bytes plus a path, and at three million files
/// that is the gigabyte this scan exists to avoid; one is built at the moment a
/// file opens and dropped after.
///
/// `Part` appears only after a large file was divided across partitions. netCDF,
/// HDF5, ODV and TIFF decline splitting in their own `FileSource`, because their
/// readers cannot honour a byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    Whole(usize),
    Part(usize, FileRange),
}

impl Work {
    /// Which file in the listing this reads.
    pub fn index(&self) -> usize {
        match self {
            Work::Whole(index) | Work::Part(index, _) => *index,
        }
    }
}

/// Everything a scan needs to drop files a predicate rules out.
///
/// Built at plan time, which costs no I/O: the predicate is compiled and the
/// store handle cloned. Every read it implies happens while the scan runs.
#[derive(Clone)]
pub struct StreamPruning {
    pub store: Arc<FileStatsStore>,
    pub predicate: Arc<dyn PhysicalExpr>,
    /// The table schema the predicate is written against — not the projected
    /// one, because a column a predicate prunes on need not be selected.
    pub table_schema: SchemaRef,
}

impl fmt::Debug for StreamPruning {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamPruning")
            .field("predicate", &self.predicate)
            .finish()
    }
}

/// The shared state of one execution.
#[derive(Default)]
pub(super) struct Shared {
    /// Filled by whichever partition polls first. The rest await it.
    pub(super) ready: OnceCell<Arc<Ready>>,
}

/// The files a scan will read, and how its partitions divide them.
pub(super) enum Ready {
    /// Every partition pops from one queue, so which one reads which file is
    /// whoever asks first. A partition that finishes early takes more.
    Shared(SegQueue<Work>),
    /// Partition `k` reads `work[lanes[k]]`. Used under a limit, where a
    /// reproducible row order is worth more than balance.
    Lanes {
        work: Vec<Work>,
        lanes: Vec<Range<usize>>,
    },
}

impl Ready {
    /// The next unit of work for a partition.
    ///
    /// `lane` is that partition's remaining slice, and matters only under a
    /// limit. It is an iterator, so taking from it advances it.
    pub(super) fn next(&self, lane: &mut Range<usize>) -> Option<Work> {
        match self {
            Ready::Shared(queue) => queue.pop(),
            Ready::Lanes { work, .. } => lane.next().map(|index| work[index].clone()),
        }
    }

    /// The slice `partition` reads. Empty when the queue is shared, because
    /// then nothing is assigned in advance.
    pub(super) fn lane(&self, partition: usize) -> Range<usize> {
        match self {
            Ready::Shared(_) => 0..0,
            Ready::Lanes { lanes, .. } => lanes.get(partition).cloned().unwrap_or(0..0),
        }
    }
}

/// Split `count` items into at most `target` contiguous ranges.
///
/// The first ranges take the remainder, so no lane is empty while another has
/// two. Balance does not have to be better than this: a limited scan stops long
/// before the tail, and an unlimited one shares a queue instead.
fn even_ranges(count: usize, target: usize) -> Vec<Range<usize>> {
    if count == 0 || target == 0 {
        return Vec::new();
    }
    let lanes = target.min(count);
    let base = count / lanes;
    let extra = count % lanes;
    let mut ranges = Vec::with_capacity(lanes);
    let mut start = 0;
    for lane in 0..lanes {
        let len = base + usize::from(lane < extra);
        ranges.push(start..start + len);
        start += len;
    }
    ranges
}

/// Decide every file the scan may read, then hand them over.
///
/// Runs once per execution, behind [`Shared::ready`], so the cost is paid by
/// the first partition to poll and shared by the rest.
///
/// The whole listing is pruned in one call. Cutting it into batches and running
/// those concurrently looks like the faster shape and is not: the store already
/// reads a predicate's columns together and each column's segments in parallel,
/// and it reads only the segments covering the file-id range it is asked about.
/// Narrowing that range per batch makes every segment that spans a boundary be
/// read again for each batch touching it.
pub(super) async fn prune_all(
    objects: Arc<Vec<ObjectMeta>>,
    split: Option<Arc<Vec<Work>>>,
    pruning: Option<StreamPruning>,
    partitions: usize,
    limited: bool,
    considered: Count,
    pruned: Count,
) -> Arc<Ready> {
    let work: Vec<Work> = match split {
        Some(split) => split.as_ref().clone(),
        None => (0..objects.len()).map(Work::Whole).collect(),
    };

    let work = match pruning {
        None => work,
        Some(pruning) => {
            let before = work.len();
            let kept = prune(&objects, &pruning, work).await;
            considered.add(before);
            pruned.add(before - kept.len());
            kept
        }
    };

    Arc::new(if limited {
        Ready::Lanes {
            lanes: even_ranges(work.len(), partitions),
            work,
        }
    } else {
        let queue = SegQueue::new();
        for item in work {
            queue.push(item);
        }
        Ready::Shared(queue)
    })
}

/// Drop the files whose recorded ranges say they cannot match.
///
/// A path the registry has never seen has no statistics and is kept: a
/// partially analyzed store must not lose files. Every failure keeps the whole
/// list for the same reason, which is what makes this infallible.
async fn prune(objects: &[ObjectMeta], pruning: &StreamPruning, work: Vec<Work>) -> Vec<Work> {
    let paths: Vec<String> = work
        .iter()
        .map(|work| objects[work.index()].location.to_string())
        .collect();
    let borrowed: Vec<&str> = paths.iter().map(String::as_str).collect();
    let Ok(ids) = pruning.store.registry().file_ids(&borrowed) else {
        return work;
    };

    let mut candidates: Vec<FileId> = ids.iter().filter_map(|id| *id).collect();
    if candidates.is_empty() {
        return work; // nothing here is analyzed, so nothing is prunable
    }
    // `prune_files` wants them ascending, and answers ascending.
    candidates.sort_unstable();
    candidates.dedup();

    let kept = beacon_file_stats::prune_files(
        &pruning.store,
        &pruning.predicate,
        &pruning.table_schema,
        &candidates,
    )
    .await;

    work.into_iter()
        .zip(ids)
        .filter(|(_, id)| match id {
            Some(id) => kept.binary_search(id).is_ok(),
            None => true,
        })
        .map(|(work, _)| work)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};

    use super::*;

    fn listing(count: usize) -> Arc<Vec<ObjectMeta>> {
        Arc::new(
            (0..count)
                .map(|index| ObjectMeta {
                    location: object_store::path::Path::from(format!("obs/{index:06}.parquet")),
                    last_modified: chrono::Utc::now(),
                    size: 1_024,
                    e_tag: None,
                    version: None,
                })
                .collect(),
        )
    }

    fn counters() -> (Count, Count) {
        let metrics = ExecutionPlanMetricsSet::new();
        (
            MetricBuilder::new(&metrics).global_counter("considered"),
            MetricBuilder::new(&metrics).global_counter("pruned"),
        )
    }

    async fn ready_over(
        objects: Arc<Vec<ObjectMeta>>,
        limited: bool,
        partitions: usize,
    ) -> Arc<Ready> {
        let (considered, pruned) = counters();
        prune_all(objects, None, None, partitions, limited, considered, pruned).await
    }

    /// With no predicate the queue holds the whole listing, in listing order.
    #[tokio::test]
    async fn prune_all_keeps_path_order() {
        let ready = ready_over(listing(10_000), false, 4).await;
        let mut lane = 0..0;
        for index in 0..10_000 {
            assert_eq!(ready.next(&mut lane), Some(Work::Whole(index)));
        }
        assert_eq!(ready.next(&mut lane), None);
    }

    /// A split's pieces reach the queue as they were produced.
    #[tokio::test]
    async fn prune_all_carries_split_pieces() {
        let objects = listing(1);
        let split = Arc::new(vec![
            Work::Part(0, FileRange { start: 0, end: 512 }),
            Work::Part(
                0,
                FileRange {
                    start: 512,
                    end: 1_024,
                },
            ),
        ]);
        let (considered, pruned) = counters();
        let ready = prune_all(
            objects,
            Some(Arc::clone(&split)),
            None,
            2,
            false,
            considered,
            pruned,
        )
        .await;

        let mut lane = 0..0;
        assert_eq!(ready.next(&mut lane), Some(split[0].clone()));
        assert_eq!(ready.next(&mut lane), Some(split[1].clone()));
        assert_eq!(ready.next(&mut lane), None);
    }

    /// Every partition popping at once takes every file, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_queue_hands_out_every_item_once() {
        let ready = ready_over(listing(1_000), false, 8).await;

        let mut takers = Vec::new();
        for _ in 0..8 {
            let ready = Arc::clone(&ready);
            takers.push(tokio::spawn(async move {
                let mut mine = Vec::new();
                let mut lane = 0..0;
                while let Some(work) = ready.next(&mut lane) {
                    mine.push(work.index());
                }
                mine
            }));
        }

        let mut seen: Vec<usize> = Vec::new();
        for taker in takers {
            seen.extend(taker.await.unwrap());
        }
        assert_eq!(seen.len(), 1_000, "no file was handed out twice");
        assert_eq!(
            seen.iter().copied().collect::<HashSet<_>>().len(),
            1_000,
            "every file was handed out"
        );
    }

    /// A partition that reads slowly holds up nobody: its peers take the work
    /// it has not reached. This is the whole point of sharing the queue, so
    /// assert it where it is deterministic rather than through a file reader.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_slow_taker_does_not_strand_work() {
        let ready = ready_over(listing(1_000), false, 8).await;

        let mut takers = Vec::new();
        for taker in 0..8 {
            let ready = Arc::clone(&ready);
            takers.push(tokio::spawn(async move {
                let mut taken = 0;
                let mut lane = 0..0;
                while ready.next(&mut lane).is_some() {
                    taken += 1;
                    if taker == 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                }
                taken
            }));
        }

        let mut taken = Vec::new();
        for taker in takers {
            taken.push(taker.await.unwrap());
        }
        assert_eq!(taken.iter().sum::<usize>(), 1_000, "every file was read");
        assert!(
            taken[0] < 125,
            "a fixed split would have given the slow partition 125 files; it took {}",
            taken[0]
        );
    }

    /// Under a limit each partition gets its own contiguous slice, and the
    /// slices tile the survivors in listing order.
    #[tokio::test]
    async fn a_limited_scan_reads_contiguous_lanes() {
        let ready = ready_over(listing(100), true, 8).await;
        assert!(
            matches!(ready.as_ref(), Ready::Lanes { lanes, .. } if lanes.len() == 8),
            "a limited scan does not share"
        );

        let mut expected = 0;
        for partition in 0..8 {
            let mut lane = ready.lane(partition);
            while let Some(work) = ready.next(&mut lane) {
                assert_eq!(work.index(), expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 100, "the lanes cover every file exactly once");
    }

    #[test]
    fn even_ranges_tile_the_survivors() {
        let ranges = even_ranges(100, 8);
        assert_eq!(ranges.len(), 8);
        assert_eq!(ranges.iter().map(Range::len).sum::<usize>(), 100);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        assert!(ranges.iter().all(|range| !range.is_empty()));

        // Fewer items than partitions gives one lane each, never an empty one.
        assert_eq!(even_ranges(3, 8), vec![0..1, 1..2, 2..3]);
        assert!(even_ranges(0, 8).is_empty());
        assert!(even_ranges(10, 0).is_empty());
    }
}
