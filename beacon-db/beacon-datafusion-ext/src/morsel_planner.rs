use std::{
    collections::VecDeque,
    fmt::Debug,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crossbeam::{
    deque::{Steal, Stealer, Worker},
    queue::ArrayQueue,
};
use datafusion::{datasource::listing::PartitionedFile, error::Result};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::nd::NdRecordBatch;

/// One unit of scan work that produces one batch.
pub trait Morsel: Send + Debug {
    /// Read the data of this morsel.
    ///
    /// # Errors
    ///
    /// Returns the error of the read.
    fn produce(self: Box<Self>) -> BoxFuture<'static, Result<NdRecordBatch>>;
}

/// Cuts one file into morsels.
pub trait MorselPlanner: Send + Sync {
    /// Plan the morsels of `file`.
    ///
    /// # Errors
    ///
    /// Returns the error of the open or of the plan.
    fn plan(
        self: Arc<Self>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, Result<Vec<Box<dyn Morsel>>>>;
}

/// The work of one scan, shared by all its partitions.
///
/// Each partition reads through its own [`PartitionMorsels`], in two phases:
/// 1. Local: it plans small files from a shared pool and keeps their morsels to itself.
/// 2. Shared: it plans large files into its own queue, which the other partitions can steal from.
///
/// Each partition can have only one handle. Clones share the same queues.
#[derive(Clone)]
pub struct MorselPlannerQueue {
    inner: Arc<SharedMorsels>,
}

/// The owner end of one partition's queue of shared morsels.
type MorselWorker = Worker<Box<dyn Morsel>>;

struct SharedMorsels {
    planner: Arc<dyn MorselPlanner>,
    local_files: ArrayQueue<PartitionedFile>, // Files whose morsels stay with the partition that plans them
    shared_files: ArrayQueue<PartitionedFile>, // Files whose morsels other partitions can steal
    morsels: Vec<Stealer<Box<dyn Morsel>>>, // Stealers of each partition's shared morsels; the owner pops the other end
    workers: Mutex<Vec<Option<MorselWorker>>>, // Owner ends, until `partition` hands them out
    planning: AtomicUsize,                  // Partitions that hold a claim on `shared_files`
    planned: Notify, // Wakes waiters when a plan ends or `planning` drops to zero
}

impl MorselPlannerQueue {
    /// Create the queue of one scan.
    ///
    /// Callers put small files in `local_files` and large files in `shared_files`.
    ///
    /// # Arguments
    ///
    /// * `local_files` - Files whose morsels stay with the partition that plans them
    /// * `shared_files` - Files whose morsels other partitions can steal
    /// * `planner` - The planner for both lists of files
    /// * `num_partitions` - The number of partitions that read from this queue
    pub fn new(
        local_files: Vec<PartitionedFile>,
        shared_files: Vec<PartitionedFile>,
        planner: Arc<dyn MorselPlanner>,
        num_partitions: usize,
    ) -> Self {
        // LIFO: the owner pops at the back, and thieves steal at the front.
        let workers: Vec<_> = (0..num_partitions).map(|_| Worker::new_lifo()).collect();
        Self {
            inner: Arc::new(SharedMorsels {
                planner,
                local_files: filled_queue(local_files),
                shared_files: filled_queue(shared_files),
                morsels: workers.iter().map(Worker::stealer).collect(),
                workers: Mutex::new(workers.into_iter().map(Some).collect()),
                planning: AtomicUsize::new(0),
                planned: Notify::new(),
            }),
        }
    }

    /// Create the handle through which `partition` reads its morsels.
    ///
    /// # Panics
    ///
    /// Panics if `partition` is not below `num_partitions`, or if `partition` already has a handle
    pub fn partition(&self, partition: usize) -> PartitionMorsels {
        let count = self.inner.morsels.len();
        assert!(
            partition < count,
            "partition {partition} is not below {count}"
        );
        // TODO: keep the queue in a OnceLock that `reset_state` replaces; a re-run panics here.
        // WITH RECURSIVE re-executes partition 0 of its recursive term on each iteration.
        let worker = self.inner.workers.lock()[partition]
            .take()
            .unwrap_or_else(|| panic!("partition {partition} already has a handle"));
        PartitionMorsels {
            queue: self.clone(),
            partition,
            local: VecDeque::new(),
            worker,
        }
    }
}

/// The view of one partition on a [`PlannerQueue`].
///
/// It holds the morsels of the small files that this partition planned.
/// No other partition can reach them.
///
/// The methods take `&mut self`: `Worker` is not `Sync`, so a future that holds `&self` is not `Send`.
pub struct PartitionMorsels {
    queue: MorselPlannerQueue,
    partition: usize,
    local: VecDeque<Box<dyn Morsel>>, // Morsels of local files; only this handle reads them
    worker: MorselWorker,             // Owner end of this partition's shared morsels
}

impl PartitionMorsels {
    /// Take the next morsel of this partition.
    ///
    /// The order is: own local morsels, then a new local file, then the shared phase.
    ///
    /// # Returns
    ///
    /// `Some(Ok(morsel))` for work, `Some(Err(_))` when a plan fails, and `None` when the scan is done
    pub async fn next_morsel(&mut self) -> Option<Result<Box<dyn Morsel>>> {
        loop {
            if let Some(morsel) = self.local.pop_front() {
                return Some(Ok(morsel));
            }
            let shared = &*self.queue.inner;
            let Some(file) = shared.local_files.pop() else {
                return self.next_shared().await;
            };
            // No claim here: other partitions cannot steal these morsels, so they never wait for them.
            match Arc::clone(&shared.planner).plan(file).await {
                Ok(morsels) => self.local.extend(morsels),
                Err(error) => return Some(Err(error)),
            }
        }
    }

    /// Take the next stealable morsel.
    ///
    /// The order is: the own queue, then a new plan, then a steal.
    /// While another partition plans, this waits for its morsels.
    async fn next_shared(&mut self) -> Option<Result<Box<dyn Morsel>>> {
        loop {
            if let Some(morsel) = self.worker.pop() {
                return Some(Ok(morsel));
            }
            match self.plan_next().await {
                Some(Ok(())) => continue,
                Some(Err(error)) => return Some(Err(error)),
                None => {}
            }

            let shared = &*self.queue.inner;
            // Register for the wake before the checks, so a wake between them is not lost.
            let mut planned = pin!(shared.planned.notified());
            planned.as_mut().enable();
            // A plan pushes its morsels before it releases its claim, so read the claims first.
            let in_flight = shared.planning.load(Ordering::SeqCst) > 0;
            if let Some(morsel) = self.steal() {
                return Some(Ok(morsel));
            }
            if !in_flight {
                return None;
            }
            planned.await;
        }
    }

    /// Plan the next shared file into the own queue.
    ///
    /// Returns `None` when no file is left to plan.
    async fn plan_next(&mut self) -> Option<Result<()>> {
        let shared = &*self.queue.inner;
        // Claim before the pop, so no moment shows "no files left and no plans in flight".
        let mut claim = PlanningClaim::new(shared);
        let file = shared.shared_files.pop()?;
        claim.wake_always = true;
        let morsels = Arc::clone(&shared.planner).plan(file).await;
        let worker = &self.worker;
        // Reverse, so the owner pops chunk 0 first and thieves take the last chunk.
        Some(morsels.map(|morsels| morsels.into_iter().rev().for_each(|m| worker.push(m))))
    }

    /// Take a morsel from the queue of another partition.
    fn steal(&self) -> Option<Box<dyn Morsel>> {
        let stealers = &self.queue.inner.morsels;
        // Start at the next partition so thieves spread over victims instead of all hitting partition 0.
        let ring = || {
            (1..stealers.len())
                .map(|offset| stealers[(self.partition + offset) % stealers.len()].steal())
        };
        loop {
            // `collect` gives the first success, else `Retry` if a steal lost a race, else `Empty`.
            let steal: Steal<_> = ring().collect();
            if !steal.is_retry() {
                return steal.success();
            }
        }
    }
}

/// A partition's claim on `shared_files`, released on drop.
///
/// `Drop` also runs when the caller cancels the future, so waiters never hang on a dead plan.
struct PlanningClaim<'a> {
    shared: &'a SharedMorsels,
    wake_always: bool, // True once the claim holds a file, as its end can bring new morsels
}

impl<'a> PlanningClaim<'a> {
    fn new(shared: &'a SharedMorsels) -> Self {
        shared.planning.fetch_add(1, Ordering::SeqCst);
        Self {
            shared,
            wake_always: false,
        }
    }
}

impl Drop for PlanningClaim<'_> {
    fn drop(&mut self) {
        let last = self.shared.planning.fetch_sub(1, Ordering::SeqCst) == 1;
        // An empty claim wakes only on the last release, so idle waiters do not wake each other.
        if self.wake_always || last {
            self.shared.planned.notify_waiters();
        }
    }
}

/// Put `items` in an `ArrayQueue` of the same capacity.
fn filled_queue<T>(items: Vec<T>) -> ArrayQueue<T> {
    // `ArrayQueue` will not take a capacity of zero, and a scan can have no files.
    let queue = ArrayQueue::new(items.len().max(1));
    for item in items {
        // `push` returns the item back when the queue is full; the capacity makes that impossible.
        let _ = queue.push(item);
    }
    queue
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::error::DataFusionError;

    use super::*;

    #[derive(Debug)]
    #[expect(dead_code, reason = "tests read the fields through Debug")]
    struct TestMorsel(String, usize);

    impl Morsel for TestMorsel {
        fn produce(self: Box<Self>) -> BoxFuture<'static, Result<NdRecordBatch>> {
            Box::pin(async { Err(DataFusionError::Internal("test morsel".to_string())) })
        }
    }

    /// Plans `count` morsels per file, or fails when `count` is `None`.
    /// With a `gate`, each plan waits for one permit of it.
    struct TestPlanner {
        count: Option<usize>,
        gate: Option<Arc<Notify>>,
    }

    impl MorselPlanner for TestPlanner {
        fn plan(
            self: Arc<Self>,
            file: PartitionedFile,
        ) -> BoxFuture<'static, Result<Vec<Box<dyn Morsel>>>> {
            Box::pin(async move {
                if let Some(gate) = &self.gate {
                    gate.notified().await;
                }
                let count = self
                    .count
                    .ok_or_else(|| DataFusionError::Internal("planning failed".to_string()))?;
                let path = file.object_meta.location.as_ref().to_string();
                Ok((0..count)
                    .map(|id| Box::new(TestMorsel(path.clone(), id)) as Box<dyn Morsel>)
                    .collect())
            })
        }
    }

    /// The file counts and planner of one test queue.
    struct Setup {
        local: usize,
        shared: usize,
        count: Option<usize>,
        partitions: usize,
        gate: Option<Arc<Notify>>,
    }

    impl Setup {
        fn new(local: usize, shared: usize, count: Option<usize>, partitions: usize) -> Self {
            Self {
                local,
                shared,
                count,
                partitions,
                gate: None,
            }
        }

        fn gated(mut self, gate: &Arc<Notify>) -> Self {
            self.gate = Some(Arc::clone(gate));
            self
        }

        fn build(self) -> MorselPlannerQueue {
            let files = |prefix: &str, count: usize| {
                (0..count)
                    .map(|i| PartitionedFile::new(format!("{prefix}_{i}.nc"), 1))
                    .collect()
            };
            MorselPlannerQueue::new(
                files("local", self.local),
                files("shared", self.shared),
                Arc::new(TestPlanner {
                    count: self.count,
                    gate: self.gate,
                }),
                self.partitions,
            )
        }
    }

    /// The `Debug` text of the morsel `id` of `file`.
    fn morsel(file: &str, id: usize) -> Option<String> {
        Some(format!("{:?}", TestMorsel(file.to_string(), id)))
    }

    /// Call `next_morsel` with a timeout, and return the morsel as its `Debug` text.
    async fn next(partition: &mut PartitionMorsels) -> Option<Result<String>> {
        tokio::time::timeout(Duration::from_secs(5), partition.next_morsel())
            .await
            .expect("next_morsel must not hang")
            .map(|result| result.map(|morsel| format!("{morsel:?}")))
    }

    async fn next_id(partition: &mut PartitionMorsels) -> Option<String> {
        next(partition)
            .await
            .map(|result| result.expect("no plan error"))
    }

    #[tokio::test]
    async fn new_accepts_empty_file_lists() {
        let mut p0 = Setup::new(0, 0, Some(1), 2).build().partition(0);

        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "partition 2 is not below 2")]
    async fn partition_rejects_an_index_out_of_range() {
        Setup::new(0, 0, Some(1), 2).build().partition(2);
    }

    #[tokio::test]
    async fn shared_files_plan_into_the_own_queue_first() {
        let mut p0 = Setup::new(0, 1, Some(2), 2).build().partition(0);

        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 0));
        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 1));
        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn every_shared_file_is_planned() {
        let mut p0 = Setup::new(0, 3, Some(1), 1).build().partition(0);

        for _ in 0..3 {
            assert!(next_id(&mut p0).await.is_some());
        }
        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn empty_plans_are_skipped() {
        let mut p0 = Setup::new(2, 2, Some(0), 1).build().partition(0);

        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn shared_plan_error_reaches_the_caller() {
        let mut p0 = Setup::new(0, 1, None, 1).build().partition(0);

        assert!(matches!(next(&mut p0).await, Some(Err(_))));
        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn shared_morsels_are_stolen_when_no_file_is_left() {
        let queue = Setup::new(0, 1, Some(2), 3).build();
        let (mut p0, mut p2) = (queue.partition(0), queue.partition(2));

        assert_eq!(next_id(&mut p2).await, morsel("shared_0.nc", 0));

        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 1));
        assert!(next(&mut p2).await.is_none());
    }

    #[tokio::test]
    async fn owner_reads_shared_morsels_in_order() {
        let mut p0 = Setup::new(0, 1, Some(3), 1).build().partition(0);

        for id in 0..3 {
            assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", id));
        }
    }

    #[tokio::test]
    async fn thief_takes_the_far_end_of_the_queue() {
        let queue = Setup::new(0, 1, Some(3), 2).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));
        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 0));

        assert_eq!(next_id(&mut p1).await, morsel("shared_0.nc", 2));

        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 1));
    }

    #[tokio::test]
    #[should_panic(expected = "partition 0 already has a handle")]
    async fn partition_gives_one_handle_per_partition() {
        let queue = Setup::new(0, 0, Some(1), 1).build();
        let _first = queue.partition(0);

        queue.partition(0);
    }

    #[tokio::test]
    async fn shared_morsels_outlive_their_owner() {
        let queue = Setup::new(0, 1, Some(2), 2).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));
        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 0));

        drop(p0);

        assert_eq!(next_id(&mut p1).await, morsel("shared_0.nc", 1));
    }

    #[test]
    fn next_morsel_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let mut p0 = Setup::new(0, 0, Some(1), 1).build().partition(0);

        assert_send(&p0.next_morsel());
    }

    #[tokio::test]
    async fn shared_phase_waits_for_a_plan_in_flight() {
        let gate = Arc::new(Notify::new());
        let queue = Setup::new(0, 1, Some(2), 2).gated(&gate).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));
        // The test runtime has one thread, so tasks run in spawn order at each yield.
        let planner = tokio::spawn(async move { next_id(&mut p0).await });
        let thief = tokio::spawn(async move { next_id(&mut p1).await });
        tokio::task::yield_now().await;

        gate.notify_one();

        assert_eq!(
            planner.await.expect("planner task"),
            morsel("shared_0.nc", 0)
        );
        assert_eq!(thief.await.expect("thief task"), morsel("shared_0.nc", 1));
    }

    #[tokio::test]
    async fn shared_phase_ends_when_a_plan_in_flight_is_cancelled() {
        let gate = Arc::new(Notify::new());
        let queue = Setup::new(0, 1, Some(1), 2).gated(&gate).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));
        let planner = tokio::spawn(async move { next(&mut p0).await.is_some() });
        tokio::task::yield_now().await;

        planner.abort();

        assert!(planner.await.is_err());
        assert!(next(&mut p1).await.is_none());
    }

    #[tokio::test]
    async fn local_files_come_before_shared_files() {
        let mut p0 = Setup::new(1, 1, Some(1), 1).build().partition(0);

        assert_eq!(next_id(&mut p0).await, morsel("local_0.nc", 0));
        assert_eq!(next_id(&mut p0).await, morsel("shared_0.nc", 0));
        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn local_morsels_are_not_stolen() {
        let queue = Setup::new(1, 0, Some(2), 2).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));

        assert_eq!(next_id(&mut p0).await, morsel("local_0.nc", 0));

        assert!(next(&mut p1).await.is_none());
        assert_eq!(next_id(&mut p0).await, morsel("local_0.nc", 1));
    }

    #[tokio::test]
    async fn local_files_are_a_shared_pool() {
        let queue = Setup::new(2, 0, Some(1), 2).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));

        let first = next_id(&mut p0).await;
        let second = next_id(&mut p1).await;

        assert_ne!(first, second);
        assert!(next(&mut p0).await.is_none());
        assert!(next(&mut p1).await.is_none());
    }

    #[tokio::test]
    async fn local_plan_error_does_not_end_the_scan() {
        let mut p0 = Setup::new(1, 1, None, 1).build().partition(0);

        assert!(matches!(next(&mut p0).await, Some(Err(_))));
        assert!(matches!(next(&mut p0).await, Some(Err(_))));
        assert!(next(&mut p0).await.is_none());
    }

    #[tokio::test]
    async fn local_plans_do_not_hold_up_other_partitions() {
        let gate = Arc::new(Notify::new());
        let queue = Setup::new(1, 0, Some(1), 2).gated(&gate).build();
        let (mut p0, mut p1) = (queue.partition(0), queue.partition(1));
        let planner = tokio::spawn(async move { next_id(&mut p0).await });
        tokio::task::yield_now().await;

        assert!(next(&mut p1).await.is_none());

        gate.notify_one();
        assert_eq!(
            planner.await.expect("planner task"),
            morsel("local_0.nc", 0)
        );
    }
}
