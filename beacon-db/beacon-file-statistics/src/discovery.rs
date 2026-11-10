//! Discovery: list each store on an interval, and queue the object versions
//! that the store has no statistics for.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::object_store::ObjectStoreUrl;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::queue::{AnalyzeQueue, AnalyzeTask};
use crate::stamp::Stamp;
use crate::store::{StatisticsStore, StoreError};

/// The number of listed objects that one store lookup covers.
const WALK_CHUNK: usize = 1024;

/// Why a walk stopped before its end.
#[derive(Debug, thiserror::Error)]
pub enum WalkError {
    /// The store listing failed.
    #[error("listing the store failed: {0}")]
    List(#[from] object_store::Error),
    /// The statistics store failed to read or remove.
    #[error("the statistics store failed: {0}")]
    Store(#[from] StoreError),
}

/// What one complete walk did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkSummary {
    /// The number of listed objects.
    pub listed: usize,
    /// The number of objects sent to the queue.
    pub queued: usize,
    /// The number of stamps removed, because their object versions are gone.
    pub removed: usize,
}

/// What discovery needs: the queue to fill, and the store to compare with.
#[derive(Debug)]
pub struct Discovery {
    queue: Arc<AnalyzeQueue>,
    store: Arc<StatisticsStore>,
}

impl Discovery {
    /// Creates the discovery stage.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue that gets the new object versions.
    /// * `store` - The store that knows which object versions have statistics.
    pub fn new(queue: Arc<AnalyzeQueue>, store: Arc<StatisticsStore>) -> Self {
        Self { queue, store }
    }

    /// Queues each object of `objects` that the store has no statistics for.
    ///
    /// Waits while the queue is full.
    ///
    /// # Returns
    ///
    /// The number of newly queued objects.
    ///
    /// # Errors
    ///
    /// Returns an error when the store read fails.
    pub async fn queue_new(
        &self,
        store_url: &ObjectStoreUrl,
        objects: &[ObjectMeta],
    ) -> Result<usize, WalkError> {
        let (_, queued) = self.queue_unknown(store_url, objects).await?;
        Ok(queued)
    }

    /// Lists the store once, queues new object versions, and removes gone ones.
    ///
    /// A failed listing removes nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the listing or a store call fails.
    pub async fn walk_once(
        &self,
        store_url: &ObjectStoreUrl,
        object_store: &dyn ObjectStore,
    ) -> Result<WalkSummary, WalkError> {
        let mut summary = WalkSummary::default();
        let mut live: HashSet<Stamp> = HashSet::new();
        let mut chunks = object_store.list(None).ready_chunks(WALK_CHUNK);
        while let Some(chunk) = chunks.next().await {
            let objects = chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
            summary.listed += objects.len();
            let (stamps, queued) = self.queue_unknown(store_url, &objects).await?;
            summary.queued += queued;
            live.extend(stamps);
        }
        summary.removed = self.store.retain(store_url, live).await?;
        Ok(summary)
    }

    /// Queues each object of `objects` that the store does not have.
    ///
    /// # Returns
    ///
    /// The stamp of each object, and the number of newly queued objects.
    async fn queue_unknown(
        &self,
        store_url: &ObjectStoreUrl,
        objects: &[ObjectMeta],
    ) -> Result<(Vec<Stamp>, usize), WalkError> {
        let stamps: Vec<Stamp> = objects
            .iter()
            .map(|object| Stamp::new(store_url, object))
            .collect();
        let stored = self.store.contains(&stamps).await?;

        let mut queued = 0;
        for ((object, stamp), stored) in objects.iter().zip(&stamps).zip(stored) {
            if stored {
                continue;
            }
            let task = AnalyzeTask {
                stamp: *stamp,
                store_url: store_url.clone(),
                object: object.clone(),
            };
            if self.queue.enqueue(task).await {
                queued += 1;
            }
        }
        Ok((stamps, queued))
    }
}

/// A background task that walks one store on an interval.
///
/// Drop the walker to stop the task.
#[derive(Debug)]
pub(crate) struct StoreWalker {
    walker_handle: JoinHandle<()>,
    trigger: Arc<Notify>,
}

impl StoreWalker {
    /// Starts a walker for one store. The first walk starts at once.
    ///
    /// # Arguments
    ///
    /// * `store_url` - The url that the stamps of the store use.
    /// * `object_store` - The store to list.
    /// * `discovery` - The queue and the statistics store.
    /// * `interval` - The time from the start of one walk to the start of the next.
    /// * `handle` - The runtime that runs the walker.
    pub fn init(
        store_url: ObjectStoreUrl,
        object_store: Arc<dyn ObjectStore>,
        discovery: Arc<Discovery>,
        interval: Duration,
        handle: &tokio::runtime::Handle,
    ) -> Self {
        let trigger = Arc::new(Notify::new());
        let walk_trigger = Arc::clone(&trigger);
        let walker_handle = handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A walk longer than the interval delays the next walk; walks never stack.
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    () = walk_trigger.notified() => {}
                }
                match discovery.walk_once(&store_url, object_store.as_ref()).await {
                    Ok(summary) => tracing::debug!(
                        store = store_url.as_str(),
                        listed = summary.listed,
                        queued = summary.queued,
                        removed = summary.removed,
                        "walked a store"
                    ),
                    Err(error) => tracing::warn!(
                        store = store_url.as_str(),
                        %error,
                        "a store walk stopped; the next walk tries again"
                    ),
                }
            }
        });
        Self {
            walker_handle,
            trigger,
        }
    }

    /// Starts a walk now, or after the walk that runs now.
    pub fn walk_now(&self) {
        self.trigger.notify_one();
    }
}

impl Drop for StoreWalker {
    fn drop(&mut self) {
        self.walker_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::store::FileRecord;

    fn url() -> ObjectStoreUrl {
        ObjectStoreUrl::parse("memory://").expect("a valid store url")
    }

    async fn put(store: &dyn ObjectStore, path: &str, bytes: &'static [u8]) {
        store
            .put(&Path::from(path), PutPayload::from_static(bytes))
            .await
            .expect("the object is stored");
    }

    /// The stamp of the current version of `path`.
    async fn stamp_of(store: &dyn ObjectStore, path: &str) -> Stamp {
        let object = store
            .head(&Path::from(path))
            .await
            .expect("the object exists");
        Stamp::new(&url(), &object)
    }

    async fn setup() -> (Arc<AnalyzeQueue>, Arc<StatisticsStore>, Discovery) {
        let queue = Arc::new(AnalyzeQueue::new(16));
        let store = Arc::new(crate::store::tests::store().await);
        let discovery = Discovery::new(Arc::clone(&queue), Arc::clone(&store));
        (queue, store, discovery)
    }

    /// Takes each queued task, and stores it as an analyzed file.
    async fn analyze_all(queue: &AnalyzeQueue, store: &StatisticsStore) {
        let tasks: Vec<AnalyzeTask> = std::iter::from_fn(|| queue.try_next()).collect();
        let records = tasks
            .iter()
            .map(|task| FileRecord::failed(task.stamp))
            .collect();
        store.put(records).await.expect("stored");
        for task in tasks {
            queue.finish(task.stamp);
        }
    }

    #[tokio::test]
    async fn a_new_file_is_queued_once_and_not_after_its_analysis() {
        // Arrange
        let (queue, store, discovery) = setup().await;
        let objects = InMemory::new();
        put(&objects, "a.nc", b"one").await;

        // Act
        let first = discovery.walk_once(&url(), &objects).await.expect("a walk");
        let second = discovery.walk_once(&url(), &objects).await.expect("a walk");
        analyze_all(&queue, &store).await;
        let third = discovery.walk_once(&url(), &objects).await.expect("a walk");

        // Assert
        assert_eq!((first.queued, second.queued, third.queued), (1, 0, 0));
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn a_changed_file_is_queued_again_and_its_old_stamp_is_removed() {
        // Arrange
        let (queue, store, discovery) = setup().await;
        let objects = InMemory::new();
        put(&objects, "a.nc", b"one").await;
        discovery.walk_once(&url(), &objects).await.expect("a walk");
        analyze_all(&queue, &store).await;
        let old = stamp_of(&objects, "a.nc").await;

        // Act
        put(&objects, "a.nc", b"a longer content").await;
        let summary = discovery.walk_once(&url(), &objects).await.expect("a walk");

        // Assert
        assert_eq!((summary.queued, summary.removed), (1, 1));
        assert_eq!(store.contains(&[old]).await.expect("read"), [false]);
    }

    #[tokio::test]
    async fn a_deleted_file_is_removed() {
        let (queue, store, discovery) = setup().await;
        let objects = InMemory::new();
        put(&objects, "a.nc", b"one").await;
        put(&objects, "b.nc", b"two").await;
        discovery.walk_once(&url(), &objects).await.expect("a walk");
        analyze_all(&queue, &store).await;
        let stamps = [
            stamp_of(&objects, "a.nc").await,
            stamp_of(&objects, "b.nc").await,
        ];

        objects.delete(&Path::from("b.nc")).await.expect("deleted");
        let summary = discovery.walk_once(&url(), &objects).await.expect("a walk");

        assert_eq!(summary.removed, 1);
        assert_eq!(store.contains(&stamps).await.expect("read"), [true, false]);
    }

    #[tokio::test]
    async fn a_full_queue_makes_the_walk_wait() {
        // Arrange
        let queue = Arc::new(AnalyzeQueue::new(1));
        let store = Arc::new(crate::store::tests::store().await);
        let discovery = Arc::new(Discovery::new(Arc::clone(&queue), store));
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(objects.as_ref(), "a.nc", b"one").await;
        put(objects.as_ref(), "b.nc", b"two").await;

        // Act
        let walk = tokio::spawn({
            let discovery = Arc::clone(&discovery);
            let objects = Arc::clone(&objects);
            async move { discovery.walk_once(&url(), objects.as_ref()).await }
        });
        // The walk waits in `enqueue` until the queue has space.
        tokio::time::timeout(Duration::from_secs(5), async {
            while queue.len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the walk reaches the second object");
        assert!(!walk.is_finished(), "the second object waits for space");
        queue.next().await;

        // Assert
        let summary = tokio::time::timeout(Duration::from_secs(5), walk)
            .await
            .expect("the walk ends")
            .expect("no panic")
            .expect("a walk");
        assert_eq!(summary.queued, 2);
    }

    #[tokio::test]
    async fn a_dropped_walker_stops_and_walk_now_starts_a_walk() {
        let (queue, store, _) = setup().await;
        let discovery = Arc::new(Discovery::new(Arc::clone(&queue), store));
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let walker = StoreWalker::init(
            url(),
            Arc::clone(&objects),
            discovery,
            Duration::from_secs(3600),
            &tokio::runtime::Handle::current(),
        );
        // The first walk finds nothing; the next one runs only on `walk_now`.
        tokio::task::yield_now().await;
        put(objects.as_ref(), "a.nc", b"one").await;

        walker.walk_now();
        let taken = tokio::time::timeout(Duration::from_secs(5), queue.next())
            .await
            .expect("walk_now queues the new file");
        assert_eq!(taken.object.location.as_ref(), "a.nc");

        let handle = walker.walker_handle.abort_handle();
        drop(walker);
        tokio::task::yield_now().await;
        assert!(handle.is_finished());
    }
}
