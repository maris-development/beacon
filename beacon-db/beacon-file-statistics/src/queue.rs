//! The bounded queue between discovery and the analyzer workers.

use std::collections::HashSet;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use parking_lot::Mutex;

use crate::stamp::Stamp;

/// One object version to analyze.
#[derive(Debug, Clone)]
pub struct AnalyzeTask {
    /// The stamp of the object version.
    pub stamp: Stamp,
    /// The store that holds the object.
    pub store_url: ObjectStoreUrl,
    /// The object, with a location relative to `store_url`.
    pub object: ObjectMeta,
}

impl AnalyzeTask {
    /// The task for `object` in the store at `store_url`.
    pub fn new(store_url: ObjectStoreUrl, object: ObjectMeta) -> Self {
        Self {
            stamp: Stamp::new(&store_url, &object),
            store_url,
            object,
        }
    }
}

/// A bounded queue of analyze tasks, with each stamp at most once.
///
/// A stamp stays queued from [`AnalyzeQueue::enqueue`] until
/// [`AnalyzeQueue::finish`], so a walk does not queue a file that a worker
/// analyzes now.
#[derive(Debug)]
pub struct AnalyzeQueue {
    sender: flume::Sender<AnalyzeTask>,
    receiver: flume::Receiver<AnalyzeTask>,
    queued: Mutex<HashSet<Stamp>>,
}

impl AnalyzeQueue {
    /// A queue that holds at most `capacity` tasks. Zero gives one.
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = flume::bounded(capacity.max(1));
        Self {
            sender,
            receiver,
            queued: Mutex::new(HashSet::new()),
        }
    }

    /// Adds `task`, and waits while the queue is full.
    ///
    /// # Returns
    ///
    /// `false` when the stamp is queued already.
    pub async fn enqueue(&self, task: AnalyzeTask) -> bool {
        if !self.queued.lock().insert(task.stamp) {
            return false;
        }
        // A cancelled wait must release the stamp, or it stays queued forever.
        let mut pending = Pending {
            queued: &self.queued,
            stamp: Some(task.stamp),
        };
        self.sender
            .send_async(task)
            .await
            .expect("the queue holds a receiver, so the channel stays open");
        pending.stamp = None;
        true
    }

    /// Takes the next task, and waits while the queue is empty.
    pub async fn next(&self) -> AnalyzeTask {
        self.receiver
            .recv_async()
            .await
            .expect("the queue holds a sender, so the channel stays open")
    }

    /// Takes the next task, if there is one.
    #[cfg(test)]
    pub(crate) fn try_next(&self) -> Option<AnalyzeTask> {
        self.receiver.try_recv().ok()
    }

    /// Marks the analysis of `stamp` as done, so a later walk can queue it again.
    pub fn finish(&self, stamp: Stamp) {
        self.queued.lock().remove(&stamp);
    }

    /// The number of stamps that are queued or in the analysis.
    pub fn len(&self) -> usize {
        self.queued.lock().len()
    }

    /// Whether no stamp is queued or in the analysis.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Releases a stamp when it is dropped with the stamp still set.
struct Pending<'a> {
    queued: &'a Mutex<HashSet<Stamp>>,
    stamp: Option<Stamp>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Some(stamp) = self.stamp {
            self.queued.lock().remove(&stamp);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::path::Path;

    use super::*;

    pub(crate) fn task(path: &str) -> AnalyzeTask {
        AnalyzeTask::new(
            ObjectStoreUrl::parse("memory://").expect("a valid store url"),
            ObjectMeta {
                location: Path::from(path),
                last_modified: chrono::DateTime::from_timestamp_millis(1_000)
                    .expect("a valid time"),
                size: 1,
                e_tag: None,
                version: None,
            },
        )
    }

    #[tokio::test]
    async fn a_stamp_is_queued_once_until_it_finishes() {
        let queue = AnalyzeQueue::new(4);

        assert!(queue.enqueue(task("a.nc")).await);
        assert!(!queue.enqueue(task("a.nc")).await);
        let taken = queue.next().await;
        assert!(
            !queue.enqueue(task("a.nc")).await,
            "a task in the analysis is still queued"
        );

        queue.finish(taken.stamp);
        assert!(queue.enqueue(task("a.nc")).await);
    }

    #[tokio::test]
    async fn a_full_queue_makes_enqueue_wait() {
        // Arrange
        let queue = Arc::new(AnalyzeQueue::new(1));
        queue.enqueue(task("a.nc")).await;
        let waiting = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.enqueue(task("b.nc")).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        // Act
        queue.next().await;

        // Assert
        let queued = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the walker wakes")
            .expect("no panic");
        assert!(queued);
    }

    #[tokio::test]
    async fn a_cancelled_enqueue_releases_its_stamp() {
        let queue = Arc::new(AnalyzeQueue::new(1));
        queue.enqueue(task("a.nc")).await;
        let waiting = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.enqueue(task("b.nc")).await }
        });
        tokio::task::yield_now().await;

        waiting.abort();
        let _ = waiting.await;

        assert_eq!(queue.len(), 1, "only the task in the channel stays queued");
    }

    #[tokio::test]
    async fn a_waiting_worker_wakes_on_enqueue() {
        let queue = Arc::new(AnalyzeQueue::new(1));
        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.next().await }
        });
        tokio::task::yield_now().await;

        queue.enqueue(task("a.nc")).await;

        let taken = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("the worker wakes")
            .expect("no panic");
        assert_eq!(taken.object.location.as_ref(), "a.nc");
    }
}
