//! The walkers that list each store on an interval and queue new and changed objects.

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::object_store::ObjectStoreUrl;
use futures::StreamExt;
use object_store::ObjectStore;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::state::FileStatisticsState;

/// The number of listed objects that one index lock covers.
const WALK_CHUNK: usize = 1024;

/// The wait before a walk tries a full analyzer queue again.
const QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(100);

/// Why a walk stopped before its end.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WalkError {
    /// The store listing failed.
    #[error("listing the store failed: {0}")]
    List(#[from] object_store::Error),
    /// The analyzer queue is closed, so the subsystem shuts down.
    #[error("the analyzer queue is closed")]
    SinkClosed,
}

/// What one complete walk did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WalkSummary {
    /// The number of listed objects.
    pub listed: usize,
    /// The number of objects sent to the analyzer.
    pub queued: usize,
    /// The number of index entries removed, because their objects are gone.
    pub removed: usize,
}

/// A background task that walks one store on an interval.
///
/// Drop the walker to stop the task.
#[derive(Debug)]
pub(crate) struct StoreWalker {
    walker_handle: JoinHandle<()>,
}

impl StoreWalker {
    /// Starts a walker for one store. The first walk starts at once.
    ///
    /// # Arguments
    ///
    /// * `store_url` - The url that the index keys the objects of the store with.
    /// * `store` - The store to list.
    /// * `index` - The index of known objects.
    /// * `sink` - The analyzer queue that gets the new and changed objects.
    /// * `interval` - The time from the start of one walk to the start of the next.
    /// * `handle` - The runtime that runs the walker.
    pub fn init(
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
        state: Arc<FileStatisticsState>,
        interval: Duration,
        handle: &tokio::runtime::Handle,
    ) -> Self {
        let walker_handle = handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A walk longer than the interval delays the next walk; walks never stack.
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                match walk_once(&store_url, store.as_ref(), &state).await {
                    Ok(summary) => tracing::debug!(
                        store = store_url.as_str(),
                        listed = summary.listed,
                        queued = summary.queued,
                        removed = summary.removed,
                        "walked a store"
                    ),
                    Err(WalkError::SinkClosed) => return,
                    Err(error) => tracing::warn!(
                        store = store_url.as_str(),
                        %error,
                        "a store walk stopped; the next walk tries again"
                    ),
                }
            }
        });
        Self { walker_handle }
    }
}

impl Drop for StoreWalker {
    fn drop(&mut self) {
        self.walker_handle.abort();
    }
}

pub(crate) async fn walk_once(
    store_url: &ObjectStoreUrl,
    store: &dyn ObjectStore,
    state: &FileStatisticsState,
) -> Result<WalkSummary, WalkError> {
    let mut summary = WalkSummary::default();
    let mut chunks = store.list(None).ready_chunks(WALK_CHUNK);
    while let Some(chunk) = chunks.next().await {
        let objects = chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
        for object in objects {
            summary.listed += 1;
            // `None` means the queue is full: wait for the analyzer to take tasks.
            while state
                .try_enqueue(store_url.clone(), object.clone())
                .is_none()
            {
                tokio::time::sleep(QUEUE_FULL_BACKOFF).await;
            }
        }
    }
    Ok(summary)
}
