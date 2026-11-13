//! Listing of the source: the first one and the background sync.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use chrono::Utc;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use serde::Serialize;
use tokio::time::{Instant, MissedTickBehavior};

use crate::state::CacheState;

/// Result of one sync.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncReport {
    /// True when the sync listed the source and corrected the cache.
    pub synced: bool,
    /// Objects in the cache after the sync.
    pub entries: usize,
    /// Duration of the sync in milliseconds.
    pub elapsed_ms: u64,
}

impl CacheState {
    /// Make sure the index holds the store. False when the call must go to the source.
    pub(crate) async fn ensure_synced(self: &Arc<Self>) -> bool {
        if self.is_fresh() {
            Self::count(&self.counters.hits);
            return true;
        }
        Self::count(&self.counters.misses);
        let state = Arc::clone(self);
        // A detached task: a caller that stops early does not stop the listing.
        let listing = tokio::spawn(async move {
            let _guard = state.sync_lock.lock().await;
            // A different caller can complete the listing during the wait for the lock.
            state.is_fresh() || state.sync().await
        });
        listing.await.unwrap_or(false)
    }

    /// List the full source and correct the index. The caller holds `sync_lock`.
    async fn sync(self: &Arc<Self>) -> bool {
        let seq = self.next_seq();
        let listing: object_store::Result<Vec<ObjectMeta>> = self.inner.list(None).try_collect().await;
        match listing {
            Ok(objects) => {
                self.index.commit(seq, objects);
                self.mark_synced();
                self.ensure_sync_loop();
                true
            }
            Err(error) => {
                Self::count(&self.counters.sync_errors);
                tracing::warn!(%error, "store cache: listing failed, old listing stays in use");
                false
            }
        }
    }

    /// Start the background sync, one time.
    fn ensure_sync_loop(self: &Arc<Self>) {
        self.sync_task.get_or_init(|| {
            let weak = Arc::downgrade(self);
            let period = self.config.interval();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(period);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(state) = weak.upgrade() else { break };
                    state.sync_pass(false).await;
                }
            })
        });
    }

    /// Compare the index with the source.
    ///
    /// Without `wait`, this is the background sync: it stops when a listing
    /// runs, and it drops the index when no call read it for a long time.
    pub(crate) async fn sync_pass(self: &Arc<Self>, wait: bool) -> SyncReport {
        let _guard = if wait {
            self.sync_lock.lock().await
        } else {
            match self.sync_lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => return SyncReport::default(),
            }
        };
        let evict_ms = u64::try_from(self.config.evict_after.as_millis()).unwrap_or(u64::MAX);
        if !wait && self.idle_ms() > evict_ms {
            self.index.clear();
            return SyncReport::default();
        }
        let started = Instant::now();
        let report = SyncReport {
            synced: self.sync().await,
            entries: self.index.len(),
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        self.counters.last_sync_ms.store(report.elapsed_ms, Ordering::Relaxed);
        self.counters
            .last_sync
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
        report
    }
}
