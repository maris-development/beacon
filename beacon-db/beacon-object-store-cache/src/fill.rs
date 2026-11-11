//! First listing of a prefix and the periodic sync with the source.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use chrono::Utc;
use futures::{FutureExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use serde::Serialize;
use tokio::time::{Instant, MissedTickBehavior};

use crate::index::ancestors;
use crate::state::{CacheState, FillOutcome, to_path};

/// Result of one sync pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncReport {
    /// Prefixes that the pass compared with the source.
    pub synced: usize,
    /// Prefixes whose listing failed.
    pub failed: usize,
    /// Objects in the cache after the pass.
    pub entries: usize,
    /// Duration of the pass in milliseconds.
    pub elapsed_ms: u64,
}

impl CacheState {
    /// List `prefix` from the source. `None` when it holds more objects than the limit.
    async fn snapshot(&self, prefix: &str) -> object_store::Result<Option<Vec<ObjectMeta>>> {
        let path = to_path(prefix);
        let mut listing = self.inner.list(path.as_ref());
        let mut objects = Vec::new();
        while let Some(meta) = listing.try_next().await? {
            if objects.len() >= self.config.max_entries {
                return Ok(None);
            }
            objects.push(meta);
        }
        Ok(Some(objects))
    }

    /// Make sure the index holds `prefix`. False when the call must go to the source.
    pub(crate) async fn ensure_covered(self: &Arc<Self>, prefix: &str) -> bool {
        if self.is_fresh(prefix, true) {
            Self::count(&self.counters.hits);
            return true;
        }
        if self.is_too_large(prefix) {
            Self::count(&self.counters.bypassed);
            return false;
        }
        let fill = {
            let mut fills = self.fills.lock();
            if self.is_fresh(prefix, true) {
                Self::count(&self.counters.hits);
                return true;
            }
            let running = ancestors(prefix, true).find_map(|ancestor| fills.get(ancestor));
            match running {
                Some(fill) => fill.clone(),
                None => {
                    let state = Arc::clone(self);
                    let owned = prefix.to_string();
                    // A detached task: a caller that stops early does not stop the listing.
                    let task = tokio::spawn(async move { state.fill(&owned).await });
                    let fill = async move { task.await.unwrap_or(FillOutcome::Failed) }
                        .boxed()
                        .shared();
                    fills.insert(prefix.to_string(), fill.clone());
                    fill
                }
            }
        };
        Self::count(&self.counters.misses);
        fill.await == FillOutcome::Filled
    }

    async fn fill(self: &Arc<Self>, prefix: &str) -> FillOutcome {
        let seq = self.next_seq();
        let outcome = match self.snapshot(prefix).await {
            Ok(Some(objects)) => {
                let mut index = self.index.write();
                index.commit(prefix, seq, objects);
                if index.len() > self.config.max_entries {
                    FillOutcome::TooLarge
                } else {
                    index.set_cover(prefix, seq, self.now_ms());
                    FillOutcome::Filled
                }
            }
            Ok(None) => FillOutcome::TooLarge,
            Err(error) => {
                tracing::warn!(prefix, %error, "store cache: first listing failed");
                FillOutcome::Failed
            }
        };
        if outcome != FillOutcome::Filled {
            self.index.write().purge_uncovered(prefix);
        }
        if outcome == FillOutcome::TooLarge {
            tracing::warn!(
                prefix,
                max_entries = self.config.max_entries,
                "store cache: prefix is over the entry limit and stays without a cache"
            );
            self.too_large.lock().insert(prefix.to_string(), Instant::now());
        }
        self.fills.lock().remove(prefix);
        if outcome == FillOutcome::Filled {
            self.ensure_sync_loop();
        }
        outcome
    }

    /// Start the periodic sync, one time.
    fn ensure_sync_loop(self: &Arc<Self>) {
        let mut task = self.sync_task.lock();
        if task.is_some() {
            return;
        }
        let weak = Arc::downgrade(self);
        let period = self.config.interval();
        *task = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(state) = weak.upgrade() else { break };
                state.sync_pass(false).await;
            }
        }));
    }

    /// Compare each cover with the source. Without `wait`, skip when a pass runs.
    pub(crate) async fn sync_pass(&self, wait: bool) -> SyncReport {
        let _guard = if wait {
            self.sync_lock.lock().await
        } else {
            match self.sync_lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => return SyncReport::default(),
            }
        };
        let started = Instant::now();
        let evict_ms = u64::try_from(self.config.evict_after.as_millis()).unwrap_or(u64::MAX);
        let prefixes = {
            let mut index = self.index.write();
            index.evict_idle(self.now_ms().saturating_sub(evict_ms));
            index.cover_prefixes()
        };
        let mut report = SyncReport::default();
        for prefix in prefixes {
            if self.sync_cover(&prefix).await {
                report.synced += 1;
            } else {
                report.failed += 1;
            }
        }
        report.entries = self.index.read().len();
        report.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.counters.last_sync_ms.store(report.elapsed_ms, Ordering::Relaxed);
        *self.counters.last_sync.lock() = Some(Utc::now());
        report
    }

    /// List one cover again and replace its objects.
    pub(crate) async fn sync_cover(&self, prefix: &str) -> bool {
        let seq = self.next_seq();
        match self.snapshot(prefix).await {
            Ok(Some(objects)) => {
                let mut index = self.index.write();
                // The cover can be gone: evicted, cleared or merged into a parent.
                if index.cover_prefixes().iter().any(|cover| cover == prefix) {
                    index.commit(prefix, seq, objects);
                    index.set_cover(prefix, seq, self.now_ms());
                }
                true
            }
            Ok(None) => {
                self.index.write().uncover(prefix, true);
                self.too_large.lock().insert(prefix.to_string(), Instant::now());
                false
            }
            Err(error) => {
                Self::count(&self.counters.sync_errors);
                tracing::warn!(prefix, %error, "store cache: sync failed, old listing stays in use");
                false
            }
        }
    }

    /// List a folder below a cover again. The cover keeps its sync time.
    pub(crate) async fn relist(&self, prefix: &str) -> bool {
        let seq = self.next_seq();
        match self.snapshot(prefix).await {
            Ok(Some(objects)) => {
                let mut index = self.index.write();
                if index.has_cover(prefix, true) {
                    index.commit(prefix, seq, objects);
                }
                true
            }
            Ok(None) => {
                self.index.write().uncover(prefix, true);
                false
            }
            Err(error) => {
                tracing::warn!(prefix, %error, "store cache: listing for an event failed");
                false
            }
        }
    }
}
