//! Change events and the handle that applies them.
//!
//! An event only tells the cache where to look. The cache reads the source
//! for each event, so a late, repeated or lost event does no damage.

use std::collections::HashSet;
use std::sync::Weak;

use futures::{StreamExt, stream};
use object_store::Error;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::fill::SyncReport;
use crate::index::is_under;
use crate::state::{CacheState, CacheStatus, head_uncached};

/// Events that run at the same time.
const APPLY_CONCURRENCY: usize = 16;

/// A report that the source changed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreEvent {
    /// One object was written, replaced or removed.
    Changed { path: String },
    /// A folder changed. An empty path is the store root.
    PrefixChanged { path: String },
    /// The sender lost events. Compare the full cache with the source.
    Resync,
}

/// Result of [`CacheHandle::apply`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ApplyReport {
    /// Events that changed or confirmed the cache.
    pub applied: usize,
    /// Events for paths that the cache does not hold.
    pub ignored: usize,
    /// Events for which the read from the source failed.
    pub failed: usize,
}

enum Outcome {
    Applied,
    Ignored,
    Failed,
}

/// Handle to a cache. It does not keep the cache alive.
#[derive(Debug, Clone)]
pub struct CacheHandle(pub(crate) Weak<CacheState>);

impl CacheHandle {
    /// Apply change events. The cache reads the source for each one.
    pub async fn apply(&self, events: Vec<StoreEvent>) -> ApplyReport {
        let mut report = ApplyReport::default();
        let Some(state) = self.0.upgrade() else {
            report.ignored = events.len();
            return report;
        };
        let mut seen = HashSet::new();
        let unique: Vec<StoreEvent> = events
            .into_iter()
            .filter(|event| seen.insert(event.clone()))
            .collect();
        let state = &state;
        let mut results = stream::iter(unique)
            .map(|event| async move { state.apply_event(event).await })
            .buffer_unordered(APPLY_CONCURRENCY);
        while let Some(result) = results.next().await {
            match result {
                Outcome::Applied => report.applied += 1,
                Outcome::Ignored => report.ignored += 1,
                Outcome::Failed => report.failed += 1,
            }
        }
        report
    }

    /// Compare the full cache with the source now.
    pub async fn sync_now(&self) -> SyncReport {
        match self.0.upgrade() {
            Some(state) => state.sync_pass(true).await,
            None => SyncReport::default(),
        }
    }

    /// Counters and sizes of the cache.
    pub fn status(&self) -> CacheStatus {
        self.0
            .upgrade()
            .map(|state| state.status())
            .unwrap_or_default()
    }

    /// Drop all cached metadata. The next listing reads the source.
    pub fn clear(&self) {
        if let Some(state) = self.0.upgrade() {
            state.index.write().clear();
            state.too_large.lock().clear();
        }
    }
}

impl CacheState {
    async fn apply_event(&self, event: StoreEvent) -> Outcome {
        match event {
            StoreEvent::Changed { path } => self.apply_changed(&path).await,
            StoreEvent::PrefixChanged { path } => self.apply_prefix(path.trim_matches('/')).await,
            StoreEvent::Resync => {
                self.sync_pass(true).await;
                Outcome::Applied
            }
        }
    }

    async fn apply_changed(&self, path: &str) -> Outcome {
        let Ok(location) = Path::parse(path) else {
            return Outcome::Failed;
        };
        let key = location.as_ref();
        if !self.index.read().has_cover(key, false) {
            return Outcome::Ignored;
        }
        let seq = self.next_seq();
        match head_uncached(self.inner.as_ref(), &location).await {
            Ok(meta) => {
                self.upsert(key, Some(meta), seq);
                Outcome::Applied
            }
            Err(Error::NotFound { .. }) => {
                self.upsert(key, None, seq);
                // The path can be a folder that moved: list it to find its objects.
                if self.relist(key).await {
                    Outcome::Applied
                } else {
                    Outcome::Failed
                }
            }
            Err(error) => {
                tracing::warn!(path = key, %error, "store cache: event read failed");
                Outcome::Failed
            }
        }
    }

    async fn apply_prefix(&self, prefix: &str) -> Outcome {
        if !prefix.is_empty() && Path::parse(prefix).is_err() {
            return Outcome::Failed;
        }
        let below: Vec<String> = self
            .index
            .read()
            .cover_prefixes()
            .into_iter()
            .filter(|cover| cover == prefix || is_under(cover, prefix))
            .collect();
        if below.is_empty() {
            if !self.index.read().has_cover(prefix, true) {
                return Outcome::Ignored;
            }
            return if self.relist(prefix).await {
                Outcome::Applied
            } else {
                Outcome::Failed
            };
        }
        let mut applied = Outcome::Applied;
        for cover in below {
            if !self.sync_cover(&cover).await {
                applied = Outcome::Failed;
            }
        }
        applied
    }
}
