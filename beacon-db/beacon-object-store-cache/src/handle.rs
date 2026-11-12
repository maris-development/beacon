//! The handle that reads the status of a cache and starts a sync.

use std::sync::Weak;

use crate::fill::SyncReport;
use crate::state::{CacheState, CacheStatus};

/// Handle to a cache. It does not keep the cache alive.
#[derive(Debug, Clone)]
pub struct CacheHandle(pub(crate) Weak<CacheState>);

impl CacheHandle {
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
            state.index.clear();
        }
    }
}
