//! Settings of the metadata cache.

use std::time::Duration;

/// Settings of a [`CachedObjectStore`](crate::CachedObjectStore).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreCacheConfig {
    /// Put the cache in front of the datasets store.
    pub enable: bool,
    /// Time between two comparisons of the cache with the source.
    pub sync_interval: Duration,
    /// Maximum number of objects in the cache. A larger prefix stays without a cache.
    pub max_entries: usize,
    /// Start a file system watcher for a local datasets store.
    pub watch_local: bool,
    /// Put the cache in front of remote stores that queries open by URL.
    pub remote: bool,
    /// Time the watcher collects events before it sends them.
    pub debounce: Duration,
    /// Time after which the cache drops a prefix that no call read.
    pub evict_after: Duration,
    /// Path segments that always read from the source.
    pub bypass_segments: Vec<String>,
}

impl StoreCacheConfig {
    /// Sync interval with the 1 second minimum applied.
    pub(crate) fn interval(&self) -> Duration {
        self.sync_interval.max(Duration::from_secs(1))
    }

    /// Age after which a prefix without a good sync counts as not cached.
    pub(crate) fn max_age(&self) -> Duration {
        self.interval() * 3
    }
}

impl Default for StoreCacheConfig {
    fn default() -> Self {
        Self {
            enable: false,
            sync_interval: Duration::from_secs(300),
            max_entries: 2_000_000,
            watch_local: true,
            remote: false,
            debounce: Duration::from_millis(500),
            evict_after: Duration::from_secs(3600),
            bypass_segments: vec!["_delta_log".to_string()],
        }
    }
}
