//! Process-global file statistics cache that implements DataFusion's
//! [`CacheAccessor`] and also exposes an iteration API.
//!
//! [`DefaultFileStatisticsCache`] — DataFusion's built-in implementation —
//! uses a private `DashMap` that cannot be iterated from outside.  This
//! implementation stores the same data in a [`moka::sync::Cache`] and adds a
//! `list_entries()` method so the `view_statistics_cache` table function can
//! display everything that has been cached.
//!
//! The singleton is created on first access via [`beacon_file_statistics_cache`]
//! and must be wired into the DataFusion session by passing it to
//! `CacheManagerConfig::table_files_statistics_cache` during `RuntimeEnv`
//! construction (see `beacon-core/src/runtime.rs`).
//!
//! [`DefaultFileStatisticsCache`]: datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache

use std::sync::{Arc, OnceLock};

use datafusion::{common::Statistics, execution::cache::CacheAccessor};
use moka::sync::Cache;
use object_store::{ObjectMeta, path::Path};

static INSTANCE: OnceLock<Arc<BeaconFileStatisticsCache>> = OnceLock::new();

/// Return (or lazily create) the process-wide [`BeaconFileStatisticsCache`].
///
/// The singleton's capacity is read once from `BEACON_STATS_CACHE_CAPACITY` on
/// first call; subsequent calls ignore the env var.
pub fn beacon_file_statistics_cache() -> Arc<BeaconFileStatisticsCache> {
    INSTANCE
        .get_or_init(|| {
            let capacity = std::env::var("BEACON_STATS_CACHE_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MAX_CAPACITY);
            Arc::new(BeaconFileStatisticsCache::with_capacity(capacity))
        })
        .clone()
}

const DEFAULT_MAX_CAPACITY: u64 = 10_000;

/// A file statistics cache that also supports listing all entries.
///
/// Cache entries are invalidated on size or last-modified mismatch, matching
/// the behaviour of DataFusion's built-in `DefaultFileStatisticsCache`.
pub struct BeaconFileStatisticsCache {
    inner: Cache<Path, (ObjectMeta, Arc<Statistics>)>,
}

impl BeaconFileStatisticsCache {
    pub fn with_capacity(max_capacity: u64) -> Self {
        Self {
            inner: Cache::builder().max_capacity(max_capacity).build(),
        }
    }
}

impl Default for BeaconFileStatisticsCache {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_CAPACITY)
    }
}

impl BeaconFileStatisticsCache {
    /// Return a snapshot of every cached entry as `(path, meta, statistics)`.
    pub fn list_entries(&self) -> Vec<(Path, ObjectMeta, Arc<Statistics>)> {
        self.inner
            .iter()
            .map(|(path, (meta, stats))| ((*path).clone(), meta.clone(), Arc::clone(&stats)))
            .collect()
    }
}

// ─── CacheAccessor impl ──────────────────────────────────────────────────────

impl CacheAccessor<Path, Arc<Statistics>> for BeaconFileStatisticsCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.inner.get(k).map(|(_, stats)| stats)
    }

    /// Returns `None` if the entry does not exist or the file has changed
    /// (size or last-modified mismatch).
    fn get_with_extra(&self, k: &Path, e: &ObjectMeta) -> Option<Arc<Statistics>> {
        self.inner.get(k).and_then(|(saved_meta, stats)| {
            if saved_meta.size == e.size && saved_meta.last_modified == e.last_modified {
                Some(stats)
            } else {
                None
            }
        })
    }

    fn put(&self, _key: &Path, _value: Arc<Statistics>) -> Option<Arc<Statistics>> {
        panic!(
            "put() without ObjectMeta is not supported by BeaconFileStatisticsCache; use put_with_extra()"
        )
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Statistics>,
        e: &ObjectMeta,
    ) -> Option<Arc<Statistics>> {
        let old = self.inner.get(key).map(|(_, stats)| stats);
        self.inner.insert(key.clone(), (e.clone(), value));
        old
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
        let old = self.inner.get(k).map(|(_, stats)| stats);
        self.inner.invalidate(k);
        old
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner.contains_key(k)
    }

    fn len(&self) -> usize {
        self.inner.entry_count() as usize
    }

    fn clear(&self) {
        self.inner.invalidate_all();
    }

    fn name(&self) -> String {
        "BeaconFileStatisticsCache".to_string()
    }
}
