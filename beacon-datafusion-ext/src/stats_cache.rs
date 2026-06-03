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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::{
    common::Statistics,
    execution::cache::{
        CacheAccessor,
        cache_manager::{CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry},
    },
};
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
    inner: Cache<Path, CachedFileMetadata>,
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
            .map(|(path, cached)| {
                ((*path).clone(), cached.meta.clone(), Arc::clone(&cached.statistics))
            })
            .collect()
    }

    /// Returns `None` if the entry does not exist or the file has changed
    /// (size or last-modified mismatch).
    pub fn get_with_extra(&self, k: &Path, e: &ObjectMeta) -> Option<Arc<Statistics>> {
        self.inner
            .get(k)
            .filter(|cached| cached.is_valid_for(e))
            .map(|cached| Arc::clone(&cached.statistics))
    }

    /// Insert `value` keyed by `key`, validating against the file metadata `e`.
    /// Returns the previously cached statistics, if any.
    pub fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Statistics>,
        e: &ObjectMeta,
    ) -> Option<Arc<Statistics>> {
        let old = self.inner.get(key).map(|cached| Arc::clone(&cached.statistics));
        self.inner
            .insert(key.clone(), CachedFileMetadata::new(e.clone(), value, None));
        old
    }
}

// ─── DataFusion CacheAccessor / FileStatisticsCache impls ────────────────────

impl CacheAccessor<Path, CachedFileMetadata> for BeaconFileStatisticsCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadata> {
        self.inner.get(k)
    }

    fn put(&self, key: &Path, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        let old = self.inner.get(key);
        self.inner.insert(key.clone(), value);
        old
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadata> {
        let old = self.inner.get(k);
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

impl FileStatisticsCache for BeaconFileStatisticsCache {
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry> {
        self.inner
            .iter()
            .map(|(path, cached)| {
                (
                    (*path).clone(),
                    FileStatisticsCacheEntry {
                        object_meta: cached.meta.clone(),
                        num_rows: cached.statistics.num_rows,
                        num_columns: cached.statistics.column_statistics.len(),
                        table_size_bytes: cached.statistics.total_byte_size,
                        statistics_size_bytes: 0,
                        has_ordering: cached.ordering.is_some(),
                    },
                )
            })
            .collect()
    }
}
