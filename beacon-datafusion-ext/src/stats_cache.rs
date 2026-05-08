//! Process-global file statistics cache that implements DataFusion's
//! [`CacheAccessor`] and also exposes an iteration API.
//!
//! [`DefaultFileStatisticsCache`] — DataFusion's built-in implementation —
//! uses a private `DashMap` that cannot be iterated from outside.  This
//! implementation stores the same data in a `parking_lot::RwLock<HashMap>`
//! and adds a `list_entries()` method so the `view_statistics_cache` table
//! function can display everything that has been cached.
//!
//! The singleton is created on first access via [`beacon_file_statistics_cache`]
//! and must be wired into the DataFusion session by passing it to
//! `CacheManagerConfig::table_files_statistics_cache` during `RuntimeEnv`
//! construction (see `beacon-core/src/runtime.rs`).
//!
//! [`DefaultFileStatisticsCache`]: datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use datafusion::{common::Statistics, execution::cache::CacheAccessor};
use object_store::{ObjectMeta, path::Path};
use parking_lot::RwLock;

// ─── Singleton ───────────────────────────────────────────────────────────────

static INSTANCE: OnceLock<Arc<BeaconFileStatisticsCache>> = OnceLock::new();

/// Return (or lazily create) the process-wide [`BeaconFileStatisticsCache`].
pub fn beacon_file_statistics_cache() -> Arc<BeaconFileStatisticsCache> {
    INSTANCE
        .get_or_init(|| Arc::new(BeaconFileStatisticsCache::default()))
        .clone()
}

// ─── Cache struct ────────────────────────────────────────────────────────────

/// A file statistics cache that also supports listing all entries.
///
/// Cache entries are invalidated on size or last-modified mismatch, matching
/// the behaviour of DataFusion's built-in `DefaultFileStatisticsCache`.
#[derive(Default)]
pub struct BeaconFileStatisticsCache {
    inner: RwLock<HashMap<Path, (ObjectMeta, Arc<Statistics>)>>,
}

impl BeaconFileStatisticsCache {
    /// Return a snapshot of every cached entry as `(path, meta, statistics)`.
    pub fn list_entries(&self) -> Vec<(Path, ObjectMeta, Arc<Statistics>)> {
        self.inner
            .read()
            .iter()
            .map(|(path, (meta, stats))| (path.clone(), meta.clone(), Arc::clone(stats)))
            .collect()
    }
}

// ─── CacheAccessor impl ──────────────────────────────────────────────────────

impl CacheAccessor<Path, Arc<Statistics>> for BeaconFileStatisticsCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.inner
            .read()
            .get(k)
            .map(|(_, stats)| Arc::clone(stats))
    }

    /// Returns `None` if the entry does not exist or the file has changed
    /// (size or last-modified mismatch).
    fn get_with_extra(&self, k: &Path, e: &ObjectMeta) -> Option<Arc<Statistics>> {
        self.inner.read().get(k).and_then(|(saved_meta, stats)| {
            if saved_meta.size == e.size && saved_meta.last_modified == e.last_modified {
                Some(Arc::clone(stats))
            } else {
                None
            }
        })
    }

    fn put(&self, _key: &Path, _value: Arc<Statistics>) -> Option<Arc<Statistics>> {
        panic!("put() without ObjectMeta is not supported by BeaconFileStatisticsCache; use put_with_extra()")
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Statistics>,
        e: &ObjectMeta,
    ) -> Option<Arc<Statistics>> {
        self.inner
            .write()
            .insert(key.clone(), (e.clone(), value))
            .map(|(_, old)| old)
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
        self.inner.write().remove(k).map(|(_, stats)| stats)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner.read().contains_key(k)
    }

    fn len(&self) -> usize {
        self.inner.read().len()
    }

    fn clear(&self) {
        self.inner.write().clear();
    }

    fn name(&self) -> String {
        "BeaconFileStatisticsCache".to_string()
    }
}
