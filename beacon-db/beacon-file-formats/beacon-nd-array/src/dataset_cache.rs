use object_store::ObjectMeta;

use crate::dataset_v2::Dataset;
use std::sync::Arc;

/// A bounded cache of opened datasets, keyed by object path and modification time.
///
/// A changed object gets a new key, so the cache never returns a stale dataset.
#[derive(Debug, Clone)]
pub struct DatasetCache {
    inner: Arc<moka::sync::Cache<ObjectKey, Arc<Dataset>>>,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct ObjectKey {
    path: String,
    modified_unix: i64,
}

impl DatasetCache {
    /// Create a cache that holds at most `size` datasets.
    pub fn new(size: usize) -> Self {
        Self {
            inner: Arc::new(moka::sync::Cache::new(size as u64)),
        }
    }

    /// Return the cached dataset of `object`, or `None` when the cache has no entry for it.
    pub fn try_get(&self, object: &ObjectMeta) -> Option<Arc<Dataset>> {
        let key = ObjectKey {
            path: object.location.to_string(),
            modified_unix: object.last_modified.timestamp(),
        };
        self.inner.get(&key)
    }

    /// Store `dataset` as the dataset of `object`.
    pub fn insert(&self, object: &ObjectMeta, dataset: Arc<Dataset>) {
        let key = ObjectKey {
            path: object.location.to_string(),
            modified_unix: object.last_modified.timestamp(),
        };
        self.inner.insert(key, dataset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use std::collections::HashSet;
    use std::time::Duration;

    fn object(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: path.into(),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn dataset() -> Arc<Dataset> {
        Arc::new(Dataset::from_arrays("test".to_string(), IndexMap::new(), HashSet::new()).unwrap())
    }

    #[test]
    fn try_get_returns_the_inserted_dataset() {
        let cache = DatasetCache::new(4);
        let meta = object("a.nc");
        let inserted = dataset();

        cache.insert(&meta, inserted.clone());

        let cached = cache.try_get(&meta).unwrap();
        assert!(Arc::ptr_eq(&cached, &inserted));
        assert!(cache.try_get(&object("b.nc")).is_none());
    }

    #[test]
    fn try_get_misses_when_the_object_changes() {
        let cache = DatasetCache::new(4);
        let meta = object("a.nc");
        cache.insert(&meta, dataset());

        let mut modified = meta.clone();
        modified.last_modified += Duration::from_secs(1);

        assert!(cache.try_get(&modified).is_none());
    }
}
