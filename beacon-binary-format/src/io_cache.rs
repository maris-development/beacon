//! Async cache for deduplicating Arrow partition fetches.
//!
//! Many readers share object-store backed partitions.  `ArrayIoCache` stores
//! previously materialized Arrow `RecordBatch`es keyed by partition path and
//! logical group index to avoid redundant downloads.

use std::{future::Future, sync::Arc};

use arrow::array::RecordBatch;

use crate::error::BBFError;

/// Cache key that uniquely addresses a partition group inside object storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub array_partition_path: object_store::path::Path,
    pub group_index: usize,
}

/// Thin wrapper around a moka cache that stores Arrow batches.
#[derive(Debug, Clone)]
pub struct ArrayIoCache<Key = CacheKey>
where
    Key: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    inner: Arc<moka::future::Cache<Key, Option<arrow::array::RecordBatch>>>,
}

impl<Key> ArrayIoCache<Key>
where
    Key: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    /// Create a cache configured with an approximate max-size weigher.
    pub fn new(max_size_bytes: usize) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(max_size_bytes as u64)
            .weigher(|_key: &Key, value: &Option<arrow::array::RecordBatch>| {
                value.as_ref().map_or(0, |rb| rb.get_array_memory_size()) as u32
            })
            .eviction_policy(moka::policy::EvictionPolicy::lru())
            .time_to_idle(std::time::Duration::from_secs(60))
            .build();
        Self {
            inner: Arc::new(cache),
        }
    }

    /// Fetch the cached value for `key`, or compute and insert it using
    /// `loader` when absent.
    pub async fn try_get_or_insert_with<F, Fut>(
        &self,
        key: Key,
        loader: F,
    ) -> Result<Option<RecordBatch>, BBFError>
    where
        F: FnOnce(&Key) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<RecordBatch>, BBFError>> + Send + 'static,
    {
        // moka’s “load‐through” API
        self.inner
            .try_get_with(key.clone(), {
                let key_clone = key.clone();
                async move { loader(&key_clone).await }
            })
            .await
            .map_err(|e| BBFError::Shared(Arc::new(e)))
    }
}
