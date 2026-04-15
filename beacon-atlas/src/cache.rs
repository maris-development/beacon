use std::sync::Arc;

use crate::typed_vec::TypedVec;

#[derive(Debug, Clone)]
pub struct Cache {
    vec_cache: Arc<VecCache>,
    io_cache: Arc<IoCache>,
}

impl Cache {
    pub fn new(vec_cache_capacity: u64, _io_cache_capacity: u64) -> Self {
        Self {
            vec_cache: Arc::new(VecCache::new(vec_cache_capacity)),
            io_cache: Arc::new(IoCache {}),
        }
    }

    pub fn vec_cache(&self) -> Arc<VecCache> {
        self.vec_cache.clone()
    }

    pub fn io_cache(&self) -> Arc<IoCache> {
        self.io_cache.clone()
    }
}

#[derive(Debug, Clone)]
pub struct IoCache {}

#[derive(Debug, Clone)]
pub struct VecCache {
    inner: moka::future::Cache<VecCacheKey, TypedVec>,
}

pub type VecCacheKey = (object_store::path::Path, u32); // object path + file array chunk index

impl VecCache {
    pub fn new(capacity: u64) -> Self {
        Self {
            inner: moka::future::Cache::new(capacity),
        }
    }

    pub async fn try_get_or_insert<F, Fut>(
        &self,
        key: VecCacheKey,
        fetch_fn: F,
    ) -> anyhow::Result<TypedVec>
    where
        F: FnOnce(VecCacheKey) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<TypedVec>>,
    {
        let cache_key = key.clone();
        self.inner
            .try_get_with(cache_key, async move { fetch_fn(key).await })
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))
    }
}
