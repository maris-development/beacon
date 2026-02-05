use arrow::array::RecordBatch;

pub type CacheKey = (object_store::path::Path, usize); // (array path, batch index)

/// Async cache for Arrow IPC `RecordBatch` values keyed by (path, batch index).
#[derive(Debug)]
pub struct IoCache {
    inner_cache: moka::future::Cache<CacheKey, RecordBatch>,
}

impl IoCache {
    /// Create a new cache with a maximum byte capacity.
    pub fn new(max_bytes_size: usize) -> Self {
        Self {
            inner_cache: moka::future::Cache::builder()
                .weigher(|_, v: &RecordBatch| v.get_array_memory_size() as u32)
                .max_capacity(max_bytes_size as u64)
                .build(),
        }
    }

    /// Fetches a batch from the cache or inserts it using `fetch_fn`.
    ///
    /// The `fetch_fn` is only called when the key is missing.
    ///
    /// # Errors
    /// Returns the error produced by `fetch_fn`.
    pub async fn get_or_insert_with<F, Fut>(
        &self,
        key: CacheKey,
        fetch_fn: F,
    ) -> anyhow::Result<RecordBatch>
    where
        F: FnOnce(CacheKey) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<RecordBatch>>,
    {
        let cache_key = key.clone();
        self.inner_cache
            .try_get_with(cache_key, async move { fetch_fn(key).await })
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{IoCache, CacheKey};

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[tokio::test]
    async fn get_or_insert_with_caches_results() -> anyhow::Result<()> {
        let cache = IoCache::new(1024 * 1024);
        let key: CacheKey = (object_store::path::Path::from("array.arrow"), 0);
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = counter.clone();
        let batch1 = cache
            .get_or_insert_with(key.clone(), move |_| {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(make_batch())
                }
            })
            .await?;

        let counter_clone = counter.clone();
        let batch2 = cache
            .get_or_insert_with(key, move |_| {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(make_batch())
                }
            })
            .await?;

        assert_eq!(batch1.num_rows(), 3);
        assert_eq!(batch2.num_rows(), 3);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
