use crate::{dataset_cache::DatasetCache, dataset_v2::Dataset};
use datafusion::datasource::listing::PartitionedFile;
use futures::{FutureExt, future::BoxFuture};
use object_store::ObjectStore;
use std::{fmt::Debug, sync::Arc};

/// Reads one file of an nd format into a [`Dataset`].
pub trait NdFileOpener: Send + Sync + Debug {
    /// Open `file` from `store`.
    ///
    /// # Errors
    ///
    /// Returns the error of the read, or of a file that the format cannot read.
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>>;
}

/// An [`NdFileOpener`] that keeps the datasets of another opener in a [`DatasetCache`].
///
/// A failed open is not cached.
#[derive(Debug, Clone)]
pub struct CachedNdFileOpener {
    inner: Arc<dyn NdFileOpener>,
    cache: DatasetCache,
}

impl CachedNdFileOpener {
    /// Wrap `inner`, so that it opens a file only when `cache` has no dataset for it.
    pub fn new(inner: Arc<dyn NdFileOpener>, cache: DatasetCache) -> Self {
        Self { inner, cache }
    }
}

impl NdFileOpener for CachedNdFileOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
        if let Some(dataset) = self.cache.try_get(&file.object_meta) {
            return futures::future::ready(Ok(Dataset::clone(&dataset))).boxed();
        }
        let inner = Arc::clone(&self.inner);
        let cache = self.cache.clone();
        async move {
            let object = file.object_meta.clone();
            let dataset = inner.open(store, file).await?;
            cache.insert(&object, Arc::new(dataset.clone()));
            Ok(dataset)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use object_store::memory::InMemory;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Counts its opens, and fails on a file named `bad.nc`.
    #[derive(Debug, Default)]
    struct CountingOpener {
        opens: AtomicUsize,
    }

    impl NdFileOpener for CountingOpener {
        fn open(
            &self,
            _store: Arc<dyn ObjectStore>,
            file: PartitionedFile,
        ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let name = file.object_meta.location.to_string();
            async move {
                anyhow::ensure!(name != "bad.nc", "cannot read {name}");
                Dataset::from_arrays(name, IndexMap::new(), HashSet::new())
            }
            .boxed()
        }
    }

    fn opener() -> (Arc<CountingOpener>, CachedNdFileOpener) {
        let counting = Arc::new(CountingOpener::default());
        let cached = CachedNdFileOpener::new(counting.clone(), DatasetCache::new(4));
        (counting, cached)
    }

    async fn open(opener: &CachedNdFileOpener, file: &PartitionedFile) -> anyhow::Result<Dataset> {
        opener.open(Arc::new(InMemory::new()), file.clone()).await
    }

    #[tokio::test]
    async fn a_second_open_of_a_file_uses_the_cache() {
        let (counting, cached) = opener();
        let file = PartitionedFile::new("a.nc", 0);

        open(&cached, &file).await.unwrap();
        let dataset = open(&cached, &file).await.unwrap();

        assert_eq!(dataset.name, "a.nc");
        assert_eq!(counting.opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_changed_file_opens_again() {
        let (counting, cached) = opener();
        let file = PartitionedFile::new("a.nc", 0);
        let mut changed = file.clone();
        changed.object_meta.last_modified += Duration::from_secs(1);

        open(&cached, &file).await.unwrap();
        open(&cached, &changed).await.unwrap();

        assert_eq!(counting.opens.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_open_is_not_cached() {
        let (counting, cached) = opener();
        let file = PartitionedFile::new("bad.nc", 0);

        assert!(open(&cached, &file).await.is_err());
        assert!(open(&cached, &file).await.is_err());

        assert_eq!(counting.opens.load(Ordering::SeqCst), 2);
    }
}
