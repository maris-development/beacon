//! The opened-dataset cache of the Rust reader, and schema inference on top.
//!
//! An open parses the metadata of one object. A scan opens the same object
//! again for its schema, its statistics and every partition, so the parse is
//! worth keeping. [`Hdf5ReaderCache`] holds it, keyed by object path and
//! last-modified time.
//!
//! This is per-runtime state. There is no process-global cache: a clone shares
//! the underlying [`moka`] cache, and the format hands clones to the sources
//! and openers it builds.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_array::dataset::AnyDataset;
use object_store::{path::Path, ObjectMeta, ObjectStore};

/// A cache of opened HDF5 datasets, sized at construction time.
#[derive(Debug, Clone)]
pub struct Hdf5ReaderCache {
    cache: moka::future::Cache<CacheKey, Arc<AnyDataset>>,
}

impl Hdf5ReaderCache {
    /// Build a cache holding up to `capacity` opened datasets.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(capacity as u64)
                .build(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    object: Path,
    last_modified: chrono::DateTime<chrono::Utc>,
}

/// Open an HDF5 dataset through the Rust reader, optionally consulting `cache`.
///
/// When `cache` is `Some`, an entry keyed by object path + last-modified is
/// checked before opening and populated afterwards. When `None`, the dataset is
/// opened directly with no caching (a table that opted out of caching).
///
/// One file opens once, however many callers ask for it at the same time. A scan
/// that splits a file gives every partition the same key, and they all arrive
/// together. A read of the cache followed by a write would let every one of them
/// miss and open the file, which is the cost the cache exists to avoid.
/// [`moka::future::Cache::try_get_with`] admits the first caller and parks the
/// rest on its result. A failed open is not cached, so the next scan retries it.
pub async fn open_dataset(
    cache: Option<&Hdf5ReaderCache>,
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let Some(cache) = cache else {
        return crate::reader::open_dataset(store.clone(), object.location.clone()).await;
    };

    let key = CacheKey {
        object: object.location.clone(),
        last_modified: object.last_modified,
    };
    let store = store.clone();
    let location = object.location.clone();

    let dataset = cache
        .cache
        .try_get_with(key, async move {
            crate::reader::open_dataset(store, location).await.map(Arc::new)
        })
        .await
        // The error belongs to whichever caller ran the open, so every waiter
        // gets it by reference. Carry the message across.
        .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!("{e:#}"))?;

    Ok((*dataset).clone())
}

/// Fetch the Arrow schema for an HDF5 object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only include
/// variables that belong to those dimensions before deriving the Arrow schema.
/// When it is absent, a broadcast-compatible default dimension set is
/// auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`]) so
/// the schema matches what `SELECT *` can actually return.
pub async fn fetch_schema(
    cache: Option<&Hdf5ReaderCache>,
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<SchemaRef> {
    let dataset = open_dataset(cache, store, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open HDF5 dataset {} for schema inference: {e}",
            object.location
        ))
    })?;

    let dataset = if let Some(dims) = beacon_nd_array::dataset::resolve_read_dimensions(
        &dataset,
        read_dimensions,
        Some("read_hdf5"),
    ) {
        let proj = beacon_nd_array::projection::DatasetProjection {
            dimension_projection: Some(dims),
            index_projection: None,
        };
        dataset.project(&proj).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to project HDF5 dataset with dimensions: {e}"
            ))
        })?
    } else {
        dataset
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from HDF5 dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    use super::*;

    /// A store that counts the reads reaching it, and forwards everything.
    ///
    /// Every read path of [`ObjectStore`] funnels through `get_opts`, so one
    /// counter there sees the lot.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        reads: Arc<AtomicUsize>,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(location, options).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A bundled HDF5 file, behind a store that counts its reads.
    fn counting_object() -> (Arc<AtomicUsize>, Arc<dyn ObjectStore>, ObjectMeta) {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join("nested-groups.h5");
        let location = Path::from_absolute_path(&path).expect("an absolute object path");
        let file_meta = std::fs::metadata(&path).expect("the bundled file exists");

        let reads = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            reads: reads.clone(),
        });

        let object = ObjectMeta {
            location,
            last_modified: file_meta.modified().map(Into::into).unwrap_or_default(),
            size: file_meta.len(),
            e_tag: None,
            version: None,
        };

        (reads, store, object)
    }

    /// One file opens once, however many partitions ask for it at the same time.
    ///
    /// An HDF5 scan splits a file across partitions, so they all reach the cache
    /// on the same key at the same time. A read of the cache followed by a write
    /// would let every one of them miss and open the file, so the cost would
    /// grow with `target_partitions` -- worst on the large files that are worth
    /// splitting in the first place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_opens_of_one_file_read_it_once() {
        const PARTITIONS: usize = 8;

        let (reads, store, object) = counting_object();

        // What a single cold open costs.
        let cache = Hdf5ReaderCache::new(4);
        open_dataset(Some(&cache), &store, &object)
            .await
            .expect("the bundled file opens");
        let one_open = reads.swap(0, Ordering::Relaxed);
        assert!(one_open > 0, "an open must read the file");

        // The shape a split scan runs in: a cold cache, every partition asking
        // at once.
        let cache = Hdf5ReaderCache::new(4);
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..PARTITIONS {
            let cache = cache.clone();
            let store = store.clone();
            let object = object.clone();
            set.spawn(async move {
                open_dataset(Some(&cache), &store, &object)
                    .await
                    .expect("the bundled file opens");
            });
        }
        while set.join_next().await.is_some() {}

        assert_eq!(
            reads.load(Ordering::Relaxed),
            one_open,
            "{PARTITIONS} concurrent opens must read the file once, not {PARTITIONS} times"
        );
    }
}
