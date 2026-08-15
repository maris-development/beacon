use std::sync::Arc;

use beacon_nd_array::dataset::AnyDataset;
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::object_meta_resolver::{DefaultNetCDFObjectResolver, NetCDFObjectResolver};

/// Which reader opens a NetCDF file.
///
/// The two readers produce the same dataset, so this only decides how the bytes
/// are fetched and decoded. See [`crate::oxcdf_reader`] for the trade-off.
///
/// This is the bare choice, small enough to key a cache by. [`FileAccess`] is
/// the choice plus whatever that reader needs to reach a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ReaderBackend {
    /// netcdf-c, through its Rust bindings. The default.
    #[default]
    NetcdfC,
    /// `oxcdf`, pure Rust over `object_store`.
    Oxcdf,
}

/// How a table reaches its files, and which reader opens them.
///
/// The two readers do not need the same things, so each variant carries exactly
/// what its reader needs and nothing else. That is the point of the type: a
/// format cannot hold a resolver the reader never consults, and it cannot pick
/// netcdf-c without one.
#[derive(Debug, Clone)]
pub enum FileAccess {
    /// netcdf-c opens a path itself, so it cannot work without a resolver to
    /// turn an object path into that path.
    NetcdfC {
        /// Builds the native path netcdf-c opens, from the table's root store.
        resolver: Arc<dyn NetCDFObjectResolver>,
    },
    /// `oxcdf` reads byte ranges through the scan's own object store. It needs
    /// nothing else, which is why this variant carries nothing.
    Oxcdf,
}

impl Default for FileAccess {
    /// netcdf-c with a resolver that cannot resolve anything.
    ///
    /// This is the "no location was supplied" state, which
    /// [`FileFormatFactory::create`](datafusion::datasource::file_format::FileFormatFactory::create)
    /// and [`FileFormatFactory::default`](datafusion::datasource::file_format::FileFormatFactory::default)
    /// produce. A scan on it fails per object, naming the path it could not
    /// resolve. See [`FileFormatFactoryExt::create_with_native_root`](beacon_datafusion_ext::format_ext::FileFormatFactoryExt::create_with_native_root)
    /// for the path that supplies one.
    fn default() -> Self {
        Self::NetcdfC {
            resolver: Arc::new(DefaultNetCDFObjectResolver),
        }
    }
}

impl FileAccess {
    /// netcdf-c, reading through `resolver`.
    pub fn netcdf_c(resolver: Arc<dyn NetCDFObjectResolver>) -> Self {
        Self::NetcdfC { resolver }
    }

    /// The reader this access uses.
    pub fn backend(&self) -> ReaderBackend {
        match self {
            FileAccess::NetcdfC { .. } => ReaderBackend::NetcdfC,
            FileAccess::Oxcdf => ReaderBackend::Oxcdf,
        }
    }

    /// Describe where one object's bytes come from.
    ///
    /// netcdf-c resolves the object to a native path. `oxcdf` takes the scan's
    /// store and the object path as they are, so it also reads s3, gs and az.
    /// The format, the statistics and the opener all come through here, so they
    /// can never disagree about a file.
    pub fn input_for(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<NetcdfInput> {
        match self {
            FileAccess::NetcdfC { resolver } => {
                let native_path = resolver.resolve(object).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to resolve object metadata (path) to NetCDF native path: {e}"
                    ))
                })?;
                Ok(NetcdfInput::NetcdfC(native_path))
            }
            FileAccess::Oxcdf => Ok(NetcdfInput::Oxcdf {
                store: store.clone(),
                path: object.location.clone(),
            }),
        }
    }
}

/// Where a reader gets the bytes of one NetCDF object.
///
/// The variant also names the reader: netcdf-c opens a path itself, and `oxcdf`
/// reads through the object store. Build one with [`FileAccess::input_for`].
#[derive(Debug, Clone)]
pub enum NetcdfInput {
    /// netcdf-c opens this native path directly. It is a local path, or an
    /// `http(s)` URL that carries the `#mode=bytes` suffix.
    NetcdfC(String),
    /// `oxcdf` range-reads the object through the store. No local copy is made,
    /// so this also covers s3, gs and az.
    Oxcdf {
        /// The store the object lives in.
        store: Arc<dyn ObjectStore>,
        /// The object, relative to that store.
        path: object_store::path::Path,
    },
}

impl NetcdfInput {
    /// The reader this input belongs to.
    pub fn backend(&self) -> ReaderBackend {
        match self {
            NetcdfInput::NetcdfC(_) => ReaderBackend::NetcdfC,
            NetcdfInput::Oxcdf { .. } => ReaderBackend::Oxcdf,
        }
    }

    /// The location to name in an error message.
    pub fn location(&self) -> String {
        match self {
            NetcdfInput::NetcdfC(path) => path.clone(),
            NetcdfInput::Oxcdf { path, .. } => path.to_string(),
        }
    }

    /// Open the dataset this input points at, with no caching.
    ///
    /// [`open_dataset`] adds the cache. Use this one where a cache entry has no
    /// value, such as one-off statistics.
    pub async fn open(self) -> anyhow::Result<AnyDataset> {
        match self {
            NetcdfInput::NetcdfC(path) => crate::reader::open_dataset(path).await,
            NetcdfInput::Oxcdf { store, path } => {
                crate::oxcdf_reader::open_dataset(store, path).await
            }
        }
    }
}

/// A NetCDF dataset reader cache, sized at construction time.
///
/// Cloning shares the underlying [`moka`] cache (the cache is reference-counted
/// internally), so a single cache instance is shared across the formats,
/// sources and openers that a runtime hands a clone to. This is per-runtime
/// state — there is no process-global cache.
#[derive(Debug, Clone)]
pub struct NetcdfReaderCache {
    cache: moka::future::Cache<CacheKey, Arc<AnyDataset>>,
}

impl NetcdfReaderCache {
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
    pub object: object_store::path::Path,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// The reader that produced the entry. One runtime can serve tables on
    /// either reader, and their datasets are not interchangeable, so the key
    /// keeps them apart.
    pub backend: ReaderBackend,
}

/// Open a NetCDF dataset, optionally consulting `cache`.
///
/// When `cache` is `Some`, an entry keyed by object path + last-modified is
/// checked before opening and populated afterwards. When `None`, the dataset is
/// opened directly with no caching (e.g. schema inference or a table that opted
/// out of caching).
///
/// One file opens once, however many callers ask for it at the same time. A
/// scan that splits a file gives every partition the same key, and they all
/// arrive together. A read of the cache followed by a write would let every one
/// of them miss and open the file, which is the cost the cache exists to avoid.
/// [`moka::future::Cache::try_get_with`] admits the first caller and parks the
/// rest on its result. A failed open is not cached, so the next scan retries it.
pub async fn open_dataset(
    cache: Option<&NetcdfReaderCache>,
    input: NetcdfInput,
    object: ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let Some(cache) = cache else {
        return input.open().await;
    };

    let key = CacheKey {
        object: object.location.clone(),
        last_modified: object.last_modified,
        backend: input.backend(),
    };

    let dataset = cache
        .cache
        .try_get_with(key, async move { input.open().await.map(Arc::new) })
        .await
        // The error belongs to whichever caller ran the open, so every waiter
        // gets it by reference. Carry the message across.
        .map_err(|e: Arc<anyhow::Error>| anyhow::anyhow!("{e:#}"))?;

    Ok((*dataset).clone())
}

/// Fetch the Arrow schema for a NetCDF object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only
/// include variables that belong to those dimensions before deriving the
/// Arrow schema. When it is absent, a broadcast-compatible default dimension
/// set is auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`])
/// so the schema matches
/// what `SELECT *` can actually return.
pub async fn fetch_schema(
    cache: Option<&NetcdfReaderCache>,
    input: NetcdfInput,
    object: ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    // Schema inference does not consult the reader cache; the cache benefits
    // repeated data scans, which flow through `NetCDFSource`.
    let dataset = open_dataset(cache, input, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open NetCDF dataset for schema inference: {e}"
        ))
    })?;

    let dataset = if let Some(dims) = beacon_nd_array::dataset::resolve_read_dimensions(
        &dataset,
        read_dimensions,
        Some("read_netcdf"),
    ) {
        let proj = beacon_nd_array::projection::DatasetProjection {
            dimension_projection: Some(dims),
            index_projection: None,
        };
        dataset.project(&proj).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to project NetCDF dataset with dimensions: {e}"
            ))
        })?
    } else {
        dataset
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from NetCDF dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::stream::BoxStream;
    use object_store::{
        path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
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

    /// The bundled gridded file, as a store that counts its reads.
    fn counting_input() -> (Arc<AtomicUsize>, NetcdfInput, ObjectMeta) {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join("gridded-example.nc");
        let location = object_store::path::Path::from_absolute_path(&path)
            .expect("the bundled file has an absolute path");
        let file_meta = std::fs::metadata(&path).expect("the bundled file exists");

        let reads = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            reads: reads.clone(),
        });

        let object = ObjectMeta {
            location: location.clone(),
            last_modified: file_meta.modified().map(Into::into).unwrap_or_default(),
            size: file_meta.len(),
            e_tag: None,
            version: None,
        };

        (
            reads,
            NetcdfInput::Oxcdf {
                store,
                path: location,
            },
            object,
        )
    }

    /// One file opens once, however many partitions ask for it at the same time.
    ///
    /// A scan that splits a file gives every partition the same cache key, and
    /// they all arrive on a cold cache together. A read of the cache followed by
    /// a write would let every one of them miss and open the file, so the cost
    /// would grow with `target_partitions` — worst on the large files that are
    /// worth splitting in the first place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_opens_of_one_file_read_it_once() {
        const PARTITIONS: usize = 8;

        let (reads, input, object) = counting_input();

        // What a single cold open costs.
        let cache = NetcdfReaderCache::new(4);
        open_dataset(Some(&cache), input.clone(), object.clone())
            .await
            .expect("the bundled file opens");
        let one_open = reads.swap(0, Ordering::Relaxed);
        assert!(one_open > 0, "an open must read the file");

        // The shape a split scan runs in: a cold cache, and every partition
        // asking at once.
        let cache = NetcdfReaderCache::new(4);
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..PARTITIONS {
            let cache = cache.clone();
            let input = input.clone();
            let object = object.clone();
            set.spawn(async move {
                open_dataset(Some(&cache), input, object)
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

    /// A failed open is not cached, so the next caller tries again.
    ///
    /// `try_get_with` keeps errors out of the cache. A cached error would turn
    /// one bad read into a table that stays broken until the entry expires.
    #[tokio::test]
    async fn a_failed_open_is_not_cached() {
        let cache = NetcdfReaderCache::new(4);
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let path = object_store::path::Path::from("no/such/file.nc");
        let object = ObjectMeta {
            location: path.clone(),
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        };
        let input = NetcdfInput::Oxcdf { store, path };

        for attempt in 1..=2 {
            let result = open_dataset(Some(&cache), input.clone(), object.clone()).await;
            assert!(result.is_err(), "attempt {attempt} must fail");
        }
        assert_eq!(cache.cache.entry_count(), 0, "a failure must not be cached");
    }
}
