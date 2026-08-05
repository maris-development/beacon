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
pub async fn open_dataset(
    cache: Option<&NetcdfReaderCache>,
    input: NetcdfInput,
    object: ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let key = CacheKey {
        object: object.location.clone(),
        last_modified: object.last_modified,
        backend: input.backend(),
    };

    if let Some(cache) = cache {
        if let Some(cached_dataset) = cache.cache.get(&key).await {
            return Ok((*cached_dataset).clone());
        }
    }

    let dataset = input.open().await?;

    if let Some(cache) = cache {
        cache.cache.insert(key, Arc::new(dataset.clone())).await;
    }

    Ok(dataset)
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
