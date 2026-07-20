use std::path::PathBuf;
use std::sync::Arc;

use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_nd_array::dataset::AnyDataset;
use object_store::ObjectMeta;

use crate::reader;

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
}

/// Open a NetCDF dataset, optionally consulting `cache`.
///
/// When `cache` is `Some`, an entry keyed by object path + last-modified is
/// checked before opening and populated afterwards. When `None`, the dataset is
/// opened directly with no caching (e.g. schema inference or a table that opted
/// out of caching).
pub async fn open_dataset(
    listing_factory: &ListingFactory,
    cache: Option<&NetcdfReaderCache>,
    object: ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let key = CacheKey {
        object: object.location.clone(),
        last_modified: object.last_modified,
    };

    if let Some(cache) = cache {
        if let Some(cached_dataset) = cache.cache.get(&key).await {
            return Ok((*cached_dataset).clone());
        }
    }

    let netcdf_path: PathBuf = todo!(); // beacon_object_storage::local_object_path(&datasets_root, &object.location)?;

    let dataset = reader::open_dataset(netcdf_path).await?;

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
    listing_factory: &ListingFactory,
    object: ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    // Schema inference does not consult the reader cache; the cache benefits
    // repeated data scans, which flow through `NetCDFSource`.
    let dataset = open_dataset(listing_factory, None, object)
        .await
        .map_err(|e| {
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
