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
pub async fn open_dataset(
    cache: Option<&Hdf5ReaderCache>,
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    let key = CacheKey {
        object: object.location.clone(),
        last_modified: object.last_modified,
    };

    if let Some(cache) = cache {
        if let Some(cached) = cache.cache.get(&key).await {
            return Ok((*cached).clone());
        }
    }

    let dataset = crate::reader::open_dataset(store.clone(), object.location.clone()).await?;

    if let Some(cache) = cache {
        cache.cache.insert(key, Arc::new(dataset.clone())).await;
    }

    Ok(dataset)
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
