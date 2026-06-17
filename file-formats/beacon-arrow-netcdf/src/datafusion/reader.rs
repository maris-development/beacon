use std::sync::{Arc, OnceLock};

use beacon_nd_array::dataset::AnyDataset;
use beacon_object_storage::DatasetsStore;
use object_store::ObjectMeta;

use crate::reader;

static DATASET_CACHE: OnceLock<moka::future::Cache<CacheKey, Arc<AnyDataset>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    pub object: object_store::path::Path,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

pub async fn open_dataset(
    object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> anyhow::Result<AnyDataset> {
    if beacon_config::CONFIG.netcdf.use_reader_cache {
        // Check the cache for an existing dataset before opening a new one.
        let cache = DATASET_CACHE.get_or_init(|| {
            moka::future::Cache::builder()
                .max_capacity(beacon_config::CONFIG.netcdf.reader_cache_size as u64)
                .build()
        });

        let key = CacheKey {
            object: object.location.clone(),
            last_modified: object.last_modified,
        };

        if let Some(cached_dataset) = cache.get(&key).await {
            return Ok((*cached_dataset).clone());
        }
    }

    let netcdf_path = object_store.translate_netcdf_url_path(&object.location)?;

    let dataset = reader::open_dataset(netcdf_path).await?;

    if beacon_config::CONFIG.netcdf.use_reader_cache {
        let cache = DATASET_CACHE.get_or_init(|| {
            moka::future::Cache::builder()
                .max_capacity(beacon_config::CONFIG.netcdf.reader_cache_size as u64)
                .build()
        });

        let key = CacheKey {
            object: object.location.clone(),
            last_modified: object.last_modified,
        };

        cache.insert(key, Arc::new(dataset.clone())).await;
    }

    Ok(dataset)
}

/// Fetch the Arrow schema for a NetCDF object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only
/// include variables that belong to those dimensions before deriving the
/// Arrow schema.
pub async fn fetch_schema(
    object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    let dataset = open_dataset(object_store, object).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open NetCDF dataset for schema inference: {e}"
        ))
    })?;

    let dataset = if let Some(dims) = read_dimensions {
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
