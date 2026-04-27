use std::sync::{Arc, OnceLock};

use beacon_nd_array::dataset::Dataset;
use beacon_object_storage::DatasetsStore;
use object_store::ObjectMeta;

use crate::reader;

static DATASET_CACHE: OnceLock<moka::future::Cache<CacheKey, Arc<Dataset>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    pub object: object_store::path::Path,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

pub async fn open_dataset(
    object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> anyhow::Result<Dataset> {
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
