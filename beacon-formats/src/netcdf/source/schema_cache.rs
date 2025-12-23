use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};

lazy_static::lazy_static!(
    static ref SCHEMA_CACHE : moka::future::Cache<CacheKey, SchemaRef> = moka::future::Cache::builder()
        .max_capacity(beacon_config::CONFIG.netcdf_schema_cache_size)
        .build();
);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    path: object_store::path::Path,
    last_modified: DateTime<Utc>,
}

pub async fn get_schema_from_cache(object_meta: &object_store::ObjectMeta) -> Option<SchemaRef> {
    let key = CacheKey {
        last_modified: object_meta.last_modified,
        path: object_meta.location.clone(),
    };

    SCHEMA_CACHE.get(&key).await
}

pub async fn insert_schema_into_cache(object_meta: &object_store::ObjectMeta, schema: SchemaRef) {
    let key = CacheKey {
        last_modified: object_meta.last_modified,
        path: object_meta.location.clone(),
    };
    SCHEMA_CACHE.insert(key, schema).await;
}
