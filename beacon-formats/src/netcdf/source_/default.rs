use std::{num::NonZeroUsize, sync::Arc};

use arrow::array::RecordBatch;
use beacon_arrow_netcdf::reader::NetCDFArrowReader;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
        schema_adapter::{self, SchemaAdapter},
    },
    execution::SendableRecordBatchStream,
};
use futures::{FutureExt, stream::BoxStream};
use object_store::ObjectMeta;

use crate::netcdf::{object_resolver::NetCDFObjectResolver, source_::schema_cache};

lazy_static::lazy_static!(
    static ref READER_CACHE: parking_lot::Mutex<lru::LruCache<ReaderCacheKey, Arc<NetCDFArrowReader>>> = {
        let capacity = NonZeroUsize::new(beacon_config::CONFIG.netcdf_reader_cache_size).unwrap();
        parking_lot::Mutex::new(lru::LruCache::new(capacity))
    };
);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReaderCacheKey {
    // Define the fields for the cache entry
    path: object_store::path::Path,
    last_modified: chrono::DateTime<chrono::Utc>,
}

pub async fn fetch_schema(
    object_resolver: Arc<NetCDFObjectResolver>,
    object: ObjectMeta,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    // First try to get the schema from the cache
    let cached_schema = if beacon_config::CONFIG.netcdf_use_schema_cache {
        schema_cache::get_schema_from_cache(&object).await
    } else {
        None
    };

    if let Some(schema) = cached_schema {
        Ok(schema)
    } else {
        let netcdf_path = object_resolver.resolve_object_meta(object.clone());
        let reader = NetCDFArrowReader::new(netcdf_path).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create NetCDFArrowReader: {}",
                e
            ))
        })?;
        let schema = reader.schema();

        if beacon_config::CONFIG.netcdf_use_reader_cache {
            // Insert a reader into the reader cache
            let key = ReaderCacheKey {
                last_modified: object.last_modified,
                path: object.location.clone(),
            };

            let mut cache = READER_CACHE.lock();
            cache.put(key, Arc::new(reader));
        }

        // Insert the schema into the cache for future use
        if beacon_config::CONFIG.netcdf_use_schema_cache {
            schema_cache::insert_schema_into_cache(&object, schema.clone()).await;
        }

        Ok(schema)
    }
}

pub struct DefaultFileOpener {
    object_resolver: Arc<NetCDFObjectResolver>,
    /// Schema adapter for mapping NetCDF schema to Arrow schema.
    schema_adapter: Arc<dyn SchemaAdapter>,
}

impl DefaultFileOpener {
    pub fn new(
        object_resolver: Arc<NetCDFObjectResolver>,
        schema_adapter: Arc<dyn SchemaAdapter>,
    ) -> Self {
        Self {
            object_resolver,
            schema_adapter,
        }
    }
}

impl DefaultFileOpener {
    fn open_reader(&self, object: ObjectMeta) -> datafusion::error::Result<Arc<NetCDFArrowReader>> {
        if beacon_config::CONFIG.netcdf_use_reader_cache {
            let key = ReaderCacheKey {
                last_modified: object.last_modified,
                path: object.location.clone(),
            };

            let mut cache = READER_CACHE.lock();
            if let Some(reader) = cache.get(&key) {
                return Ok(reader.clone());
            }
        }
        let netcdf_path = self.object_resolver.resolve_object_meta(object.clone());
        let reader = NetCDFArrowReader::new(netcdf_path).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create NetCDFArrowReader: {}",
                e
            ))
        })?;
        let reader = Arc::new(reader);
        if beacon_config::CONFIG.netcdf_use_reader_cache {
            let key = ReaderCacheKey {
                last_modified: object.last_modified,
                path: object.location.clone(),
            };
            let mut cache = READER_CACHE.lock();
            cache.put(key, reader.clone());
        }

        Ok(reader)
    }

    async fn read_task(
        file_path: String,
        projection: Option<Vec<usize>>,
        schema_adapter: Arc<dyn SchemaAdapter>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        todo!()
    }
}

impl FileOpener for DefaultFileOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let stream = Self::read_task().boxed();
        Ok(stream)
    }
}
