pub mod mpio;
pub mod options;
pub mod schema_cache;

use std::{num::NonZeroUsize, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use beacon_arrow_netcdf::reader::NetCDFArrowReader;
use beacon_object_storage::DatasetsStore;
use datafusion::{
    common::Statistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileSource},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{FutureExt, stream::BoxStream};
use object_store::ObjectMeta;

use crate::netcdf::object_resolver::NetCDFObjectResolver;

const DEFAULT_MPIO_CHUNK_SIZE: usize = 128 * 1024;
const DEFAULT_MPIO_STREAM_SIZE: usize = 4 * 1024 * 1024;

fn mpio_enabled() -> bool {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;
        let v = MPIO_ENABLED_OVERRIDE.load(Ordering::Relaxed);
        if v != -1 {
            return v == 1;
        }
    }

    beacon_config::CONFIG.enable_multiplexer_netcdf
}

#[cfg(test)]
static MPIO_ENABLED_OVERRIDE: std::sync::atomic::AtomicI8 = std::sync::atomic::AtomicI8::new(-1);

#[cfg(test)]
struct MpioOverrideGuard;

#[cfg(test)]
impl Drop for MpioOverrideGuard {
    fn drop(&mut self) {
        MPIO_ENABLED_OVERRIDE.store(-1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
fn override_mpio_enabled_for_test(enabled: bool) -> MpioOverrideGuard {
    MPIO_ENABLED_OVERRIDE.store(
        if enabled { 1 } else { 0 },
        std::sync::atomic::Ordering::Relaxed,
    );
    MpioOverrideGuard
}

/// DataFusion `FileSource` for NetCDF (`.nc`) files.
///
/// This integrates `beacon_arrow_netcdf::reader::NetCDFArrowReader` with
/// DataFusion's file scan pipeline via a `FileOpener`.
///
/// Notes:
/// - Schema discovery is performed by opening the NetCDF file and building an
///   Arrow schema.
/// - Reading currently returns a single `RecordBatch` per file.
/// - Reader/schema caches are controlled via `beacon_config::CONFIG`.
#[derive(Debug, Clone)]
pub struct NetCDFFileSource {
    datasets_object_store: Arc<DatasetsStore>,
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Option<Statistics>,
}

impl NetCDFFileSource {
    /// Creates a new `NetCDFFileSource`.
    ///
    /// The `object_resolver` determines how an `ObjectMeta` location is turned
    /// into a concrete path that `netcdf::open` can handle (local path or
    /// remote URL).
    pub fn new(datasets_object_store: Arc<DatasetsStore>) -> Self {
        Self {
            datasets_object_store,
            override_schema: None,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            schema_adapter_factory: None,
        }
    }
}

impl FileSource for NetCDFFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &datafusion::datasource::physical_plan::FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        let table_schema = self
            .override_schema
            .clone()
            .unwrap_or_else(|| base_config.file_schema.clone());
        let projected_schema = base_config.projected_schema();
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);
        let arc_schema_adapter = Arc::from(schema_adapter);

        Arc::new(DefaultFileOpener::new(
            self.datasets_object_store.clone(),
            arc_schema_adapter,
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        // The underlying NetCDF reader currently emits a single batch per file.
        // Batch sizing is therefore not applicable here.
        let _ = batch_size;
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(
        &self,
        config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> Arc<dyn FileSource> {
        // Projection is handled during schema mapping inside the `FileOpener`.
        let _ = config;
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        if let Some(statistics) = &self.projected_statistics {
            Ok(statistics.clone())
        } else if let Some(schema) = self.override_schema.as_ref() {
            Ok(Statistics::new_unknown(schema))
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "netcdf"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

lazy_static::lazy_static!(
    static ref READER_CACHE: parking_lot::Mutex<lru::LruCache<ReaderCacheKey, Arc<NetCDFArrowReader>>> = {
        let capacity = NonZeroUsize::new(beacon_config::CONFIG.netcdf_reader_cache_size)
            .unwrap_or_else(|| NonZeroUsize::new(1).expect("non-zero"));
        parking_lot::Mutex::new(lru::LruCache::new(capacity))
    };
);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReaderCacheKey {
    /// Object location.
    path: object_store::path::Path,
    /// Timestamp used to invalidate cached readers when the object changes.
    last_modified: chrono::DateTime<chrono::Utc>,
}

/// Fetch the Arrow schema for a NetCDF object.
///
/// When enabled via `beacon_config::CONFIG.netcdf_use_schema_cache`, the schema
/// is cached using the object's location and last-modified time.
pub async fn fetch_schema(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    // First try to get the schema from the cache.
    let cached_schema = if beacon_config::CONFIG.netcdf_use_schema_cache {
        schema_cache::get_schema_from_cache(&object).await
    } else {
        None
    };

    if let Some(schema) = cached_schema {
        Ok(schema)
    } else {
        let schema = if mpio_enabled() {
            mpio::read_schema(datasets_object_store.clone(), object.clone())
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "MPIO read_schema failed: {}",
                        e
                    ))
                })?
        } else {
            let reader = open_reader(datasets_object_store.clone(), object.clone())?;
            reader.schema()
        };
        // Insert the schema into the cache for future use
        if beacon_config::CONFIG.netcdf_use_schema_cache {
            schema_cache::insert_schema_into_cache(&object, schema.clone()).await;
        }

        Ok(schema)
    }
}

/// Open (or reuse) a `NetCDFArrowReader` for an object.
///
/// When enabled via `beacon_config::CONFIG.netcdf_use_reader_cache`, readers are
/// cached using object location and last-modified time.
///
/// Important: this function avoids holding the cache lock while performing IO.
pub fn open_reader(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> datafusion::error::Result<Arc<NetCDFArrowReader>> {
    if beacon_config::CONFIG.netcdf_use_reader_cache {
        let key = ReaderCacheKey {
            last_modified: object.last_modified,
            path: object.location.clone(),
        };
        if let Some(reader) = READER_CACHE.lock().get(&key).cloned() {
            return Ok(reader);
        }
    }

    let netcdf_path = datasets_object_store
        .translate_netcdf_url_path(&object.location)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to translate NetCDF URL path: {}",
                e
            ))
        })?;
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
        READER_CACHE.lock().put(key, reader.clone());
    }
    Ok(reader)
}

/// Default `FileOpener` implementation for NetCDF scans.
///
/// Reads the whole NetCDF file into a single `RecordBatch`, then applies the
/// DataFusion schema mapping and projection.
pub struct DefaultFileOpener {
    datasets_object_store: Arc<DatasetsStore>,
    /// Schema adapter for mapping NetCDF schema to Arrow schema.
    schema_adapter: Arc<dyn SchemaAdapter>,
}

impl DefaultFileOpener {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter,
        }
    }
}

impl DefaultFileOpener {
    async fn read_task(
        object: ObjectMeta,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        // Map the file schema into the table schema.
        // In MPIO mode, schema comes from the worker (and can be cached); otherwise open the local reader.
        let (reader, file_schema) = if mpio_enabled() {
            let schema = fetch_schema(datasets_object_store.clone(), object.clone()).await?;
            (None, schema)
        } else {
            let reader = open_reader(datasets_object_store.clone(), object.clone())?;
            let schema = reader.schema();
            (Some(reader), schema)
        };
        let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;

        // NetCDF reading happens synchronously inside `read_as_batch`.
        // DataFusion calls the `FileOpenFuture` asynchronously; if we later need
        // to mitigate blocking, this is the place to `spawn_blocking`.
        let stream = Box::pin(futures::stream::once(async move {
            let batch = if mpio_enabled() {
                mpio::read_file_as_batch(
                    datasets_object_store.clone(),
                    object,
                    Some(projection),
                    DEFAULT_MPIO_CHUNK_SIZE,
                    DEFAULT_MPIO_STREAM_SIZE,
                )
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "MPIO read_file_as_batch failed: {}",
                        e
                    ))
                })?
            } else {
                let reader = reader.expect("reader must be present when MPIO is disabled");
                reader.read_as_batch(Some(&projection)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read NetCDF batch: {}",
                        e
                    ))
                })?
            };
            let adapted_batch = schema_mapper.map_batch(batch)?;
            Ok(adapted_batch)
        })) as BoxStream<'static, datafusion::error::Result<RecordBatch>>;

        Ok(stream)
    }
}

impl FileOpener for DefaultFileOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let stream = Self::read_task(
            file_meta.object_meta,
            self.datasets_object_store.clone(),
            self.schema_adapter.clone(),
        )
        .boxed();
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netcdf::object_resolver::NetCDFObjectResolver;
    use beacon_object_storage::get_datasets_object_store;
    use datafusion::datasource::physical_plan::FileMeta;
    use futures::StreamExt;
    use object_store::path::Path;

    fn test_object_meta() -> ObjectMeta {
        // For tests, keep `ObjectMeta.location` relative and point the resolver
        // endpoint at the crate directory. This matches how the resolver builds
        // filesystem paths.
        ObjectMeta {
            location: Path::from("test-files/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    async fn test_datasets_object_store() -> Arc<DatasetsStore> {
        get_datasets_object_store().await
    }

    #[tokio::test]
    async fn fetch_schema_reads_and_caches() {
        let _guard = override_mpio_enabled_for_test(false);
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let s1 = fetch_schema(datasets_object_store.clone(), obj.clone())
            .await
            .expect("schema");
        assert!(!s1.fields().is_empty());

        let s2 = fetch_schema(datasets_object_store.clone(), obj)
            .await
            .expect("schema 2");
        assert_eq!(s1.fields().len(), s2.fields().len());
    }

    #[tokio::test]
    async fn open_reader_returns_reader() {
        let _guard = override_mpio_enabled_for_test(false);
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let reader = open_reader(datasets_object_store, obj).expect("reader");
        let schema = reader.schema();
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn default_file_opener_produces_one_batch() {
        let _guard = override_mpio_enabled_for_test(false);
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let table_schema = fetch_schema(datasets_object_store.clone(), obj.clone())
            .await
            .expect("schema");
        let schema_adapter_factory = DefaultSchemaAdapterFactory;
        let schema_adapter = schema_adapter_factory.create(table_schema.clone(), table_schema);

        let opener = DefaultFileOpener::new(datasets_object_store, Arc::from(schema_adapter));

        let file_meta = FileMeta {
            object_meta: obj,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let fut = opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open");

        let stream = fut.await.expect("open future");
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().expect("batch ok");
        assert!(batch.num_columns() > 0);
    }

    #[tokio::test]
    async fn multiplexer_reads_schema_and_batch_via_worker() {
        let _guard = override_mpio_enabled_for_test(true);

        // Requires the `beacon-arrow-netcdf-mpio` executable.
        // Optionally provide a path via `BEACON_NETCDF_MPIO_WORKER`.
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let table_schema = fetch_schema(datasets_object_store.clone(), obj.clone())
            .await
            .expect("schema");
        assert!(!table_schema.fields().is_empty());

        // Table schema should only contain projected columns.
        let projection = vec![0];
        let table_schema = Arc::new(table_schema.project(&projection).expect("projected schema"));

        let schema_adapter_factory = DefaultSchemaAdapterFactory;
        let schema_adapter =
            schema_adapter_factory.create(table_schema.clone(), table_schema.clone());
        let opener = DefaultFileOpener::new(datasets_object_store, Arc::from(schema_adapter));

        let file_meta = FileMeta {
            object_meta: obj,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let fut = opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open");

        let stream = fut.await.expect("open future");
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().expect("batch ok");
        assert!(batch.num_columns() > 0);
        assert!(batch.num_rows() > 0);
    }
}
