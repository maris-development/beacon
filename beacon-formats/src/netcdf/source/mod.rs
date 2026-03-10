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
use futures::{StreamExt, TryStreamExt as _};
use object_store::ObjectMeta;

fn mpio_enabled() -> bool {
    beacon_config::CONFIG.enable_multiplexer_netcdf
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
    /// Whether reads should go through the MPIO worker path.
    use_mpio: bool,
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
    pub fn new(datasets_object_store: Arc<DatasetsStore>, use_mpio: bool) -> Self {
        Self {
            datasets_object_store,
            use_mpio,
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
            self.use_mpio,
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

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> datafusion::error::Result<Option<datafusion::datasource::physical_plan::FileScanConfig>>
    {
        Ok(None)
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

    // fn repartitioned(
    //     &self,
    //     target_partitions: usize,
    //     repartition_file_min_size: usize,
    //     output_ordering: Option<datafusion::physical_expr::LexOrdering>,
    //     config: &datafusion::datasource::physical_plan::FileScanConfig,
    // ) -> datafusion::error::Result<Option<datafusion::datasource::physical_plan::FileScanConfig>>
    // {
    //     // Repartitioning is not currently supported for NetCDF files.
    //     let _ = (
    //         target_partitions,
    //         repartition_file_min_size,
    //         output_ordering,
    //         config,
    //     );
    // }

    fn file_type(&self) -> &str {
        "netcdf"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

lazy_static::lazy_static!(
    static ref READER_CACHE: parking_lot::Mutex<lru::LruCache<ReaderCacheKey, Arc<NetCDFArrowReader>>> = {
        let mut capacity = NonZeroUsize::new(beacon_config::CONFIG.netcdf_reader_cache_size)
            .unwrap_or_else(|| NonZeroUsize::new(1).expect("non-zero"));

        // If platform unix. then get the rlimit to check the capacity is at best rlimit/2 to avoid hitting the open file limit. This is a soft limit and can be increased by the user, but we want to avoid hitting it by default.
        #[cfg(unix)]
        {
            use rlimit::{getrlimit, Resource};
            if let Ok((soft_limit, _)) = getrlimit(Resource::NOFILE) {
                tracing::debug!("NetCDF Reader cache rlimit NOFILE soft limit: {}", soft_limit);
                let max_capacity = (soft_limit / 2) as usize;
                if capacity.get() > max_capacity {
                    capacity = NonZeroUsize::new(max_capacity).expect("non-zero");
                }
            }
        }

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
    fetch_schema_with_read_mode(datasets_object_store, object, mpio_enabled()).await
}

pub async fn fetch_schema_with_read_mode(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
    use_mpio: bool,
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
        let schema = if use_mpio {
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
    tracing::debug!("Opening NetCDFArrowReader for path: {}", netcdf_path);
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
    /// Whether reads should go through the MPIO worker path.
    use_mpio: bool,
}

impl DefaultFileOpener {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        use_mpio: bool,
    ) -> Self {
        Self {
            datasets_object_store,
            schema_adapter,
            use_mpio,
        }
    }
}

impl DefaultFileOpener {
    async fn read_task(
        object: ObjectMeta,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
        use_mpio: bool,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
        if use_mpio {
            // Fetch the file schema from the MPIO server (uses schema cache when enabled).
            let file_schema =
                fetch_schema_with_read_mode(datasets_object_store.clone(), object.clone(), true)
                    .await?;
            let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;
            let proj = if projection.is_empty() {
                // return an empty stream if no columns are projected, to avoid unnecessary reads.
                return Ok(futures::stream::empty().boxed());
            } else {
                Some(projection)
            };

            // Fetch data as a single concatenated batch from the MPIO server.
            let flight_stream =
                mpio::read_file_as_batch_stream(datasets_object_store, object, proj, None)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "MPIO read_file_as_batch failed: {e}"
                        ))
                    })?;

            let maybe_stream = flight_stream
                .map_err(|err| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error reading NetCDF file as Arrow stream: {err}"
                    ))
                })
                .and_then(move |batch| {
                    let adapted_res = schema_mapper.map_batch(batch).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to map NetCDF batch schema: {e}"
                        ))
                    });
                    match adapted_res {
                        Ok(adapted) => futures::future::ok(adapted),
                        Err(e) => futures::future::err(e),
                    }
                })
                .boxed();

            Ok(maybe_stream)
        } else {
            // Use the local (cached) NetCDF reader.
            let mut reader = open_reader(datasets_object_store, object.clone())?;
            let file_schema = reader.schema();
            let (schema_mapper, projection) = schema_adapter.map_schema(&file_schema)?;
            if !projection.is_empty() {
                reader = reader
                    .project::<&[usize]>(projection.as_ref())
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to project NetCDF reader: {e}"
                        ))
                    })?
                    .into();
            } else {
                // No columns found, so we should return an empty stream.
                return Ok(futures::stream::empty().boxed());
            };

            let arrow_stream = match reader
                .read_as_arrow_stream(beacon_config::CONFIG.beacon_batch_size)
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    // Skip for now. but warn about it and return an empty stream to avoid failing the whole query.
                    tracing::warn!(
                        "Failed to read NetCDF file {0} as Arrow stream: {error}",
                        object.location
                    );
                    futures::stream::empty().boxed()
                }
            };

            let maybe_stream = arrow_stream
                .map_err(|err| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error reading NetCDF file as Arrow stream: {err}"
                    ))
                })
                .and_then(move |batch| {
                    let adapted_res = schema_mapper.map_batch(batch).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to map NetCDF batch schema: {e}"
                        ))
                    });
                    match adapted_res {
                        Ok(adapted) => futures::future::ok(adapted),
                        Err(e) => futures::future::err(e),
                    }
                })
                .boxed();

            Ok(maybe_stream)
        }
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
            self.use_mpio,
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
    use std::path::PathBuf;
    use std::sync::Once;

    static TEST_FIXTURES: Once = Once::new();

    fn ensure_test_fixtures() {
        TEST_FIXTURES.call_once(|| {
            let src: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("test-files")
                .join("gridded-example.nc");

            let dst_dir: PathBuf = beacon_config::DATASETS_DIR_PATH.join("test-files");
            std::fs::create_dir_all(&dst_dir).expect("create datasets test-files dir");

            let dst = dst_dir.join("gridded-example.nc");
            if !dst.exists() {
                std::fs::copy(&src, &dst).expect("copy NetCDF test fixture into datasets dir");
            }
        });
    }

    fn test_object_meta() -> ObjectMeta {
        // For tests, keep `ObjectMeta.location` relative to the datasets store
        // root (./data/datasets). We copy the fixture into place via
        // `ensure_test_fixtures()`.
        ObjectMeta {
            location: Path::from("test-files/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    async fn test_datasets_object_store() -> Arc<DatasetsStore> {
        ensure_test_fixtures();
        get_datasets_object_store().await
    }

    #[tokio::test]
    async fn fetch_schema_reads_and_caches() {
        ensure_test_fixtures();
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let s1 = fetch_schema_with_read_mode(datasets_object_store.clone(), obj.clone(), false)
            .await
            .expect("schema");
        assert!(!s1.fields().is_empty());

        let s2 = fetch_schema_with_read_mode(datasets_object_store.clone(), obj, false)
            .await
            .expect("schema 2");
        assert_eq!(s1.fields().len(), s2.fields().len());
    }

    #[tokio::test]
    async fn open_reader_returns_reader() {
        ensure_test_fixtures();
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let reader = open_reader(datasets_object_store, obj).expect("reader");
        let schema = reader.schema();
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn default_file_opener_produces_one_batch() {
        ensure_test_fixtures();
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let table_schema =
            fetch_schema_with_read_mode(datasets_object_store.clone(), obj.clone(), false)
                .await
                .expect("schema");
        let analysed_sst_index = table_schema
            .fields()
            .iter()
            .position(|f| f.name() == "analysed_sst")
            .expect("analysed_sst field");
        let schema_adapter_factory = DefaultSchemaAdapterFactory;
        let schema_adapter = schema_adapter_factory.create(
            table_schema.project(&[analysed_sst_index]).unwrap().into(),
            table_schema,
        );

        let opener =
            DefaultFileOpener::new(datasets_object_store, Arc::from(schema_adapter), false);

        let file_meta = FileMeta {
            object_meta: obj,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let fut = opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open");

        let mut stream = fut.await.expect("open future");
        println!("Stream batch: {:?}", stream.next().await.unwrap());
        // let batches: Vec<_> = stream.collect().await;
        // assert_eq!(batches.len(), 1);
        // let batch = batches[0].as_ref().expect("batch ok");
        // assert!(batch.num_columns() > 0);
    }

    #[tokio::test]
    async fn multiplexer_reads_schema_and_batch_via_worker() {
        ensure_test_fixtures();

        // Requires the `beacon-arrow-netcdf-mpio` executable.
        // Optionally provide a path via `BEACON_NETCDF_MPIO_WORKER`.
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let table_schema =
            fetch_schema_with_read_mode(datasets_object_store.clone(), obj.clone(), true)
                .await
                .expect("schema");
        assert!(!table_schema.fields().is_empty());

        // Table schema should only contain projected columns.
        let projection = vec![0];
        let table_schema = Arc::new(table_schema.project(&projection).expect("projected schema"));

        let schema_adapter_factory = DefaultSchemaAdapterFactory;
        let schema_adapter =
            schema_adapter_factory.create(table_schema.clone(), table_schema.clone());
        let opener = DefaultFileOpener::new(datasets_object_store, Arc::from(schema_adapter), true);

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
