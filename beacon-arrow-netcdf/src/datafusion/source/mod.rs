pub mod schema_cache;

use std::{num::NonZeroUsize, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
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
use futures::{stream::BoxStream, FutureExt};
use futures::{StreamExt, TryStreamExt as _};
use object_store::ObjectMeta;

use crate::reader::NetCDFArrowReader;

/// DataFusion `FileSource` for NetCDF (`.nc`) files.
#[derive(Debug, Clone)]
pub struct NetCDFFileSource {
    datasets_object_store: Arc<DatasetsStore>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    override_schema: Option<SchemaRef>,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl NetCDFFileSource {
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
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> datafusion::error::Result<Option<datafusion::datasource::physical_plan::FileScanConfig>>
    {
        Ok(None)
    }

    fn with_projection(
        &self,
        _config: &datafusion::datasource::physical_plan::FileScanConfig,
    ) -> Arc<dyn FileSource> {
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
    path: object_store::path::Path,
    last_modified: chrono::DateTime<chrono::Utc>,
}

pub async fn fetch_schema(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    let cached_schema = if beacon_config::CONFIG.netcdf_use_schema_cache {
        schema_cache::get_schema_from_cache(&object).await
    } else {
        None
    };

    if let Some(schema) = cached_schema {
        Ok(schema)
    } else {
        let reader = open_reader(datasets_object_store, object.clone())?;
        let schema = reader.schema();
        if beacon_config::CONFIG.netcdf_use_schema_cache {
            schema_cache::insert_schema_into_cache(&object, schema.clone()).await;
        }

        Ok(schema)
    }
}

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

pub struct DefaultFileOpener {
    datasets_object_store: Arc<DatasetsStore>,
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

    async fn read_task(
        object: ObjectMeta,
        datasets_object_store: Arc<DatasetsStore>,
        schema_adapter: Arc<dyn SchemaAdapter>,
    ) -> datafusion::error::Result<BoxStream<'static, datafusion::error::Result<RecordBatch>>> {
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
            return Ok(futures::stream::empty().boxed());
        }

        let arrow_stream = match reader
            .read_as_arrow_stream(beacon_config::CONFIG.beacon_batch_size)
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(
                    "Failed to read NetCDF file {} as Arrow stream: {error}",
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
                .join("test_files")
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
        ensure_test_fixtures();
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let reader = open_reader(datasets_object_store, obj).expect("reader");
        let schema = reader.schema();
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn default_file_opener_streams_projected_batches() {
        ensure_test_fixtures();
        let datasets_object_store = test_datasets_object_store().await;
        let obj = test_object_meta();

        let table_schema = fetch_schema(datasets_object_store.clone(), obj.clone())
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

        let mut stream = fut.await.expect("open future");
        let mut total_rows = 0usize;
        let mut saw_batch = false;
        while let Some(next) = stream.next().await {
            let batch = next.expect("batch ok");
            saw_batch = true;
            assert_eq!(batch.num_columns(), 1);
            total_rows += batch.num_rows();
        }

        assert!(saw_batch);
        assert!(total_rows > 0);
    }
}
