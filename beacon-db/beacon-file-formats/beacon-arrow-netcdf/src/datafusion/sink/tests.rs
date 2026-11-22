//! The sinks against an object store: the upload, and what a failed write leaves.

use std::{fmt::Formatter, sync::Arc};

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use beacon_datafusion_ext::unique_values::{UniqueColumnValues, UniqueValuesHandleCollection};
use datafusion::datasource::physical_plan::{FileGroup, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use object_store::path::Path as ObjectPath;

use super::{NetCDFNdSink, NetCDFSink};
use crate::datafusion::options::NetcdfWriterOptions;

/// Build a minimal [`FileSinkConfig`] for testing.
pub(super) fn test_sink_config(schema: SchemaRef) -> FileSinkConfig {
    FileSinkConfig {
        original_url: String::new(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::default(),
        table_paths: vec![],
        output_schema: schema,
        table_partition_cols: vec![],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "nc".to_string(),
        file_output_mode: datafusion::datasource::physical_plan::FileOutputMode::SingleFile,
    }
}

// ── helpers ────────────────────────────────────────────────────────

// ── writes through the object store ────────────────────────────────

/// A sink config that writes to `location` in the store of `store_url`.
fn store_sink_config(schema: SchemaRef, store_url: &str, location: &str) -> FileSinkConfig {
    let mut config = test_sink_config(schema);
    config.object_store_url = ObjectStoreUrl::parse(store_url).unwrap();
    config.table_paths = vec![
        datafusion::datasource::listing::ListingTableUrl::parse(format!("{store_url}{location}"))
            .unwrap(),
    ];
    config
}

/// A task context with `store` under `store_url` and its temp files in `scratch`.
fn context_with_store(
    store_url: &str,
    store: Arc<dyn object_store::ObjectStore>,
    scratch: &std::path::Path,
) -> Arc<TaskContext> {
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_temp_file_path(scratch)
        .build_arc()
        .unwrap();
    let url = ObjectStoreUrl::parse(store_url).unwrap();
    runtime.register_object_store(url.as_ref(), store);
    Arc::new(TaskContext::default().with_runtime(runtime))
}

/// The regular files in the tree of `dir`.
fn files_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = vec![];
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(files_in(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// The disk manager folder is in `scratch`, and no file of the sink stays in it.
fn assert_no_scratch_file(scratch: &std::path::Path) {
    let folders = std::fs::read_dir(scratch).unwrap().count();
    assert_eq!(folders, 1, "the disk manager makes one folder");
    assert_eq!(files_in(scratch), Vec::<std::path::PathBuf>::new());
}

/// A store that refuses each multipart upload, the path of a file on disk.
#[derive(Debug)]
struct FailingUploadStore(Arc<object_store::memory::InMemory>);

impl std::fmt::Display for FailingUploadStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingUploadStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailingUploadStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(object_store::Error::Generic {
            store: "FailingUploadStore",
            source: "the upload fails".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.0.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// A stream of `batches`.
fn batch_stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ),
    )
}

/// Open the NetCDF object at `location` in `store`.
async fn open_object(store: &dyn object_store::ObjectStore, location: &str) -> oxcdf::NetcdfFile {
    use object_store::ObjectStoreExt;
    let bytes = store
        .get(&object_store::path::Path::from(location))
        .await
        .expect("the object is in the store")
        .bytes()
        .await
        .unwrap();
    oxcdf::NetcdfFile::from_source(Arc::new(oxcdf::MemorySource::new(bytes.to_vec())))
        .expect("the object is a NetCDF file")
}

/// Whether `store` holds an object at `location`.
async fn has_object(store: &dyn object_store::ObjectStore, location: &str) -> bool {
    use object_store::ObjectStoreExt;
    match store.head(&object_store::path::Path::from(location)).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => panic!("head failed: {e}"),
    }
}

/// No local file named after the location `a/b.nc` exists.
fn assert_no_local_copy() {
    for root in [std::path::PathBuf::from("."), std::env::temp_dir()] {
        assert!(!root.join("a").join("b.nc").exists(), "{}", root.display());
        assert!(!root.join("b.nc").exists(), "{}", root.display());
    }
}

fn pressure_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("temperature", DataType::Float64, true),
        Field::new("pressure", DataType::Int64, true),
    ]))
}

fn pressure_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![15.0, 20.5, 30.1])),
            Arc::new(Int64Array::from(vec![1013, 1010, 1005])),
        ],
    )
    .unwrap()
}

/// The handles of the grid lat = [10, 20], lon = [100, 200, 300].
fn grid_collection() -> UniqueValuesHandleCollection {
    use beacon_datafusion_ext::unique_values::UniqueValuesHandle;
    let lat: FieldRef = Arc::new(Field::new("lat", DataType::Int32, false));
    let lon: FieldRef = Arc::new(Field::new("lon", DataType::Int32, false));
    let handle = UniqueValuesHandle::try_new(&[lat.clone(), lon.clone()]).unwrap();
    {
        let mut values = handle.unique_column_values.lock();
        values.insert(
            lat,
            Box::new(UniqueColumnValues::<i32> {
                values: vec![10, 20],
            }),
        );
        values.insert(
            lon,
            Box::new(UniqueColumnValues::<i32> {
                values: vec![100, 200, 300],
            }),
        );
    }
    let collection = UniqueValuesHandleCollection::new();
    collection.register_handle(Arc::new(handle));
    collection
}

fn grid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lat", DataType::Int32, false),
        Field::new("lon", DataType::Int32, false),
        Field::new("temperature", DataType::Float64, true),
    ]))
}

/// Rows on the grid of `grid_collection`. The cell lat = 20, lon = 300 gets no row.
fn grid_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 10, 10, 20, 20])),
            Arc::new(Int32Array::from(vec![100, 200, 300, 100, 200])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn the_flat_sink_writes_through_the_object_store() {
    let scratch = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let schema = pressure_schema();
    let config = store_sink_config(schema.clone(), "memory://out/", "a/b.nc");
    let sink = NetCDFSink::new(config, Default::default());
    let context = context_with_store("memory://out/", store.clone(), scratch.path());

    let rows = sink
        .write_all(
            batch_stream(schema.clone(), vec![pressure_batch(&schema)]),
            &context,
        )
        .await
        .unwrap();

    assert_eq!(rows, 3);
    let file = open_object(store.as_ref(), "a/b.nc").await;
    let temperature: Vec<f64> = file
        .variable("temperature")
        .unwrap()
        .get_values(..)
        .unwrap();
    assert_eq!(temperature, vec![15.0, 20.5, 30.1]);
    let pressure: Vec<i64> = file.variable("pressure").unwrap().get_values(..).unwrap();
    assert_eq!(pressure, vec![1013, 1010, 1005]);
    assert_no_local_copy();
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn the_nd_sink_writes_through_the_object_store() {
    let scratch = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let schema = grid_schema();
    let config = store_sink_config(schema.clone(), "memory://out/", "a/b.nc");
    let sink = NetCDFNdSink::new(config, 2, grid_collection()).unwrap();
    let context = context_with_store("memory://out/", store.clone(), scratch.path());

    let rows = sink
        .write_all(
            batch_stream(schema.clone(), vec![grid_batch(&schema)]),
            &context,
        )
        .await
        .unwrap();

    assert_eq!(rows, 5);
    let file = open_object(store.as_ref(), "a/b.nc").await;
    let lat: Vec<i32> = file.variable("lat").unwrap().get_values(..).unwrap();
    assert_eq!(lat, vec![10, 20]);
    let lon: Vec<i32> = file.variable("lon").unwrap().get_values(..).unwrap();
    assert_eq!(lon, vec![100, 200, 300]);
    let data = file
        .variable("temperature")
        .unwrap()
        .get::<f64, _>(..)
        .unwrap();
    assert_eq!(data.shape(), &[2, 3]);
    assert_eq!(data[[0, 0]], 1.0);
    assert_eq!(data[[1, 1]], 5.0);
    assert_eq!(data[[1, 2]], f64::MIN);
    assert_no_local_copy();
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn a_failed_flat_write_puts_nothing_in_the_store() {
    let scratch = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("temp", DataType::Float64, true),
        Field::new("temp.comment", DataType::Utf8, true),
    ]));
    // An attribute above 64 KiB fails at the end of the write.
    let comment = "x".repeat(70_000);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(arrow::array::StringArray::from(vec![comment.as_str()])),
        ],
    )
    .unwrap();
    let options = NetcdfWriterOptions {
        combine_attrs: Some(crate::attributes::CombineAttrs::Identical),
        ..Default::default()
    };
    let config = store_sink_config(schema.clone(), "local://out/", "a/b.nc");
    let sink = NetCDFSink::new(config, options);
    let context = context_with_store("local://out/", store.clone(), scratch.path());

    let err = sink
        .write_all(batch_stream(schema, vec![batch]), &context)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("comment"), "{err}");
    assert!(!has_object(store.as_ref(), "a/b.nc").await);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn a_failed_nd_write_puts_nothing_in_the_store() {
    use arrow::array::DurationSecondArray;
    use beacon_datafusion_ext::unique_values::UniqueValuesHandle;

    let scratch = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let lat: FieldRef = Arc::new(Field::new("lat", DataType::Int32, false));
    // The test needs a column type that `write_ndarray_slab` rejects.
    // The sink defines a Duration column but cannot write its slab.
    let wait = Field::new(
        "wait",
        DataType::Duration(arrow::datatypes::TimeUnit::Second),
        true,
    );
    let schema = Arc::new(Schema::new(vec![lat.as_ref().clone(), wait]));
    let config = store_sink_config(schema.clone(), "local://out/", "grid.nc");

    let handle = UniqueValuesHandle::try_new(&[lat.clone()]).unwrap();
    handle.unique_column_values.lock().insert(
        lat,
        Box::new(UniqueColumnValues::<i32> {
            values: vec![10, 20],
        }),
    );
    let collection = UniqueValuesHandleCollection::new();
    collection.register_handle(Arc::new(handle));
    let sink = NetCDFNdSink::new(config, 1, collection).unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(DurationSecondArray::from(vec![1, 2])),
        ],
    )
    .unwrap();
    let context = context_with_store("local://out/", store.clone(), scratch.path());
    let err = sink
        .write_all(batch_stream(schema, vec![batch]), &context)
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("slab writing not implemented"),
        "{err}"
    );
    assert!(!has_object(store.as_ref(), "grid.nc").await);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn a_failed_flat_upload_puts_nothing_in_the_store() {
    let scratch = tempfile::tempdir().unwrap();
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = Arc::new(FailingUploadStore(inner.clone()));
    let schema = pressure_schema();
    let config = store_sink_config(schema.clone(), "memory://out/", "a/b.nc");
    let sink = NetCDFSink::new(config, Default::default());
    let context = context_with_store("memory://out/", store, scratch.path());

    let err = sink
        .write_all(
            batch_stream(schema.clone(), vec![pressure_batch(&schema)]),
            &context,
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("the upload fails"), "{err}");
    assert!(!has_object(inner.as_ref(), "a/b.nc").await);
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn a_failed_nd_upload_puts_nothing_in_the_store() {
    let scratch = tempfile::tempdir().unwrap();
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store = Arc::new(FailingUploadStore(inner.clone()));
    let schema = grid_schema();
    let config = store_sink_config(schema.clone(), "memory://out/", "a/b.nc");
    let sink = NetCDFNdSink::new(config, 2, grid_collection()).unwrap();
    let context = context_with_store("memory://out/", store, scratch.path());

    let err = sink
        .write_all(
            batch_stream(schema.clone(), vec![grid_batch(&schema)]),
            &context,
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("the upload fails"), "{err}");
    assert!(!has_object(inner.as_ref(), "a/b.nc").await);
    assert_no_scratch_file(scratch.path());
}

#[tokio::test]
async fn a_disabled_disk_manager_puts_the_scratch_file_in_the_os_temp_folder() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_disk_manager_builder(
            datafusion::execution::disk_manager::DiskManagerBuilder::default()
                .with_mode(datafusion::execution::disk_manager::DiskManagerMode::Disabled),
        )
        .build_arc()
        .unwrap();
    let url = ObjectStoreUrl::parse("memory://out/").unwrap();
    runtime.register_object_store(url.as_ref(), store.clone());
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let schema = pressure_schema();
    let config = store_sink_config(schema.clone(), "memory://out/", "a/b.nc");
    let sink = NetCDFSink::new(config, Default::default());

    sink.write_all(
        batch_stream(schema.clone(), vec![pressure_batch(&schema)]),
        &context,
    )
    .await
    .unwrap();

    let file = open_object(store.as_ref(), "a/b.nc").await;
    assert!(file.variable("pressure").is_some());
}
