//! Benchmarks of the statistics engine at 100K and 1M files.
//!
//! Each file is a small Parquet file with 4 columns (`TIME`, `LATITUDE`,
//! `LONGITUDE`, `DEPTH`) in an in-memory data store. The statistics tables live
//! in a Lance warehouse over a temporary folder, so the writes go to disk.
//!
//! Run with `cargo bench -p beacon-file-statistics`. The 1M setup analyzes one
//! million files first, which takes a while.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_datafusion_ext::format_ext::{FileFormatFactoryExt, FileFormatRegistry};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_external_table::statistics_cache::{
    ExternalTableStatisticsProvider, StatisticsRegistration,
};
use beacon_file_statistics::{Config, FileStatistics, SessionCell};
use beacon_lance::LanceWarehouse;
use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// The columns that every file has.
const COLUMNS: [&str; 4] = ["TIME", "LATITUDE", "LONGITUDE", "DEPTH"];
/// The files of one directory.
const FILES_PER_DIRECTORY: usize = 1_000;
/// The number of changed files in the change benchmark.
const CHANGED: usize = 1_000;

fn table_schema() -> SchemaRef {
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// The bytes of one small Parquet file. Every file shares them.
fn parquet_bytes() -> Bytes {
    let schema = table_schema();
    let columns: Vec<ArrayRef> = COLUMNS
        .iter()
        .map(|_| Arc::new(Float64Array::from(vec![1.0, 5.0, 3.0])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("a valid batch");
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("a writer");
    writer.write(&batch).expect("the batch is written");
    writer.close().expect("the file is closed");
    Bytes::from(bytes)
}

fn path(file: usize) -> Path {
    Path::from(format!(
        "data/dir-{:04}/file-{file:07}.parquet",
        file / FILES_PER_DIRECTORY
    ))
}

/// A data store with `files` files, a session over it, and an engine over a temporary warehouse.
struct Setup {
    data: Arc<dyn ObjectStore>,
    _ctx: Arc<SessionContext>,
    engine: FileStatistics,
    objects: Vec<ObjectMeta>,
    _warehouse_dir: TempDir,
}

impl Setup {
    async fn new(files: usize) -> Self {
        let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bytes = parquet_bytes();
        for file in 0..files {
            data.put(&path(file), bytes.clone().into())
                .await
                .expect("an in-memory put succeeds");
        }
        let objects: Vec<ObjectMeta> = data
            .list(Some(&Path::from("data")))
            .try_collect()
            .await
            .expect("a listing");

        let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![Arc::new(ParquetFormatFactory)];
        let registry = OnceLock::new();
        assert!(registry.set(FileFormatRegistry::new(&formats)).is_ok());
        // As in Beacon: a table location without a glob covers its subfolders too.
        let config = SessionConfig::new()
            .set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            )
            .with_extension(Arc::new(registry))
            .with_extension(Arc::new(ListingFactory::new(None)));
        let ctx = Arc::new(SessionContext::new_with_config(config));
        ctx.register_object_store(store_url().as_ref(), Arc::clone(&data));
        let cell: SessionCell = Arc::new(OnceLock::new());
        assert!(cell.set(Arc::downgrade(&ctx)).is_ok());

        let warehouse_dir = tempfile::tempdir().expect("a temporary directory");
        let warehouse_store =
            LocalFileSystem::new_with_prefix(warehouse_dir.path()).expect("a local store");
        let warehouse = Arc::new(LanceWarehouse::new(Arc::new(warehouse_store)));
        let engine = FileStatistics::new(
            Config::default(),
            tokio::runtime::Handle::current(),
            cell,
            warehouse,
        );
        Self {
            data,
            _ctx: ctx,
            engine,
            objects,
            _warehouse_dir: warehouse_dir,
        }
    }

    /// Registers the table `obs`, and waits until its first refresh ends.
    async fn register(&self) {
        self.engine
            .register_table(registration())
            .await
            .expect("the table registers");
        let summary = self.engine.refresh("obs").await.expect("a refresh");
        // A bench over an empty statistics table measures nothing.
        assert_eq!(
            summary.listed,
            self.objects.len(),
            "the refresh lists each file"
        );
    }

    /// Writes the first `CHANGED` files again, which gives them a new mtime.
    async fn change(&self) {
        let bytes = parquet_bytes();
        for file in 0..CHANGED {
            self.data
                .put(&path(file), bytes.clone().into())
                .await
                .expect("an in-memory put succeeds");
        }
    }
}

fn store_url() -> ObjectStoreUrl {
    ObjectStoreUrl::parse("memory://").expect("a valid store url")
}

fn registration() -> StatisticsRegistration {
    StatisticsRegistration {
        table: "obs".to_string(),
        store_url: store_url(),
        urls: vec![ListingTableUrl::parse("memory:///data/").expect("a valid url")],
        file_type: "parquet".to_string(),
        format_options: HashMap::new(),
        schema: table_schema(),
        columns: None,
        // Only the benchmarks refresh.
        refresh_interval: Duration::from_secs(24 * 3600),
        type_widening: Arc::new(DefaultArrowTypeWidening::new()),
    }
}

fn fetch_schema(columns: usize) -> SchemaRef {
    Arc::new(Schema::new(table_schema().fields()[..columns].to_vec()))
}

fn bench_size(c: &mut Criterion, runtime: &Runtime, files: usize, label: &str) {
    let mut group = c.benchmark_group(format!("statistics/{label}"));
    group.sample_size(10);

    // The first refresh: each file is new.
    if files <= 100_000 {
        group.bench_function("refresh_first", |b| {
            b.iter_batched(
                || runtime.block_on(Setup::new(files)),
                |setup| runtime.block_on(setup.register()),
                BatchSize::PerIteration,
            );
        });
    }

    let setup = runtime.block_on(Setup::new(files));
    runtime.block_on(setup.register());

    group.bench_function("refresh_unchanged", |b| {
        b.iter(|| {
            runtime
                .block_on(setup.engine.refresh("obs"))
                .expect("a refresh")
        });
    });

    group.bench_function(format!("refresh_{CHANGED}_changed"), |b| {
        b.iter_batched(
            || runtime.block_on(setup.change()),
            |()| {
                runtime
                    .block_on(setup.engine.refresh("obs"))
                    .expect("a refresh")
            },
            BatchSize::PerIteration,
        );
    });

    let all: Vec<&ObjectMeta> = setup.objects.iter().collect();
    let one_directory: Vec<&ObjectMeta> = setup.objects[..FILES_PER_DIRECTORY].iter().collect();
    for (scope, objects) in [("all", &all), ("1k", &one_directory)] {
        for columns in [1, 4] {
            let schema = fetch_schema(columns);
            group.bench_function(format!("fetch_{columns}_columns_{scope}"), |b| {
                b.iter(|| {
                    runtime
                        .block_on(setup.engine.fetch_statistics(
                            "obs",
                            black_box(objects),
                            Arc::clone(&schema),
                        ))
                        .expect("the statistics fetch")
                });
            });
        }
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the runtime starts");
    bench_size(c, &runtime, 100_000, "100k");
    bench_size(c, &runtime, 1_000_000, "1m");
}

criterion_group!(statistics, benches);
criterion_main!(statistics);
