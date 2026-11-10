//! Benchmarks of the store and a walk, at 100K files and 10K columns.
//!
//! Each file has 20 sparse columns out of 10K, plus 4 dense columns that every
//! file has (`TIME`, `LATITUDE`, `LONGITUDE`, `DEPTH`). Two layouts spread the
//! sparse columns:
//!
//! - `clustered`: 100 directories of 1000 files. The files of one directory
//!   take their columns from one set of 100.
//! - `scattered`: each file takes 20 columns from all 10K.
//!
//! The store is a redb file in a temporary directory, so the numbers include
//! redb, but the page cache hides most disk reads.
//!
//! Run with `cargo bench -p beacon-file-statistics`.

use std::hint::black_box;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_external_table::statistics_cache::ExternalTableStatisticsProvider;
use beacon_file_statistics::discovery::Discovery;
use beacon_file_statistics::queue::AnalyzeQueue;
use beacon_file_statistics::store::FileRecord;
use beacon_file_statistics::{Config, FileStatistics, Stamp, StatisticsStore};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use redb::Database;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const FILES: usize = 100_000;
const COLUMNS: usize = 10_000;
const COLUMNS_PER_FILE: usize = 20;
const FILES_PER_DIRECTORY: usize = 1_000;
/// The default batch size of the sink, so each write has the same size as in production.
const BATCH: usize = 1_024;
/// The number of files in the query that reads one directory.
const SMALL_QUERY: usize = 1_000;
/// The columns that every file has.
const DENSE: [&str; 4] = ["TIME", "LATITUDE", "LONGITUDE", "DEPTH"];

/// How the files take their sparse columns from the 10K.
#[derive(Debug, Clone, Copy)]
enum Layout {
    Clustered,
    Scattered,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Clustered => "clustered",
            Self::Scattered => "scattered",
        }
    }

    /// The 20 distinct column indexes of file `file`.
    fn columns(self, file: usize) -> impl Iterator<Item = usize> {
        let (base, start, step, span) = match self {
            Self::Clustered => {
                let directory = file / FILES_PER_DIRECTORY;
                // 100 directories x 100 columns gives the 10K columns.
                let span = COLUMNS / (FILES / FILES_PER_DIRECTORY);
                (directory * span, file % span, span / COLUMNS_PER_FILE, span)
            }
            Self::Scattered => {
                // A multiplicative hash spreads the start; the step keeps the 20 columns distinct.
                let start = ((file as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize;
                (0, start % COLUMNS, COLUMNS / COLUMNS_PER_FILE, COLUMNS)
            }
        };
        (0..COLUMNS_PER_FILE).map(move |k| base + (start + k * step) % span)
    }
}

/// The files, as one data store lists them.
struct Files {
    store_url: ObjectStoreUrl,
    data_store: Arc<dyn ObjectStore>,
    /// Each file, in listing order.
    objects: Vec<ObjectMeta>,
    /// The stamp of each file, in listing order.
    stamps: Vec<Stamp>,
    names: Vec<String>,
}

impl Files {
    async fn new() -> Self {
        let data_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for file in 0..FILES {
            let path = Path::from(format!(
                "dir-{:03}/file-{file:06}.nc",
                file / FILES_PER_DIRECTORY
            ));
            data_store
                .put(&path, vec![0u8; 8].into())
                .await
                .expect("an in-memory put succeeds");
        }
        // InMemory sets the mtime on put, so the stamps come from the listing.
        let mut objects: Vec<ObjectMeta> = data_store
            .list(None)
            .try_collect()
            .await
            .expect("an in-memory listing succeeds");
        objects.sort_by(|a, b| a.location.cmp(&b.location));

        let store_url = ObjectStoreUrl::parse("memory://").expect("a valid store url");
        let stamps = objects
            .iter()
            .map(|object| Stamp::new(&store_url, object))
            .collect();
        Self {
            store_url,
            data_store,
            objects,
            stamps,
            names: (0..COLUMNS)
                .map(|column| format!("col-{column:05}"))
                .collect(),
        }
    }

    /// The records of all files, in batches of 1024 in listing order, as the sink gets them.
    fn batches(&self, layout: Layout) -> Vec<Vec<FileRecord>> {
        let files: Vec<usize> = (0..FILES).collect();
        files
            .chunks(BATCH)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|file| self.record(*file, layout))
                    .collect()
            })
            .collect()
    }

    /// The record of file `file`, from the statistics a format reports.
    fn record(&self, file: usize, layout: Layout) -> FileRecord {
        let fields: Vec<Field> = layout
            .columns(file)
            .map(|column| self.names[column].as_str())
            .chain(DENSE)
            .map(|name| Field::new(name, DataType::Float64, true))
            .collect();
        let value = file as f64;
        let column = ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Exact(ScalarValue::Float64(Some(value))),
            max_value: Precision::Exact(ScalarValue::Float64(Some(value + 1.0))),
            ..Default::default()
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![column; fields.len()],
        };
        FileRecord::new(self.stamps[file], &Schema::new(fields), &statistics)
    }
}

fn schema(names: &[&str]) -> SchemaRef {
    let fields: Vec<Field> = names
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// An empty store in a new redb file. The directory must outlive the store.
async fn empty_store() -> (TempDir, Arc<Database>, StatisticsStore) {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let database = Arc::new(
        Database::create(directory.path().join("statistics.redb")).expect("the database opens"),
    );
    let store = StatisticsStore::open(Arc::clone(&database))
        .await
        .expect("the store opens");
    (directory, database, store)
}

async fn put_all(store: &StatisticsStore, batches: Vec<Vec<FileRecord>>) {
    for batch in batches {
        store.put(batch).await.expect("the batch is stored");
    }
}

fn bench_layout(c: &mut Criterion, runtime: &Runtime, files: &Files, layout: Layout) {
    let batches = files.batches(layout);
    let (_directory, database, store) = runtime.block_on(empty_store());
    runtime.block_on(put_all(&store, batches.clone()));
    let store = Arc::new(store);

    let mut group = c.benchmark_group(format!("statistics/{}", layout.name()));
    group.sample_size(10);

    group.bench_function("put_100k", |b| {
        b.iter_batched(
            || (runtime.block_on(empty_store()), batches.clone()),
            |((_directory, _database, store), batches)| {
                runtime.block_on(put_all(&store, batches));
            },
            BatchSize::PerIteration,
        );
    });
    drop(batches);

    group.bench_function("contains_100k", |b| {
        b.iter(|| runtime.block_on(store.contains(black_box(&files.stamps))));
    });

    let sparse_1 = schema(&["col-00000"]);
    let sparse_3 = schema(&["col-00000", "col-00001", "col-00002"]);
    let dense_1 = schema(&DENSE[..1]);
    let dense_4 = schema(&DENSE);
    let reads = [
        ("sparse_1_column", &sparse_1),
        ("sparse_3_columns", &sparse_3),
        ("dense_1_column", &dense_1),
        ("dense_4_columns", &dense_4),
    ];
    let one_directory = &files.stamps[..SMALL_QUERY];
    for (label, stamps) in [("100k", &files.stamps[..]), ("1k", one_directory)] {
        for (name, schema) in reads {
            group.bench_function(format!("get_{name}_{label}"), |b| {
                b.iter(|| {
                    runtime
                        .block_on(store.get(schema, black_box(stamps)))
                        .expect("the statistics read")
                });
            });
        }
    }

    // The full query path: stamp 100k objects, read them, and build the pruning statistics.
    let statistics = runtime
        .block_on(FileStatistics::open(
            Config::default().with_num_threads(1),
            runtime.handle().clone(),
            Arc::new(OnceLock::new()),
            Arc::clone(&database),
        ))
        .expect("the pipeline opens");
    let directory_objects = &files.objects[..SMALL_QUERY];
    for (label, objects) in [("100k", &files.objects[..]), ("1k", directory_objects)] {
        for (name, schema) in [
            ("sparse_1_column", &sparse_1),
            ("dense_1_column", &dense_1),
            ("dense_4_columns", &dense_4),
        ] {
            group.bench_function(format!("fetch_statistics_{name}_{label}"), |b| {
                b.iter(|| {
                    runtime
                        .block_on(statistics.fetch_statistics(
                            files.store_url.clone(),
                            black_box(objects),
                            Arc::new(DefaultArrowTypeWidening::new()),
                            Arc::clone(schema),
                        ))
                        .expect("the statistics fetch")
                });
            });
        }
    }

    // A walk where each file is known: the cost of each walk after a restart.
    let discovery = Discovery::new(Arc::new(AnalyzeQueue::new(BATCH)), Arc::clone(&store));
    let walk = || {
        runtime
            .block_on(discovery.walk_once(&files.store_url, files.data_store.as_ref()))
            .expect("the walk succeeds")
    };
    assert_eq!(walk().queued, 0, "each listed file is in the store");
    group.bench_function("walk_known_100k", |b| b.iter(walk));

    group.finish();
}

fn benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the runtime starts");
    let files = runtime.block_on(Files::new());
    for layout in [Layout::Clustered, Layout::Scattered] {
        bench_layout(c, &runtime, &files, layout);
    }
}

criterion_group!(statistics, benches);
criterion_main!(statistics);
