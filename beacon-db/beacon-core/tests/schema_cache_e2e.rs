//! The schema cache through a real runtime.
//!
//! Everything below this has been tested against a fake format. This is where a
//! `RuntimeBuilder` produces the collector, the collector reads real files with
//! a real format, and a real `read_*` query answers from what it recorded.
//!
//! The assertions are on the cache's own counters rather than on a clock. A
//! query that reports one hit per file derived nothing, however long it took,
//! and a timing assertion on a two-file collection would say nothing at all.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_common::FileStatsConfig;
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_datafusion_ext::listing_factory::RootStore;
use beacon_file_stats::SchemaCacheCounters;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;

/// File statistics on, with the timer far enough out that every pass in these
/// tests is one the test asked for.
fn enabled(schema_cache: bool) -> FileStatsConfig {
    FileStatsConfig {
        enable: true,
        interval_secs: 3_600,
        on_startup: false,
        concurrency: 2,
        batch_files: 100,
        target_group_files: 100,
        min_group_files: 2,
        prefix_depth: None,
        scan_prefix: String::new(),
        discovery_chunk: 50,
        schema_cache,
    }
}

async fn runtime(root: &Path, schema_cache: bool) -> Runtime {
    let datasets = root.join("datasets");
    std::fs::create_dir_all(&datasets).unwrap();
    std::fs::create_dir_all(root.join("tmp")).unwrap();
    RuntimeBuilder::new()
        .with_db_path(root.join("beacon.db"))
        .with_default_store(
            ObjectStoreUrl::parse("datasets://").unwrap(),
            RootStore::FileSystem(datasets),
        )
        .with_tmp_dir_path(root.join("tmp"))
        .with_file_stats(enabled(schema_cache))
        .build()
        .await
        .unwrap()
}

/// A Parquet file holding `TEMP` and `DEPTH`.
fn write_parquet(path: &Path, temp: f64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, false),
        Field::new("DEPTH", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![temp])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A NetCDF-4 file holding `TEMP` and `DEPTH`. The format the issue measured.
fn write_netcdf(path: &Path, temp: f64) {
    use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
    use beacon_arrow_netcdf::writer::ArrowRecordBatchWriter;

    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, false),
        Field::new("DEPTH", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![temp])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mut writer = ArrowRecordBatchWriter::<DefaultEncoder>::new(path, schema).unwrap();
    writer.write_record_batch(batch).unwrap();
    writer.finish().unwrap();
}

async fn query(runtime: &Runtime, sql: &str) -> String {
    let batches = runtime
        .run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

fn counters(runtime: &Runtime) -> SchemaCacheCounters {
    runtime
        .file_stats()
        .expect("the subsystem is enabled")
        .store()
        .schema_cache()
        .counters()
}

/// What one query asked of the cache. The counters are cumulative, so a test
/// reads the difference across the call it cares about.
fn delta(before: SchemaCacheCounters, after: SchemaCacheCounters) -> (u64, u64) {
    (after.hits - before.hits, after.misses - before.misses)
}

/// The headline, through the real thing. A collection the collector has been
/// over answers every file from the cache, and reports the same columns it
/// reported when it was cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_analysed_collection_answers_from_the_cache() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..3 {
        write_parquet(
            &root.path().join(format!("datasets/obs/{i}.parquet")),
            i as f64,
        );
    }
    let runtime = runtime(root.path(), true).await;

    // Cold: nothing is recorded, so the plan derives all three. Ordered,
    // because row order across file groups is the scan's business and varies
    // with partitioning — what this test compares is the answer, not the plan.
    const ROWS: &str = "SELECT * FROM read_parquet('obs/*.parquet') ORDER BY \"TEMP\"";
    let cold = query(&runtime, ROWS).await;
    let (hits, misses) = delta(SchemaCacheCounters::default(), counters(&runtime));
    assert_eq!((hits, misses), (0, 3), "an empty cache answers nothing");

    query(&runtime, "ANALYZE FILES").await;
    let store = runtime.file_stats().unwrap().store().clone();
    assert_eq!(
        store.schema_cache().num_entries().unwrap(),
        3,
        "the pass kept a schema per file"
    );
    assert_eq!(
        store.schema_cache().num_schemas().unwrap(),
        1,
        "three identical schemas share one blob"
    );

    let before = counters(&runtime);
    let warm = query(&runtime, ROWS).await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (3, 0),
        "every file answered from the cache"
    );
    assert_eq!(warm, cold, "and the query returns what it always did");
}

/// netCDF is the format the issue measured, and the one the cache exists for:
/// its inference is an HDF5 metadata walk rather than a footer read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn netcdf_files_answer_from_the_cache_too() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..3 {
        write_netcdf(&root.path().join(format!("datasets/argo/{i}.nc")), i as f64);
    }
    let runtime = runtime(root.path(), true).await;
    query(&runtime, "ANALYZE FILES").await;

    let before = counters(&runtime);
    let rows = query(&runtime, "SELECT \"TEMP\" FROM read_netcdf('argo/*.nc')").await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (3, 0),
        "netCDF opened no file to learn its own schema"
    );
    assert!(rows.contains("TEMP"), "{rows}");
}

/// The point of keying per file rather than per table: a collection that gained
/// one file derives one schema, not the whole collection's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_file_costs_one_derivation() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..4 {
        write_parquet(
            &root.path().join(format!("datasets/obs/{i}.parquet")),
            i as f64,
        );
    }
    let runtime = runtime(root.path(), true).await;
    query(&runtime, "ANALYZE FILES").await;

    // A file arrives after the pass, so nothing has recorded it.
    write_parquet(&root.path().join("datasets/obs/4.parquet"), 4.0);

    let before = counters(&runtime);
    query(&runtime, "SELECT * FROM read_parquet('obs/*.parquet')").await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (4, 1),
        "four answered, one derived"
    );
}

/// A file rewritten after its analysis must be derived again. Serving its old
/// schema is the one failure that would make this wrong rather than slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewritten_file_is_derived_again() {
    let root = tempfile::tempdir().unwrap();
    let changing = root.path().join("datasets/obs/0.parquet");
    write_parquet(&changing, 0.0);
    write_parquet(&root.path().join("datasets/obs/1.parquet"), 1.0);

    let runtime = runtime(root.path(), true).await;
    query(&runtime, "ANALYZE FILES").await;

    // The same path, holding a column it did not hold before.
    std::fs::create_dir_all(changing.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "SALINITY",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![35.0, 36.0]))],
    )
    .unwrap();
    let file = std::fs::File::create(&changing).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let before = counters(&runtime);
    let rows = query(&runtime, "SELECT * FROM read_parquet('obs/*.parquet')").await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (1, 1),
        "the unchanged file answered, the rewritten one did not"
    );
    assert!(
        rows.contains("SALINITY"),
        "the table must report the file's new column:\n{rows}"
    );
}

/// The switch takes the cache out of the path entirely, and the query is
/// unaffected. This is the way back if the cache ever misbehaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_switch_leaves_the_query_alone() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..3 {
        write_parquet(
            &root.path().join(format!("datasets/obs/{i}.parquet")),
            i as f64,
        );
    }

    let runtime = runtime(root.path(), false).await;
    query(&runtime, "ANALYZE FILES").await;

    let store = runtime.file_stats().unwrap().store().clone();
    assert_eq!(
        store.schema_cache().num_entries().unwrap(),
        0,
        "the pass wrote nothing"
    );

    let before = counters(&runtime);
    let rows = query(
        &runtime,
        "SELECT \"TEMP\" FROM read_parquet('obs/*.parquet')",
    )
    .await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (0, 3),
        "every file is derived, as it was before this cache existed"
    );
    assert!(rows.contains("TEMP"), "{rows}");
}

/// A read that names dimensions is not cached, and still answers.
///
/// `read_dimensions` decides which variables appear, so the same file has one
/// schema per dimension set. Carrying that set in the key is the follow-up the
/// `TODO(#367)` notes name; until then such a read derives its schema, exactly
/// as it did before the cache existed. What must not happen is the default
/// read's entry answering for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dimension_projected_read_is_not_cached() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..3 {
        write_netcdf(&root.path().join(format!("datasets/argo/{i}.nc")), i as f64);
    }
    let runtime = runtime(root.path(), true).await;
    query(&runtime, "ANALYZE FILES").await;

    // The default read is cached, as ever.
    let before = counters(&runtime);
    query(&runtime, "SELECT \"TEMP\" FROM read_netcdf('argo/*.nc')").await;
    assert_eq!(delta(before, counters(&runtime)), (3, 0));

    // Naming a dimension takes the whole read off the cache: no hits, and no
    // misses either, because nothing is asked.
    let before = counters(&runtime);
    let rows = query(
        &runtime,
        // `obs` is the flat unlimited dimension the default encoder writes.
        "SELECT \"TEMP\" FROM read_netcdf('argo/*.nc', ['obs'])",
    )
    .await;
    assert_eq!(
        delta(before, counters(&runtime)),
        (0, 0),
        "a dimension-projected read never consults the cache"
    );
    assert!(rows.contains("TEMP"), "and it still answers:\n{rows}");
}
