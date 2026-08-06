//! The file-statistics subsystem, driven through a real runtime.
//!
//! Everything below this point has been tested against fakes or against formats
//! in isolation. This is the first test where a `RuntimeBuilder` produces the
//! service, the service lists a real datasets store, a real format reads real
//! files, and the results land in the real `beacon.db`.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_common::FileStatsConfig;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;

/// File statistics on, but with the timer far enough out that every pass in
/// these tests is one this code asked for.
fn enabled() -> FileStatsConfig {
    FileStatsConfig {
        enable: true,
        interval_secs: 3_600,
        concurrency: 2,
        batch_files: 100,
        target_group_files: 100,
        min_group_files: 2,
        prefix_depth: None,
        scan_prefix: String::new(),
        discovery_chunk: 50,
    }
}

fn builder(root: &Path, file_stats: FileStatsConfig) -> RuntimeBuilder {
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
        .with_file_stats(file_stats)
}

/// A Parquet file whose TEMP column spans `[min, max]`.
fn write_parquet(path: &Path, min: f64, max: f64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, false),
        Field::new("DEPTH", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![min, max])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// The whole loop, through a runtime: discovery finds the files, the format
/// reads their footers, and the ranges come back out of the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pass_discovers_analyzes_and_stores() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/argo/b.parquet"), 90.0, 100.0);
    write_parquet(&root.path().join("datasets/ctd/c.parquet"), 40.0, 50.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let service = runtime
        .file_stats()
        .expect("the subsystem is enabled and the runtime has a database file");

    let pass = service.run_once().await.unwrap();
    assert_eq!(pass.discovered, 3, "the listing found every file");
    assert_eq!(pass.analyzed, 3);
    assert_eq!(pass.failed, 0);
    assert_eq!(pass.pending, 0, "the queue drained");

    // Grouping is derived, and argo/ and ctd/ are separate roots.
    assert_eq!(pass.segments, 2);

    let store = service.store();
    let id = store
        .registry()
        .file_id("argo/b.parquet")
        .unwrap()
        .expect("the file was registered");
    let record = store.registry().record(id).unwrap().unwrap();
    assert_eq!(record.format, "parquet");
    assert_eq!(record.num_rows, Some(2));
    assert_eq!(record.column_count, 2, "TEMP and DEPTH both carry ranges");

    // And the ranges are readable back out, per column.
    let temp = store
        .column_stats_by_name("TEMP", (0, 10))
        .await
        .unwrap();
    let rows: usize = temp.iter().map(|segment| segment.len()).sum();
    assert_eq!(rows, 3, "every file contributed a TEMP row");
}

/// A second pass over an unchanged store does nothing. The registry recognises
/// the files, so nothing re-queues and no segment is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_pass_over_unchanged_files_is_idle() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let service = runtime.file_stats().unwrap();

    let first = service.run_once().await.unwrap();
    assert_eq!(first.analyzed, 1);
    assert_eq!(first.segments, 1);

    let second = service.run_once().await.unwrap();
    assert_eq!(second.discovered, 1, "the listing still reports the file");
    assert_eq!(second.analyzed, 0, "but nothing needed re-analyzing");
    assert_eq!(second.segments, 0);
}

/// A file rewritten under us goes stale on the next discovery and is analyzed
/// again, into a newer segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_file_is_picked_up_by_the_next_pass() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("datasets/argo/a.parquet");
    write_parquet(&file, 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let service = runtime.file_stats().unwrap();
    service.run_once().await.unwrap();

    let store = service.store();
    let id = store.registry().file_id("argo/a.parquet").unwrap().unwrap();
    assert_eq!(store.registry().record(id).unwrap().unwrap().stats_epoch, 1);

    // Rewrite it with a different range, and a different size so the change is
    // visible without depending on mtime resolution.
    write_parquet(&file, 90.0, 100.0);
    std::fs::write(root.path().join("datasets/argo/pad.parquet"), b"x").ok();

    let pass = service.run_once().await.unwrap();
    assert!(
        pass.analyzed >= 1,
        "the rewritten file must be analyzed again, got {pass:?}"
    );
    let record = store.registry().record(id).unwrap().unwrap();
    assert_eq!(record.stats_epoch, 2, "its statistics were rewritten");
}

/// Off means off: no service, and nothing written under the statistics prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_subsystem_stays_out_of_the_way_when_disabled() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), FileStatsConfig::default())
        .build()
        .await
        .unwrap();
    assert!(
        runtime.file_stats().is_none(),
        "the default is off, so no service is built"
    );
}

/// A file the formats cannot read must not stop the pass, and must leave the
/// queue so it is not retried forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_file_fails_without_stopping_the_pass() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/good.parquet"), 0.0, 5.0);
    std::fs::write(
        root.path().join("datasets/argo/broken.parquet"),
        b"not parquet at all",
    )
    .unwrap();

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let service = runtime.file_stats().unwrap();

    let pass = service.run_once().await.unwrap();
    assert_eq!(pass.discovered, 2);
    assert_eq!(pass.analyzed, 1, "the readable file still went through");
    assert_eq!(pass.failed, 1);
    assert_eq!(pass.pending, 0, "the failure left the queue");
}

/// Statistics live in the one database file, so they survive a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statistics_survive_a_restart() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);

    {
        let runtime = builder(root.path(), enabled()).build().await.unwrap();
        runtime.file_stats().unwrap().run_once().await.unwrap();
    } // dropped: the timer stops and redb releases its lock

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let store = runtime.file_stats().unwrap().store();

    let id = store
        .registry()
        .file_id("argo/a.parquet")
        .unwrap()
        .expect("the registry came back with the database");
    assert_eq!(store.registry().record(id).unwrap().unwrap().stats_epoch, 1);
    assert_eq!(store.num_segments().await, 1, "the segment came back too");
}
