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
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;

/// File statistics on, but with the timer far enough out that every pass in
/// these tests is one this code asked for.
fn enabled() -> FileStatsConfig {
    FileStatsConfig {
        enable: true,
        interval_secs: 3_600,
        // Each test drives its own passes; the flag has its own test.
        on_startup: false,
        concurrency: 2,
        batch_files: 100,
        target_group_files: 100,
        min_group_files: 2,
        prefix_depth: None,
        scan_prefix: String::new(),
        discovery_chunk: 50,
        schema_cache: true,
    }
}

/// File statistics off. The default is on, so a test of the off state sets it.
fn disabled() -> FileStatsConfig {
    FileStatsConfig {
        enable: false,
        ..enabled()
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

/// A NetCDF-4 file whose TEMP column spans `[min, max]`.
///
/// A NetCDF-4 file *is* an HDF5 file, so the caller picks the extension and with
/// it which format reads the result: `.nc` is netCDF, `.h5` and `.hdf5` are the
/// HDF5 format over the very same bytes.
fn write_netcdf4(path: &Path, min: f64, max: f64) {
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
            Arc::new(Float64Array::from(vec![min, max])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    let mut writer = ArrowRecordBatchWriter::<DefaultEncoder>::new(path, schema).unwrap();
    writer.write_record_batch(batch).unwrap();
    writer.finish().unwrap();
}

/// The plain HDF5 fixture shipped with the HDF5 reader: `station_id` spans
/// 11..33, and its datasets live two group levels deep.
fn copy_hdf5(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/nested-groups.h5");
    std::fs::copy(fixture, path).expect("copy the hdf5 fixture");
}

/// The collector records ranges for HDF5 only on the pure-Rust reader.
///
/// It resolves a format by file extension and calls `infer_stats`, so an HDF5
/// file follows the same rule netCDF does: under netcdf-c the format reports
/// unknown — every call there queues on a process-global lock — and under the
/// Rust reader it computes. The pass succeeds either way; only the ranges
/// differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hdf5_ranges_need_the_rust_reader() {
    use beacon_arrow_hdf5::Hdf5Config;

    async fn analyze(use_rust_reader: bool) -> u32 {
        let root = tempfile::tempdir().unwrap();
        copy_hdf5(&root.path().join("datasets/obs/nested.h5"));

        let runtime = builder(root.path(), enabled())
            .with_hdf5_config(Hdf5Config {
                use_rust_reader,
                ..Hdf5Config::default()
            })
            .build()
            .await
            .unwrap();
        let service = runtime.file_stats().expect("the subsystem is enabled");

        let pass = service.run_once().await.unwrap();
        assert_eq!(pass.discovered, 1, "the listing found the .h5 file");
        assert_eq!(pass.analyzed, 1, "and analyzed it: {pass:?}");
        assert_eq!(pass.failed, 0);

        let store = service.store();
        let id = store
            .registry()
            .file_id("obs/nested.h5")
            .unwrap()
            .expect("the file was registered");
        let record = store.registry().record(id).unwrap().unwrap();
        assert_eq!(
            record.format, "hdf5",
            "the extension resolved to the HDF5 format"
        );
        record.column_count
    }

    let with_rust_reader = analyze(true).await;
    assert!(
        with_rust_reader > 0,
        "the Rust reader must record a range for station_id"
    );

    let with_netcdf_c = analyze(false).await;
    assert_eq!(
        with_netcdf_c, 0,
        "netcdf-c reports unknown, so no column carries a range"
    );
}

/// Copy the bundled Zarr v3 store under `dir`, keeping its layout.
///
/// A zarr store is a directory, not a file, so the listing reports every object
/// in it. Only the store's top-level `zarr.json` resolves to a group with a
/// dataset behind it; the rest is what the collector has to shrug off.
fn copy_zarr(dir: &Path) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-zarr/test_files/gridded-example.zarr");
    for entry in walkdir(&fixture) {
        let relative = entry.strip_prefix(&fixture).unwrap();
        let target = dir.join(relative);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(&entry, &target).expect("copy the zarr fixture");
    }
}

/// Every file under `root`, recursively.
fn walkdir(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// Write a minimal Zarr v3 store at `dir` holding one `lat` coordinate.
///
/// Hand-written rather than built through `zarrs`: the `bytes` codec makes a
/// chunk a plain little-endian array, so the whole store is two JSON files and
/// sixteen bytes, and the test stays readable.
fn write_zarr_lat_store(dir: &Path, values: &[f32]) {
    std::fs::create_dir_all(dir.join("lat/c")).unwrap();
    std::fs::write(
        dir.join("zarr.json"),
        r#"{"zarr_format":3,"node_type":"group","attributes":{}}"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("lat/zarr.json"),
        format!(
            r#"{{"zarr_format":3,"node_type":"array","shape":[{n}],"data_type":"float32",
                 "chunk_grid":{{"name":"regular","configuration":{{"chunk_shape":[{n}]}}}},
                 "chunk_key_encoding":{{"name":"default","configuration":{{"separator":"/"}}}},
                 "fill_value":0.0,
                 "codecs":[{{"name":"bytes","configuration":{{"endian":"little"}}}}],
                 "attributes":{{"units":"degrees_north"}},
                 "dimension_names":["lat"],"storage_transformers":[]}}"#,
            n = values.len()
        ),
    )
    .unwrap();
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    std::fs::write(dir.join("lat/c/0"), bytes).unwrap();
}

/// The point of the ranges: a `WHERE` on a coordinate drops the stores that
/// cannot hold a matching row, before any chunk is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zarr_scan_prunes_on_a_coordinate() {
    let root = tempfile::tempdir().unwrap();
    write_zarr_lat_store(
        &root.path().join("datasets/sst/south.zarr"),
        &[0.0, 1.0, 2.0, 3.0],
    );
    write_zarr_lat_store(
        &root.path().join("datasets/sst/north.zarr"),
        &[80.0, 81.0, 82.0, 83.0],
    );

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let analyzed = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT lat FROM read_zarr('sst/') WHERE lat > 50",
    )
    .await;

    assert!(
        analyzed.contains("file_stats_files_considered=2"),
        "both stores should reach the scan:\n{analyzed}"
    );
    assert!(
        analyzed.contains("file_stats_files_pruned=1"),
        "the southern store cannot hold lat > 50 and must be dropped:\n{analyzed}"
    );
}

/// A zarr store records ranges for its coordinates, and none for its grids.
///
/// This is the acceptance the format exists to meet: `column_count` above zero
/// for a store, so a `WHERE` on a coordinate can prune it. The switch turns it
/// back off, and then the store analyzes successfully with nothing in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zarr_records_coordinate_ranges() {
    use beacon_arrow_zarr::ZarrConfig;

    async fn analyze(enable_statistics: bool) -> u32 {
        let root = tempfile::tempdir().unwrap();
        copy_zarr(&root.path().join("datasets/sst/gridded.zarr"));

        let runtime = builder(root.path(), enabled())
            .with_zarr_config(ZarrConfig { enable_statistics })
            .build()
            .await
            .unwrap();
        let service = runtime.file_stats().expect("the subsystem is enabled");
        service.run_once().await.unwrap();

        let store = service.store();
        let id = store
            .registry()
            .file_id("sst/gridded.zarr/zarr.json")
            .unwrap()
            .expect("the store's metadata was registered");
        let record = store.registry().record(id).unwrap().unwrap();
        assert_eq!(
            record.format, "zarr",
            "zarr.json resolved to the Zarr format"
        );
        record.column_count
    }

    assert!(
        analyze(true).await > 0,
        "the coordinates lat, lon and time must each carry a range"
    );
    assert_eq!(
        analyze(false).await,
        0,
        "with statistics off no column carries a range"
    );
}

/// A NetCDF-4 file carries ranges under either HDF5 extension.
///
/// The extension picks the format, not the reader's opinion of the bytes: `.h5`
/// and `.hdf5` both resolve to the HDF5 format, and with the Rust reader on it
/// reads a NetCDF-4 container as readily as a plain HDF5 one. Both files here
/// hold the same two rank-1 variables, and both must record a range for each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_netcdf4_file_under_an_hdf5_extension_records_its_ranges() {
    use beacon_arrow_hdf5::Hdf5Config;

    let root = tempfile::tempdir().unwrap();
    write_netcdf4(&root.path().join("datasets/obs/cold.h5"), 0.0, 5.0);
    write_netcdf4(&root.path().join("datasets/obs/hot.hdf5"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled())
        .with_hdf5_config(Hdf5Config {
            use_rust_reader: true,
            ..Hdf5Config::default()
        })
        .build()
        .await
        .unwrap();
    let service = runtime.file_stats().expect("the subsystem is enabled");

    let pass = service.run_once().await.unwrap();
    assert_eq!(pass.analyzed, 2, "both files were read: {pass:?}");
    assert_eq!(pass.failed, 0);

    let store = service.store();
    for path in ["obs/cold.h5", "obs/hot.hdf5"] {
        let id = store
            .registry()
            .file_id(path)
            .unwrap()
            .unwrap_or_else(|| panic!("{path} was registered"));
        let record = store.registry().record(id).unwrap().unwrap();
        assert_eq!(record.format, "hdf5", "{path} resolved to the HDF5 format");
        assert!(
            record.column_count >= 2,
            "{path} must carry a range for TEMP and for DEPTH: {record:?}"
        );
    }

    // And the ranges are the files' own, not a merged one.
    let segments = store.column_stats_by_name("TEMP", (0, 10)).await.unwrap();
    let mut ranges: Vec<(f64, f64)> = segments
        .iter()
        .flat_map(|segment| {
            let min = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(
                segment.min.as_ref(),
            );
            let max = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(
                segment.max.as_ref(),
            );
            (0..segment.len()).map(move |i| (min.value(i), max.value(i)))
        })
        .collect();
    ranges.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(ranges, vec![(0.0, 5.0), (90.0, 100.0)]);
}

/// The point of recording them: a predicate the ranges rule out leaves the file
/// out of the scan.
///
/// An HDF5 scan is not a bare `DataSourceExec`. Its arrays reach the plan
/// encoded, so the format returns `NdBroadcastExec(NdSourceExec(scan))`, and the
/// file list under those nodes is what pruning rewrites.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_drops_hdf5_files_from_the_scan() {
    use beacon_arrow_hdf5::Hdf5Config;

    let root = tempfile::tempdir().unwrap();
    write_netcdf4(&root.path().join("datasets/obs/cold.h5"), 0.0, 5.0);
    write_netcdf4(&root.path().join("datasets/obs/mild.h5"), 20.0, 25.0);
    write_netcdf4(&root.path().join("datasets/obs/hot.h5"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled())
        .with_hdf5_config(Hdf5Config {
            use_rust_reader: true,
            ..Hdf5Config::default()
        })
        .build()
        .await
        .unwrap();
    let pass = runtime.file_stats().unwrap().run_once().await.unwrap();
    assert_eq!(pass.analyzed, 3);

    let all = explain(&runtime, "SELECT * FROM read_hdf5('obs/*.h5')").await;
    assert_eq!(
        files_in_plan(&all),
        3,
        "no predicate reads everything:\n{all}"
    );

    let hot = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM read_hdf5('obs/*.h5') WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(
        counter(&hot, "file_stats_files_pruned"),
        2,
        "only hot.h5 can hold a TEMP above 80:\n{hot}"
    );
    // Identity is proven by reading: hot.h5's rows are 90 and 100.

    // Pruning changed which files are opened, not what the query answers.
    let rows = query(
        &runtime,
        "SELECT \"TEMP\" FROM read_hdf5('obs/*.h5') WHERE \"TEMP\" > 80 ORDER BY \"TEMP\"",
    )
    .await;
    assert!(rows.contains("90.0"), "{rows}");
    assert!(rows.contains("100.0"), "{rows}");
    assert!(!rows.contains("25.0"), "{rows}");

    // And a predicate no file can match leaves a scan with no files at all. The
    // nd nodes above it have to rebuild over that, so this is the shape worth
    // pinning: an empty scan, not an error.
    let none = query(
        &runtime,
        "SELECT \"TEMP\" FROM read_hdf5('obs/*.h5') WHERE \"TEMP\" > 1000",
    )
    .await;
    assert!(!none.contains("90.0"), "{none}");
    let plan = explain(
        &runtime,
        "SELECT * FROM read_hdf5('obs/*.h5') WHERE \"TEMP\" > 1000",
    )
    .await;
    assert_eq!(plan.matches(".h5").count(), 0, "no file survives:\n{plan}");
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
    let temp = store.column_stats_by_name("TEMP", (0, 10)).await.unwrap();
    let rows: usize = temp.iter().map(|segment| segment.len()).sum();
    assert_eq!(rows, 3, "every file contributed a TEMP row");
}

/// `on_startup` is what a restarted server needs: the timer's first pass is one
/// interval away and the interval starts again on every boot, so a server that
/// restarts more often than that never collects anything.
///
/// The collection is spawned, not awaited, so the runtime is usable immediately
/// and the test waits for the queue to drain rather than for `build`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_startup_collects_without_waiting_for_the_timer() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/argo/b.parquet"), 90.0, 100.0);

    let config = FileStatsConfig {
        on_startup: true,
        // An hour away, so nothing but the startup collection can do this work.
        interval_secs: 3_600,
        ..enabled()
    };
    let runtime = builder(root.path(), config).build().await.unwrap();
    let store = runtime.file_stats().unwrap().store().clone();

    // Poll rather than sleep a fixed time: the pass is a spawned task.
    let mut analyzed = 0;
    for _ in 0..100 {
        analyzed = store
            .registry()
            .scan_records()
            .unwrap()
            .iter()
            .filter(|(_, record)| record.state == beacon_file_stats::FileState::Analyzed)
            .count();
        if analyzed == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        analyzed, 2,
        "both files analyzed without a tick of the timer"
    );
    assert_eq!(store.num_segments().await, 1);
}

/// Without the flag, boot collects nothing: the first pass is one interval away.
/// This is the default, and it keeps startup free for queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_flag_boot_collects_nothing() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let store = runtime.file_stats().unwrap().store().clone();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert!(
        store.registry().scan_records().unwrap().is_empty(),
        "discovery runs inside a pass, and no pass has run"
    );
    assert_eq!(store.num_segments().await, 0);
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

    let runtime = builder(root.path(), disabled()).build().await.unwrap();
    assert!(
        runtime.file_stats().is_none(),
        "the switch is off, so no service is built"
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

// ── scan-time pruning ───────────────────────────────────────────────────────

/// The number of files a plan will actually open.
fn files_in_plan(explain: &str) -> usize {
    // An nd scan does not print a file list. It plans one standing entry per
    // partition, pointing at a queue that holds the files, and states the count
    // in the entry: `nd-morsel-scan/3-files`. That entry repeats once per
    // partition, so read the count rather than counting occurrences.
    if let Some(files) = morsel_scan_files(explain) {
        return files;
    }

    // Pruning runs before the scan is built, so the file groups the plan prints
    // are the files it will read. Counting the extensions in them is crude, but
    // it is what the plan actually says.
    [".parquet", ".h5", ".nc", "zarr.json"]
        .iter()
        .map(|extension| explain.matches(extension).count())
        .sum()
}

/// The file count an nd scan states in its standing entry, when it has one.
///
/// `None` for a plan that lists its files: a parquet scan, or an nd scan that
/// pruning left with no file at all.
fn morsel_scan_files(explain: &str) -> Option<usize> {
    const MARKER: &str = "nd-morsel-scan/";
    let rest = &explain[explain.find(MARKER)? + MARKER.len()..];
    rest[..rest.find("-files")?].parse().ok()
}

/// One metric value, expanded from the abbreviated form `EXPLAIN ANALYZE` uses.
///
/// DataFusion prints a large count in SI style: `2000` is `2.00 K` and `99990`
/// is `99.99 K`. Reading digits until the first non-digit would stop at the
/// decimal point and report `2`, which looks like a plausible small number
/// rather than a parse failure. The abbreviation also rounds to four
/// significant digits, so a caller checking a large count has to allow for it.
fn parse_metric_value(rest: &str) -> usize {
    let digits: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let Ok(value) = digits.parse::<f64>() else {
        return 0;
    };
    let scale = match rest[digits.len()..].trim_start().chars().next() {
        Some('K') => 1_000.0,
        Some('M') => 1_000_000.0,
        Some('G') => 1_000_000_000.0,
        _ => 1.0,
    };
    (value * scale).round() as usize
}

/// A metric's value from `EXPLAIN ANALYZE` output.
///
/// Pruning happens while the scan reads, so these counters — not the plan
/// text — are where the numbers live.
fn counter(analyzed: &str, name: &str) -> usize {
    let marker = format!("{name}=");
    analyzed
        .split(&marker)
        .skip(1)
        .map(parse_metric_value)
        .sum()
}

async fn explain(runtime: &Runtime, sql: &str) -> String {
    let batches = runtime
        .run_query(Query::sql(format!("EXPLAIN {sql}")), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN {sql}: {e}"))
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

/// The whole point: a predicate the statistics rule out must leave the file out
/// of the plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_drops_files_from_the_scan() {
    let root = tempfile::tempdir().unwrap();
    // Three files with disjoint TEMP ranges.
    write_parquet(&root.path().join("datasets/obs/cold.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/mild.parquet"), 20.0, 25.0);
    write_parquet(&root.path().join("datasets/obs/hot.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let pass = runtime.file_stats().unwrap().run_once().await.unwrap();
    assert_eq!(pass.analyzed, 3);

    let all = explain(&runtime, "SELECT * FROM read_parquet('obs/*.parquet')").await;
    assert_eq!(
        files_in_plan(&all),
        3,
        "no predicate reads everything:\n{all}"
    );

    // Pruning runs while the scan reads, so the counters say what it dropped.
    let hot = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(
        counter(&hot, "file_stats_files_considered"),
        3,
        "every file reaches the scan:\n{hot}"
    );
    assert_eq!(
        counter(&hot, "file_stats_files_pruned"),
        2,
        "only hot.parquet can hold a TEMP above 80:\n{hot}"
    );

    // And identity is proven by reading: hot.parquet's rows are 90 and 100.
    let rows = query(
        &runtime,
        "SELECT \"TEMP\" FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80 ORDER BY \"TEMP\"",
    )
    .await;
    assert!(
        rows.contains("90.0") && rows.contains("100.0"),
        "and it must be that one:\n{rows}"
    );
}

/// Pruning must never remove a file that could match. A predicate every file
/// could satisfy leaves the plan alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_every_file_could_match_drops_nothing() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 20.0, 25.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let plan = explain(
        &runtime,
        "SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > -1000",
    )
    .await;
    assert_eq!(files_in_plan(&plan), 2, "both files can match:\n{plan}");
}

/// A file the collector has never seen has no statistics, so nothing may rule it
/// out. This is the case a partially-backfilled store is in constantly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanalyzed_file_is_never_dropped() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/analyzed.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    // Appears after the pass, so it is in no segment.
    write_parquet(&root.path().join("datasets/obs/fresh.parquet"), 0.0, 5.0);

    let analyzed = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(
        counter(&analyzed, "file_stats_files_considered"),
        2,
        "both files reach the scan:\n{analyzed}"
    );
    assert_eq!(
        counter(&analyzed, "file_stats_files_pruned"),
        1,
        "the analyzed one is ruled out, the one with no statistics survives:\n{analyzed}"
    );
}

/// With the subsystem off there is no store to prune against, so every file is
/// read. Correct, just slower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_pruned_when_the_subsystem_is_off() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/cold.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/hot.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), disabled()).build().await.unwrap();

    let plan = explain(
        &runtime,
        "SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(
        files_in_plan(&plan),
        2,
        "no statistics means no pruning:\n{plan}"
    );
}

// ── the SQL surface ─────────────────────────────────────────────────────────

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

/// `beacon.system.file_stats` is how an operator sees a background process at
/// all. Without it, a subsystem that analyzes everything and stores nothing
/// looks exactly like one that works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_view_shows_what_the_subsystem_knows() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let rows = query(
        &runtime,
        "SELECT path, state, format, column_count FROM beacon.system.file_stats ORDER BY path",
    )
    .await;
    assert!(rows.contains("obs/a.parquet"), "{rows}");
    assert!(rows.contains("Analyzed"), "{rows}");
    assert!(rows.contains("parquet"), "{rows}");

    // The diagnosis query from the module docs: which formats yield nothing.
    let barren = query(
        &runtime,
        "SELECT format, count(*) AS files, \
         sum(CASE WHEN column_count = 0 THEN 1 ELSE 0 END) AS barren \
         FROM beacon.system.file_stats GROUP BY format",
    )
    .await;
    assert!(barren.contains("parquet"), "{barren}");
    assert!(barren.contains("| 2 "), "two files analyzed:\n{barren}");
}

/// The segments view answers the question nothing else can on a live node:
/// whether the batching is producing narrow segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_segments_view_shows_the_batching() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/ctd/b.parquet"), 0.0, 5.0);

    // `min_group_files` must allow a split, or the rule correctly keeps a
    // two-file batch whole and there is nothing to observe.
    let splitting = FileStatsConfig {
        min_group_files: 1,
        ..enabled()
    };
    let runtime = builder(root.path(), splitting).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let rows = query(
        &runtime,
        "SELECT segment, num_files, num_columns FROM beacon.system.file_stats_segments \
         ORDER BY seq",
    )
    .await;
    // Two roots, so two segments, each holding one file's columns.
    assert_eq!(rows.matches("segment-").count(), 2, "{rows}");
}

/// Runs `sql` and returns the error it produced. Panics if it succeeded.
async fn query_error(runtime: &Runtime, sql: &str) -> String {
    match runtime
        .run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await
    {
        Ok(_) => panic!("{sql} should have failed"),
        Err(error) => error.to_string(),
    }
}

/// `beacon.system.file_stats` counts a file's columns; this shows their ranges.
/// A `column_count` of 200 says nothing about whether the bounds are usable, and
/// the bounds are what pruning runs on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_reports_the_recorded_range_of_one_file() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let rows = query(
        &runtime,
        "SELECT column, data_type, min, max, row_count \
         FROM file_statistics('obs/a.parquet') ORDER BY column",
    )
    .await;

    assert!(rows.contains("TEMP"), "{rows}");
    assert!(rows.contains("DEPTH"), "{rows}");
    // The range the file was written with, rendered in the column's own type.
    assert!(rows.contains("0.0"), "the recorded minimum:\n{rows}");
    assert!(rows.contains("5.0"), "the recorded maximum:\n{rows}");
}

/// A dataset is usually a directory, so the function takes a glob and reports
/// every file it matches, keeping the path on each row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_globs_a_dataset_directory() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 90.0, 100.0);
    write_parquet(&root.path().join("datasets/other/c.parquet"), 0.0, 1.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let rows = query(
        &runtime,
        "SELECT path, count(*) AS columns FROM file_statistics('obs/*') \
         GROUP BY path ORDER BY path",
    )
    .await;

    assert!(rows.contains("obs/a.parquet"), "{rows}");
    assert!(rows.contains("obs/b.parquet"), "{rows}");
    assert!(
        !rows.contains("other/c.parquet"),
        "the glob must not widen:\n{rows}"
    );
}

/// A path the registry never saw is a typo far more often than it is a question,
/// and an empty result would read as "this file has no statistics".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_rejects_a_path_it_does_not_know() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let error = query_error(
        &runtime,
        "SELECT * FROM file_statistics('obs/nope.parquet')",
    )
    .await;
    assert!(error.contains("nope.parquet"), "{error}");
    assert!(error.contains("beacon.system.file_stats"), "{error}");
}

/// With the subsystem off the function says so, rather than returning the empty
/// result the views correctly return. The views describe a state; a call asks a
/// question, and "no statistics exist anywhere" is the answer to a different one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_says_so_when_the_subsystem_is_off() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), disabled()).build().await.unwrap();

    let error = query_error(&runtime, "SELECT * FROM file_statistics('obs/a.parquet')").await;
    assert!(error.contains("BEACON_FILE_STATS_ENABLE"), "{error}");
}

/// The function reports the value ranges of files, which is data. It belongs to
/// the super-user for the same reason `beacon.system` does — and, like that gate,
/// it cannot wait for grant enforcement to be turned on. A function carries no
/// schema name, so the name-based gate cannot see it: this pins the provider one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_function_is_super_user_only() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    // Enforcement off, the default posture: an ordinary read would be allowed.
    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let denied = runtime
        .run_query(
            Query::sql("SELECT * FROM file_statistics('obs/a.parquet')".to_string()),
            AuthIdentity::empty(),
        )
        .await;
    assert!(
        denied
            .as_ref()
            .is_err_and(|e| e.to_string().contains("restricted to the super-user")),
        "a non-super-user must not read recorded ranges, got: {:?}",
        denied.as_ref().err().map(|e| e.to_string())
    );
}

/// Both views are empty rather than an error when the subsystem never started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_views_are_empty_when_the_subsystem_is_off() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), disabled()).build().await.unwrap();

    let rows = query(&runtime, "SELECT count(*) FROM beacon.system.file_stats").await;
    assert!(rows.contains("| 0 "), "no store means no rows:\n{rows}");
}

/// `ANALYZE FILES` drains the queue now instead of waiting for the timer, and
/// reports what it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_runs_a_pass_on_demand() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 9.0, 9.5);

    // An interval long enough that the timer cannot be what did the work.
    let runtime = builder(root.path(), enabled()).build().await.unwrap();

    let report = query(&runtime, "ANALYZE FILES").await;
    assert!(report.contains("discovered"), "{report}");
    assert!(report.contains("| 2 "), "two files analyzed:\n{report}");

    let rows = query(
        &runtime,
        "SELECT count(*) FROM beacon.system.file_stats WHERE state = 'Analyzed'",
    )
    .await;
    assert!(rows.contains("| 2 "), "{rows}");
}

/// A prefix restricts it, so an operator can validate one corner of a store
/// before turning the subsystem loose on all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_can_be_restricted_to_a_prefix() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/argo/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/ctd/b.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    query(&runtime, "ANALYZE FILES 'argo/'").await;

    let rows = query(
        &runtime,
        "SELECT path FROM beacon.system.file_stats ORDER BY path",
    )
    .await;
    assert!(rows.contains("argo/a.parquet"), "{rows}");
    assert!(
        !rows.contains("ctd/b.parquet"),
        "the other prefix was never discovered:\n{rows}"
    );
}

/// FORCE is the way back from a reader that could not produce ranges. Without
/// it, an already-analyzed file is never re-queued, because its content did not
/// change -- only what Beacon can read from it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_force_re_analyzes() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    query(&runtime, "ANALYZE FILES").await;

    let store = runtime.file_stats().unwrap().store();
    let id = store.registry().file_id("obs/a.parquet").unwrap().unwrap();
    assert_eq!(store.registry().record(id).unwrap().unwrap().stats_epoch, 1);

    // Without FORCE nothing is re-queued: the file has not changed.
    query(&runtime, "ANALYZE FILES").await;
    assert_eq!(store.registry().record(id).unwrap().unwrap().stats_epoch, 1);

    let report = query(&runtime, "ANALYZE FILES FORCE").await;
    assert!(report.contains("| 1 "), "one file requeued:\n{report}");
    assert_eq!(
        store.registry().record(id).unwrap().unwrap().stats_epoch,
        2,
        "FORCE rewrote its statistics"
    );
}

/// With the subsystem off the statement says so, rather than reporting a pass
/// that did nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_says_when_the_subsystem_is_off() {
    let root = tempfile::tempdir().unwrap();
    let runtime = builder(root.path(), disabled()).build().await.unwrap();

    // The statement plans fine; the refusal comes when the stream is polled, so
    // the error has to be collected rather than awaited.
    let error = match runtime
        .run_query(
            Query::sql("ANALYZE FILES".to_string()),
            AuthIdentity::system(),
        )
        .await
    {
        Err(error) => error.to_string(),
        Ok(result) => result
            .into_record_stream()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default(),
    };
    assert!(
        error.contains("BEACON_FILE_STATS_ENABLE"),
        "the error should name the switch: {error}"
    );
}

/// A pruned scan reports what it did, where people already look.
///
/// The plan alone shows the surviving files but never the ratio, so "pruning ran
/// and kept everything" and "pruning never ran" read identically. The counters
/// separate them, and `columns_used` separates a third case: a predicate whose
/// columns the store holds no statistics for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pruned_scan_reports_its_metrics() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/cold.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/mild.parquet"), 20.0, 25.0);
    write_parquet(&root.path().join("datasets/obs/hot.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let analyzed = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80",
    )
    .await;

    assert!(
        analyzed.contains("file_stats_files_considered=3"),
        "the scan should report how many files it started from:\n{analyzed}"
    );
    assert!(
        analyzed.contains("file_stats_files_pruned=2"),
        "and how many it dropped:\n{analyzed}"
    );
}

// ── the scan reads a listing and prunes as it goes ──────────────────────────

/// A `SELECT *` plans without touching a segment, and reads every file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_without_a_predicate_sets_up_no_pruning() {
    let root = tempfile::tempdir().unwrap();
    for i in 0..5 {
        write_parquet(
            &root.path().join(format!("datasets/obs/{i}.parquet")),
            i as f64,
            i as f64 + 1.0,
        );
    }

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let analyzed = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet')",
    )
    .await;
    assert_eq!(
        counter(&analyzed, "file_stats_files_considered"),
        0,
        "no predicate means no pruning to report:\n{analyzed}"
    );

    let rows = query(
        &runtime,
        "SELECT count(*) AS n FROM read_parquet('obs/*.parquet')",
    )
    .await;
    assert!(rows.contains("10"), "five files of two rows each:\n{rows}");
}

/// A predicate the statistics cannot answer must not set up pruning, which
/// would cost a segment read per chunk for nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_it_cannot_prune_on_sets_up_no_pruning() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    // TEMP is recorded, so the file that cannot match is dropped before the
    // scan is built and never appears in it.
    let known = explain(
        &runtime,
        "SELECT * FROM read_parquet('obs/*.parquet') WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(files_in_plan(&known), 1, "{known}");

    // A literal predicate names no column at all, so nothing is pruned and no
    // segment is read to find that out.
    let unknown = explain(
        &runtime,
        "SELECT * FROM read_parquet('obs/*.parquet') WHERE 1 = 1",
    )
    .await;
    assert_eq!(files_in_plan(&unknown), 2, "{unknown}");
}

/// A file the store lists but has never analyzed is read: the scan sees it
/// because it lists, and pruning cannot drop what it has no statistics for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_written_after_the_pass_is_read_at_once() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/a.parquet"), 0.0, 5.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    let before = query(
        &runtime,
        "SELECT count(*) AS n FROM read_parquet('obs/*.parquet')",
    )
    .await;
    assert!(before.contains('2'), "one file of two rows:\n{before}");

    // No discovery pass in between: the scan lists, so it sees the file now.
    write_parquet(&root.path().join("datasets/obs/b.parquet"), 90.0, 100.0);
    let after = query(
        &runtime,
        "SELECT count(*) AS n FROM read_parquet('obs/*.parquet')",
    )
    .await;
    assert!(
        after.contains('4'),
        "a file is queryable the moment it lands:\n{after}"
    );
}

/// A `CREATE EXTERNAL TABLE` prunes on the registry, the same as a `read_*`
/// scan does.
///
/// It did not always. An external table builds its own listing table to hold
/// the schema and options its DDL declares, and that table used to be the
/// provider — so it never reached the pruning path at all. It is wrapped now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_external_table_drops_files_from_the_scan() {
    let root = tempfile::tempdir().unwrap();
    write_parquet(&root.path().join("datasets/obs/cold.parquet"), 0.0, 5.0);
    write_parquet(&root.path().join("datasets/obs/mild.parquet"), 20.0, 25.0);
    write_parquet(&root.path().join("datasets/obs/hot.parquet"), 90.0, 100.0);

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let pass = runtime.file_stats().unwrap().run_once().await.unwrap();
    assert_eq!(pass.analyzed, 3);

    query(
        &runtime,
        "CREATE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'obs/'",
    )
    .await;

    let all = explain(&runtime, "SELECT * FROM obs").await;
    assert_eq!(
        files_in_plan(&all),
        3,
        "no predicate reads everything:\n{all}"
    );

    let hot = query(
        &runtime,
        "EXPLAIN ANALYZE SELECT * FROM obs WHERE \"TEMP\" > 80",
    )
    .await;
    assert_eq!(
        counter(&hot, "file_stats_files_considered"),
        3,
        "every file reaches the scan:\n{hot}"
    );
    assert_eq!(
        counter(&hot, "file_stats_files_pruned"),
        2,
        "only hot.parquet can hold a TEMP above 80:\n{hot}"
    );

    // Reading proves it kept the right one rather than merely the right count.
    let rows = query(
        &runtime,
        "SELECT \"TEMP\" FROM obs WHERE \"TEMP\" > 80 ORDER BY \"TEMP\"",
    )
    .await;
    assert!(
        rows.contains("90.0") && rows.contains("100.0"),
        "and it must be that one:\n{rows}"
    );
}

/// A partitioned external table still reads its partition columns.
///
/// The wrapper plans through the listing table rather than around it, which is
/// what keeps this working: a hand-built scan configuration would have to carry
/// the partition columns itself, and the earlier one declared it had none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_external_table_keeps_its_partition_columns() {
    let root = tempfile::tempdir().unwrap();
    // Hive layout: the year lives in the directory name, not in the files.
    write_parquet(
        &root.path().join("datasets/part/year=2023/a.parquet"),
        0.0,
        5.0,
    );
    write_parquet(
        &root.path().join("datasets/part/year=2024/b.parquet"),
        90.0,
        100.0,
    );

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    runtime.file_stats().unwrap().run_once().await.unwrap();

    query(
        &runtime,
        "CREATE EXTERNAL TABLE part STORED AS PARQUET \
         PARTITIONED BY (year) LOCATION 'part/'",
    )
    .await;

    // The partition column is in the schema and carries the directory's value.
    let years = query(&runtime, "SELECT DISTINCT year FROM part ORDER BY year").await;
    assert!(
        years.contains("2023") && years.contains("2024"),
        "both partition values should be readable:\n{years}"
    );

    // And a predicate on it selects by directory.
    let rows = query(
        &runtime,
        "SELECT \"TEMP\" FROM part WHERE year = '2024' ORDER BY \"TEMP\"",
    )
    .await;
    assert!(
        rows.contains("90.0") && rows.contains("100.0"),
        "only the 2024 partition should be read:\n{rows}"
    );
    assert!(
        !rows.contains("5.0"),
        "the 2023 partition should not appear:\n{rows}"
    );
}

/// Counts the files the registry records as analyzed.
fn num_analyzed(store: &beacon_file_stats::FileStatsStore) -> usize {
    store
        .registry()
        .scan_records()
        .unwrap()
        .iter()
        .filter(|(_, record)| record.state == beacon_file_stats::FileState::Analyzed)
        .count()
}

/// One tick drains the queue. It does not stop after `batch_files`.
///
/// The config below makes the difference visible: one file per batch, five
/// files, one second between ticks. A tick that took one batch would need five
/// ticks, so five seconds, and the deadline here is under three. A tick that
/// drains covers all five on the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tick_drains_the_queue_rather_than_taking_one_batch() {
    let root = tempfile::tempdir().unwrap();
    for (index, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        write_parquet(
            &root.path().join(format!("datasets/argo/{name}.parquet")),
            index as f64,
            index as f64 + 1.0,
        );
    }

    let config = FileStatsConfig {
        interval_secs: 1,
        batch_files: 1,
        // One file per segment too, so a batch really is a batch.
        target_group_files: 1,
        min_group_files: 1,
        ..enabled()
    };
    let runtime = builder(root.path(), config).build().await.unwrap();
    let store = runtime.file_stats().unwrap().store().clone();

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(2_900);
    let mut analyzed = 0;
    while std::time::Instant::now() < deadline {
        analyzed = num_analyzed(&store);
        if analyzed == 5 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(
        analyzed, 5,
        "the first tick should have drained all five files, not taken one batch"
    );
}

/// `ANALYZE FILES` says so when a pass is already running. It does not wait.
///
/// Nothing claims a file when it comes off the queue, so without the pass guard
/// both of these take the same three files and read every one of them twice. A
/// guard that waited instead would hold the statement for the length of the
/// running pass, which over a large archive is minutes of looking hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_says_when_a_pass_is_already_running() {
    let root = tempfile::tempdir().unwrap();
    for (index, name) in ["a", "b", "c"].iter().enumerate() {
        write_parquet(
            &root.path().join(format!("datasets/argo/{name}.parquet")),
            index as f64,
            index as f64 + 1.0,
        );
    }

    let runtime = builder(root.path(), enabled()).build().await.unwrap();
    let service = runtime.file_stats().unwrap().clone();
    let other = service.clone();

    let (first, second) = tokio::join!(
        async move { service.analyze_now(None, false).await },
        async move { other.analyze_now(None, false).await },
    );

    let (pass, refused) = match (first, second) {
        (Ok(pass), Err(refused)) => (pass, refused),
        (Err(refused), Ok(pass)) => (pass, refused),
        (Ok(_), Ok(_)) => panic!("both passes ran; the guard did not hold"),
        (Err(a), Err(b)) => panic!("neither pass ran: {a}, {b}"),
    };

    assert_eq!(pass.analyzed, 3, "the pass that got the guard did the work");
    let refused = refused.to_string();
    assert!(
        refused.contains("already running"),
        "the error should name the running pass: {refused}"
    );
    assert_eq!(num_analyzed(runtime.file_stats().unwrap().store()), 3);
}
