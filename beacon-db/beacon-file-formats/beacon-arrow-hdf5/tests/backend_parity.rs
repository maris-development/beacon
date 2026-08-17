//! The two HDF5 backends, side by side.
//!
//! An HDF5 table reads through netcdf-c or through the pure-Rust reader, and
//! the flag that picks between them is the whole point of this crate's second
//! reader. These tests hold the two to the same answer on a NetCDF-4 file —
//! which is an HDF5 file — and then exercise what only the Rust one can do:
//! a plain HDF5 file with a nested group, a compound dataset, and an object
//! read with no local copy.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use beacon_arrow_hdf5::{Hdf5Config, Hdf5FormatFactory};
use beacon_arrow_netcdf::datafusion::{
    options::NetcdfOptions, NetCDFFormatFactory, NetcdfConfig, ReaderBackend,
};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::common::stats::Precision;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

/// A NetCDF-4 file with a contiguous ragged layout, bundled with the netCDF
/// crate. Both crates read it, so it is not copied into either.
const WOD_FILE: &str = "wod_ctd_1964.nc";
/// A NetCDF-4 file with a chunked grid, packed variables and CF time.
/// The share minimum a scan takes from its session
/// (`repartition_file_min_size`), which these tests leave at the default.
const MIN_SHARE_SIZE: u64 = 10 * 1024 * 1024;

const GRIDDED_FILE: &str = "gridded-example.nc";
/// Plain HDF5: datasets two group levels deep, and no netCDF convention.
const NESTED_FILE: &str = "nested-groups.h5";
/// Plain HDF5: one compound dataset.
const COMPOUND_FILE: &str = "compound.h5";

/// Which reader a table reads through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    NetcdfC,
    Rust,
}

impl Backend {
    fn options(self) -> HashMap<String, String> {
        HashMap::from([(
            "backend".to_string(),
            match self {
                Backend::NetcdfC => "netcdf-c".to_string(),
                Backend::Rust => "rust".to_string(),
            },
        )])
    }
}

/// The absolute path of a bundled NetCDF-4 file.
///
/// Canonical, because `object_store::path::Path` rejects a `..` segment and
/// this one reaches into a sibling crate.
fn netcdf_file(name: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../beacon-arrow-netcdf/test_files")
        .join(name)
        .canonicalize()
        .unwrap_or_else(|e| panic!("the bundled netCDF fixture {name} exists: {e}"));

    strip_verbatim_prefix(path)
}

/// Drop the Windows verbatim prefix from a canonical path.
///
/// `canonicalize` returns `\\?\C:\...` on Windows, and `ListingTableUrl` cannot
/// turn one of those back into a file path: it parses, then panics in
/// `to_file_path`. Every test that registers a table from a canonical path needs
/// this. On every other platform, and on a UNC path, it does nothing.
fn strip_verbatim_prefix(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(rest) = text.strip_prefix(r"\\?\") else {
        return path;
    };
    // Only the drive form (`C:\...`) is safe to unwrap. `\\?\UNC\server\share`
    // would lose its leading slashes and stop naming the same place.
    let mut chars = rest.chars();
    let is_drive = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.next() == Some(':')
        && chars.next() == Some('\\');

    if is_drive {
        PathBuf::from(rest.to_string())
    } else {
        path
    }
}

/// The absolute path of a bundled plain-HDF5 file.
fn hdf5_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_files")
        .join(name)
}

/// The factory a runtime registers, on the runtime's default reader.
fn factory(default_reader: Backend) -> Hdf5FormatFactory {
    let listing = Arc::new(ListingFactory::dynamic());
    let inner = NetCDFFormatFactory::new(
        listing,
        std::env::temp_dir(),
        NetcdfOptions::default(),
        NetcdfConfig::default(),
    );
    Hdf5FormatFactory::new(
        inner,
        Hdf5Config {
            backend: match default_reader {
                Backend::NetcdfC => ReaderBackend::NetcdfC,
                Backend::Rust => ReaderBackend::Oxcdf,
            },
            ..Hdf5Config::default()
        },
    )
}

/// A single-partition session, so a scan yields rows in a stable order.
fn session() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                // `FastObjectTable` merges its schemas through this. A session
                // that skips `RuntimeBuilder` has to register it itself.
                .with_extension(
                    beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
                ),
        )
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

/// Register `path` as a table read on `backend`.
///
/// This goes through `create_with_native_root`, the path a `CREATE EXTERNAL
/// TABLE` takes, so netcdf-c gets the resolver it cannot work without and the
/// Rust reader is left to read through the object store.
async fn register(ctx: &SessionContext, table: &str, backend: Backend, path: &std::path::Path) {
    let listing = Arc::new(ListingFactory::dynamic());
    let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
    let format = factory(Backend::NetcdfC)
        .create_with_native_root(&ctx.state(), &backend.options(), &url, &listing)
        .unwrap_or_else(|e| panic!("build the {backend:?} format for {}: {e}", path.display()));

    let table_provider = FastObjectTable::try_new(&ctx.state(), format, vec![url])
        .await
        .unwrap_or_else(|e| panic!("register {} on {backend:?}: {e}", path.display()));
    ctx.register_table(table, Arc::new(table_provider)).unwrap();
}

/// A session with `target_partitions` partitions.
///
/// Nothing is done to the share minimum here, so it stays the session default
/// ([`MIN_SHARE_SIZE`]). Asking for partitions is therefore not the same as
/// getting a share: the file has to be large enough to earn one.
fn splitting_session(target_partitions: usize) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_extension(
            beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
        );

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

/// Run `sql` and concatenate the result into one batch.
///
/// A query that matches nothing still gives a batch, with the right schema and
/// no rows, so a comparison of two empty results is still a comparison.
async fn collect(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let frame = ctx.sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.unwrap();
    concat_batches(&schema, &batches).unwrap()
}

// ─── A NetCDF-4 file reads the same on both backends ────────────────────────

#[tokio::test]
async fn both_backends_infer_the_same_schema() {
    for file in [GRIDDED_FILE, WOD_FILE] {
        let ctx = session();
        register(&ctx, "netcdf_c", Backend::NetcdfC, &netcdf_file(file)).await;
        register(&ctx, "rust", Backend::Rust, &netcdf_file(file)).await;

        // Compare the Arrow schemas: a `DFSchema` also carries the table name,
        // which differs here by construction.
        let c = ctx
            .table("netcdf_c")
            .await
            .unwrap()
            .schema()
            .as_arrow()
            .clone();
        let rust = ctx.table("rust").await.unwrap().schema().as_arrow().clone();
        assert_eq!(rust, c, "schemas differ for {file}");
    }
}

/// A full scan of a gridded file: chunked storage, `scale_factor` packing and a
/// CF time axis all decode the same way on either backend.
#[tokio::test]
async fn both_backends_return_the_same_rows_for_a_gridded_file() {
    let ctx = session();
    register(
        &ctx,
        "netcdf_c",
        Backend::NetcdfC,
        &netcdf_file(GRIDDED_FILE),
    )
    .await;
    register(&ctx, "rust", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let rust = collect(&ctx, "SELECT analysed_sst, lat, lon, time FROM rust").await;
    assert!(rust.num_rows() > 0, "the scan must return rows");
    assert_eq!(
        rust,
        collect(&ctx, "SELECT analysed_sst, lat, lon, time FROM netcdf_c").await
    );
}

// ── Splitting one file across partitions ────────────────────────────────────

/// The partition count of the scan at the bottom of `plan`.
///
/// The root can carry more partitions than the scan does: DataFusion adds a
/// round-robin repartition above a single-partition scan, which hides whether
/// the scan itself was split. This looks at the scan, which is the thing
/// splitting changes.
fn scan_partitions(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    use datafusion::physical_plan::ExecutionPlanProperties;

    let mut node = plan.clone();
    while let Some(child) = node.children().first() {
        node = Arc::clone(child);
    }
    node.output_partitioning().partition_count()
}

/// A table is served by the reader it asked for, and only `oxcdf` can split.
///
/// The reader decides, not the format. `Hdf5Source` is the `oxcdf` source and
/// permits the split; a table on netcdf-c is served by `NetCDFSource`, which
/// declines because every netcdf-c call queues on one process-global mutex, so
/// shares of a file would run one at a time and pay for an extra open each.
///
/// That routing lives in `Hdf5FormatFactory`, three files from the source that
/// allows the split. This holds it in place, by reading the scan's file type off
/// the plan rather than by counting partitions: the bundled fixtures are under
/// [`MIN_SHARE_SIZE`], so neither reader shares them and a partition count would
/// say nothing about which source is underneath.
#[tokio::test]
async fn each_reader_serves_its_own_source() {
    for (backend, file_type) in [
        (Backend::NetcdfC, "file_type=netcdf"),
        (Backend::Rust, "file_type=hdf5"),
    ] {
        let ctx = splitting_session(4);
        register(&ctx, "t", backend, &netcdf_file(GRIDDED_FILE)).await;

        let plan = ctx
            .sql("SELECT analysed_sst FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();

        assert!(
            rendered.contains(file_type),
            "{backend:?} should scan through {file_type}:
{rendered}"
        );
    }
}

/// A small scan runs on every partition, and returns the same rows it returns
/// in a single-partition session.
///
/// File size decides nothing here any more. It used to: a file under the split
/// minimum was left on one partition, because a share of it opened the file and
/// built its chunk list before reading a byte, and on a small file that setup
/// cost more than the parallelism returned.
///
/// The scan is planned morsel-driven now — one standing entry per partition and
/// one queue behind them — so a partition that finds the queue empty simply
/// finishes. There is nothing to decline, and no size at which declining helps.
///
/// The row check is what matters and it is unchanged: whatever the partitions
/// divide between them, the answer must equal a single partition's.
#[tokio::test]
async fn a_small_scan_runs_on_every_partition_and_returns_the_same_rows() {
    const QUERY: &str = "SELECT count(*), min(analysed_sst), max(analysed_sst) FROM t";
    const PARTITIONS: usize = 4;

    let whole = session();
    register(&whole, "t", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let asked = splitting_session(PARTITIONS);
    register(&asked, "t", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let plan = asked
        .sql("SELECT analysed_sst FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(
        scan_partitions(&plan),
        PARTITIONS,
        "the scan is planned on every partition:
{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );

    let asked_summary = format!("{:?}", collect(&asked, QUERY).await.columns());
    let whole_summary = format!("{:?}", collect(&whole, QUERY).await.columns());
    assert_eq!(
        asked_summary, whole_summary,
        "{PARTITIONS} partitions over one queue read the file exactly once"
    );
}

/// How many columns [`write_large_netcdf`] writes, and how many rows in each.
const LARGE_COLUMNS: usize = 20;
const LARGE_ROWS: usize = 100_000;

/// Write a netCDF-4 file larger than [`MIN_SHARE_SIZE`].
///
/// A netCDF-4 file is an HDF5 file, so the Rust reader here reads it as one.
/// Every bundled fixture is under the minimum, so a test that needs a real
/// split has to make its own file.
///
/// It is written **wide** rather than long, and that is not a free choice.
/// `oxcdf` cannot read this writer's output once a single variable grows past
/// roughly 200k values: it fails with "chunk at […] was neither cached nor
/// fetched" while netcdf-c reads the same bytes. The limit follows one
/// variable's chunk count, not the file's size, so 20 columns of 100k values
/// clears 8 MB three times over and still reads back.
///
/// The caller holds the [`TempDir`](tempfile::TempDir) for as long as the table
/// is registered.
fn write_large_netcdf() -> (tempfile::TempDir, PathBuf) {
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
    use beacon_arrow_netcdf::writer::ArrowRecordBatchWriter;

    let schema = Arc::new(Schema::new(
        (0..LARGE_COLUMNS)
            .map(|column| Field::new(format!("V{column}"), DataType::Float64, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = (0..LARGE_COLUMNS)
        .map(|column| {
            Arc::new(Float64Array::from_iter_values(
                (0..LARGE_ROWS).map(|row| (row + column) as f64 * 0.25),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("a batch");

    let dir = tempfile::tempdir().expect("a temp directory");
    let path = dir.path().join("large.nc");
    let mut writer =
        ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema).expect("a netCDF writer");
    writer.write_record_batch(batch).expect("write the batch");
    writer.finish().expect("finish the file");

    let size = std::fs::metadata(&path).expect("the written file").len();
    assert!(
        size > MIN_SHARE_SIZE,
        "the generated file must clear the share minimum, got {size} bytes"
    );

    (dir, path)
}

/// A file over the split minimum scans in several partitions, and returns the
/// same rows it returns in one.
///
/// This is the only HDF5 test that reaches the split the way a query does: a
/// real file over the real minimum, planned and executed through SQL.
///
/// The partition count is the point of the feature. The row check is the guard
/// on it: `count(*)` catches a share that overlapped another or a gap between
/// two, and `min`/`max` catch a share that read the wrong region. None of those
/// raise an error on their own.
#[tokio::test]
async fn a_large_file_splits_and_returns_the_same_rows() {
    const QUERY: &str = r#"SELECT count(*), count("V0"), min("V0"), max("V0") FROM t"#;

    let (_dir, file) = write_large_netcdf();

    let whole = session();
    register(&whole, "t", Backend::Rust, &file).await;

    let split = splitting_session(4);
    register(&split, "t", Backend::Rust, &file).await;

    let plan = split
        .sql(r#"SELECT "V0" FROM t"#)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(
        scan_partitions(&plan),
        4,
        "a file over the minimum should scan in 4 partitions:\n{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );

    let split_summary = format!("{:?}", collect(&split, QUERY).await.columns());
    let whole_summary = format!("{:?}", collect(&whole, QUERY).await.columns());
    assert_eq!(split_summary, whole_summary);
}

/// A ragged file, where the row count comes from the instance/observation
/// layout rather than from a grid.
#[tokio::test]
async fn both_backends_return_the_same_rows_for_a_ragged_file() {
    let ctx = session();
    register(&ctx, "netcdf_c", Backend::NetcdfC, &netcdf_file(WOD_FILE)).await;
    register(&ctx, "rust", Backend::Rust, &netcdf_file(WOD_FILE)).await;

    let rust = collect(&ctx, "SELECT * FROM rust").await;
    assert!(rust.num_rows() > 0, "the scan must return rows");
    assert_eq!(rust, collect(&ctx, "SELECT * FROM netcdf_c").await);
}

/// A pushed-down predicate must not change the answer either.
#[tokio::test]
async fn both_backends_agree_under_a_predicate() {
    let ctx = session();
    register(
        &ctx,
        "netcdf_c",
        Backend::NetcdfC,
        &netcdf_file(GRIDDED_FILE),
    )
    .await;
    register(&ctx, "rust", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let sql = "SELECT lat, lon, analysed_sst FROM {table} WHERE lat > 45 AND lon < 30";
    let rust = collect(&ctx, &sql.replace("{table}", "rust")).await;
    assert!(rust.num_rows() > 0, "the predicate must match rows");
    assert_eq!(
        rust,
        collect(&ctx, &sql.replace("{table}", "netcdf_c")).await
    );
}

// ─── What only the Rust reader can do ───────────────────────────────────────

/// A plain HDF5 file whose datasets live two group levels deep. netcdf-c
/// reports only the root group, so this is the Rust reader's alone.
#[tokio::test]
async fn a_nested_group_reads_end_to_end() {
    let ctx = session();
    register(&ctx, "nested", Backend::Rust, &hdf5_file(NESTED_FILE)).await;

    let batch = collect(
        &ctx,
        r#"SELECT station_id, "observations/temperature", "observations/qc/flag"
           FROM nested ORDER BY station_id, "observations/temperature""#,
    )
    .await;
    assert_eq!(batch.num_rows(), 12, "3 stations x 4 samples");
    assert_eq!(batch.num_columns(), 3);
}

/// A compound dataset becomes one column per member. netcdf-c reports neither
/// the dataset nor an error for it.
#[tokio::test]
async fn a_compound_dataset_reads_end_to_end() {
    let ctx = session();
    register(&ctx, "compound", Backend::Rust, &hdf5_file(COMPOUND_FILE)).await;

    let batch = collect(
        &ctx,
        r#"SELECT "measurements/station", "measurements/label", "measurements/temp"
           FROM compound ORDER BY "measurements/station""#,
    )
    .await;
    assert_eq!(batch.num_rows(), 4);

    let stations = arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(batch.column(0));
    assert_eq!(stations.values(), &[1, 2, 3, 4]);

    let labels = arrow::array::as_string_array(batch.column(1));
    assert_eq!(labels.value(0), "alpha");
    assert_eq!(labels.value(3), "delta");
}

/// The same file read straight out of an in-memory object store: no path, no
/// local file, only byte ranges. This is the mechanism an s3, gs or az object
/// reads through, and it is the one netcdf-c does not have.
#[tokio::test]
async fn an_object_reads_with_no_local_copy() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("bucket/nested-groups.h5");
    let bytes = std::fs::read(hdf5_file(NESTED_FILE)).unwrap();
    store.put(&path, bytes.into()).await.unwrap();

    let dataset = beacon_arrow_hdf5::reader::open_dataset(store, path)
        .await
        .expect("an in-memory object opens with no local file");
    assert!(dataset.get_array("observations/temperature").is_some());
    assert!(dataset.get_array("observations/qc/flag").is_some());
}

// ─── Concurrency ────────────────────────────────────────────────────────────

/// Many scans of one file, at the same time, all give the right answer.
///
/// netcdf-c serialises these on a process-global mutex; the Rust reader holds
/// no lock, so they overlap. The assertion here is correctness, not timing —
/// see `concurrent_scan_cost` for the measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_scans_of_one_file_all_agree() {
    const SCANS: usize = 8;

    let ctx = Arc::new(session());
    register(&ctx, "rust", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let expected = collect(&ctx, "SELECT lat, lon FROM rust").await;

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..SCANS {
        let ctx = ctx.clone();
        set.spawn(async move { collect(&ctx, "SELECT lat, lon FROM rust").await });
    }
    while let Some(batch) = set.join_next().await {
        assert_eq!(batch.unwrap(), expected);
    }
}

/// What two concurrent scans of one file actually cost, on each backend.
///
/// Run it with:
///
/// ```text
/// cargo test --release -p beacon-arrow-hdf5 --test backend_parity \
///     concurrent_scan_cost -- --ignored --nocapture
/// ```
///
/// Ignored because it is a measurement, not an assertion: wall-clock ratios
/// depend on the machine and on what else it is doing, so asserting on them
/// makes a flaky test. It prints the serial and the concurrent time for both
/// backends, and the speed-up each one gets from running the scans together.
/// netcdf-c cannot overlap them — every call queues on one process-global
/// mutex — so its speed-up stays near 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement, not an assertion"]
async fn concurrent_scan_cost() {
    use std::time::Instant;

    const SCANS: usize = 8;
    const QUERY: &str = "SELECT analysed_sst FROM t";

    for backend in [Backend::NetcdfC, Backend::Rust] {
        // A fresh session for each backend, so no reader cache carries over.
        let ctx = Arc::new(session());
        register(&ctx, "t", backend, &netcdf_file(GRIDDED_FILE)).await;

        // Warm the page cache and the reader cache, so this measures the read
        // rather than the first open.
        let rows = collect(&ctx, QUERY).await.num_rows();

        let start = Instant::now();
        for _ in 0..SCANS {
            collect(&ctx, QUERY).await;
        }
        let serial = start.elapsed().as_secs_f64();

        let start = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..SCANS {
            let ctx = ctx.clone();
            set.spawn(async move { collect(&ctx, QUERY).await });
        }
        while set.join_next().await.is_some() {}
        let concurrent = start.elapsed().as_secs_f64();

        println!(
            "\n{backend:?}  ({SCANS} scans of {GRIDDED_FILE}, {rows} rows each)\n  \
             serial     : {:.0} ms total ({:.0} ms/scan)\n  \
             concurrent : {:.0} ms total ({:.0} ms/scan)\n  \
             speed-up   : {:.2}x",
            serial * 1e3,
            serial * 1e3 / SCANS as f64,
            concurrent * 1e3,
            concurrent * 1e3 / SCANS as f64,
            serial / concurrent,
        );
    }
}

// ─── Statistics ─────────────────────────────────────────────────────────────

/// A bare local store, plus the metadata of one bundled file inside it.
fn local_object(path: &std::path::Path) -> (Arc<dyn ObjectStore>, ObjectMeta) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let location = object_store::path::Path::from_absolute_path(path)
        .unwrap_or_else(|e| panic!("{} is not an absolute object path: {e}", path.display()));
    let metadata = std::fs::metadata(path).expect("the bundled test file exists");
    let object = ObjectMeta {
        location,
        last_modified: metadata.modified().map(Into::into).unwrap_or_default(),
        size: metadata.len(),
        e_tag: None,
        version: None,
    };
    (store, object)
}

/// The format a table would get for `path` on `backend`, ready to infer.
fn format_on(
    ctx: &SessionContext,
    backend: Backend,
    path: &std::path::Path,
) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
    let listing = Arc::new(ListingFactory::dynamic());
    let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
    factory(Backend::NetcdfC)
        .create_with_native_root(&ctx.state(), &backend.options(), &url, &listing)
        .unwrap_or_else(|e| panic!("build the {backend:?} format: {e}"))
}

/// The analysis form of a format: the one that measures files.
///
/// A format built any other way reports unknown statistics, so that a query
/// never pays to compute them. Only the file analyzer asks for this one.
fn analysis_format_on(
    ctx: &SessionContext,
    backend: Backend,
    path: &std::path::Path,
) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
    let listing = Arc::new(ListingFactory::dynamic());
    let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
    factory(Backend::NetcdfC)
        .create_for_analysis(&ctx.state(), &backend.options(), &url, &listing)
        .unwrap_or_else(|e| panic!("build the {backend:?} analysis format: {e}"))
}

/// How many columns came back with a real minimum.
fn columns_with_a_range(statistics: &datafusion::common::Statistics) -> usize {
    statistics
        .column_statistics
        .iter()
        .filter(|c| c.min_value.get_value().is_some())
        .count()
}

/// Statistics are a capability of the reader, not of the format — the same rule
/// netCDF follows. Every netcdf-c call serialises on a process-global mutex and
/// the read is synchronous, so computing statistics under it is serial and parks
/// a tokio worker. The Rust reader has neither problem.
#[tokio::test]
async fn statistics_come_from_the_rust_reader_only() {
    let ctx = session();
    let state = ctx.state();
    let path = netcdf_file(WOD_FILE);
    let (store, object) = local_object(&path);

    let rust = analysis_format_on(&ctx, Backend::Rust, &path);
    let schema = rust
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .expect("the Rust reader infers a schema");
    let with_rust_reader = rust
        .infer_stats(&state, &store, schema.clone(), &object)
        .await
        .expect("statistics are never an error");
    assert!(
        columns_with_a_range(&with_rust_reader) > 0,
        "the Rust reader must produce real ranges for the coordinate variables"
    );

    // netcdf-c reports unknown rather than erroring, so a deployment on it keeps
    // working and simply prunes nothing.
    let netcdf_c = analysis_format_on(&ctx, Backend::NetcdfC, &path);
    let without_rust_reader = netcdf_c
        .infer_stats(&state, &store, schema.clone(), &object)
        .await
        .expect("netcdf-c reports unknown, it does not fail");
    assert_eq!(
        columns_with_a_range(&without_rust_reader),
        0,
        "netcdf-c must report unknown rather than compute"
    );
    assert_eq!(
        without_rust_reader.column_statistics.len(),
        schema.fields().len(),
        "unknown statistics still cover every column, as DataFusion requires"
    );
}

/// A plain HDF5 file gets ranges too, including a dataset inside a group.
#[tokio::test]
async fn statistics_cover_a_plain_hdf5_file() {
    let ctx = session();
    let state = ctx.state();
    let path = hdf5_file(NESTED_FILE);
    let (store, object) = local_object(&path);

    let format = analysis_format_on(&ctx, Backend::Rust, &path);
    let schema = format
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .unwrap();
    let statistics = format
        .infer_stats(&state, &store, schema.clone(), &object)
        .await
        .unwrap();

    let range = |column: &str| {
        let index = schema
            .index_of(column)
            .unwrap_or_else(|e| panic!("{column}: {e}"));
        let stats = &statistics.column_statistics[index];
        (stats.min_value.clone(), stats.max_value.clone())
    };

    // A 1-d dataset in the root group, and one two groups down. Both are cheap
    // to scan, so both carry an exact range.
    assert_eq!(
        range("station_id"),
        (
            Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(11))),
            Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(33)))
        )
    );
    // A 2-d dataset is not scanned eagerly, so it stays unknown — the same rule
    // the netCDF statistics apply to a gridded variable.
    assert_eq!(
        range("observations/qc/flag"),
        (Precision::Absent, Precision::Absent)
    );
}

/// The switch is still honoured on top of the reader gate.
#[tokio::test]
async fn disabling_statistics_wins_over_the_reader() {
    let ctx = session();
    let state = ctx.state();
    let path = netcdf_file(WOD_FILE);
    let (store, object) = local_object(&path);

    let on = format_on(&ctx, Backend::Rust, &path);
    let schema = on
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .unwrap();

    let listing = Arc::new(ListingFactory::dynamic());
    let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
    let off = factory(Backend::NetcdfC)
        .create_with_native_root(
            &ctx.state(),
            &HashMap::from([
                ("backend".to_string(), "rust".to_string()),
                ("enable_statistics".to_string(), "false".to_string()),
            ]),
            &url,
            &listing,
        )
        .unwrap();

    let statistics = off
        .infer_stats(&state, &store, schema, &object)
        .await
        .unwrap();
    assert_eq!(
        columns_with_a_range(&statistics),
        0,
        "enable_statistics=false must still mean no statistics"
    );
}

/// The predicate reaches the HDF5 scan through the nd spine, and skips chunks.
///
/// The scan sits under an `NdSourceExec` and an `NdBroadcastExec`. A node that
/// does not forward filters leaves the source with nothing to prune on, and that
/// failure is invisible in a result — every row still comes back, the scan just
/// reads the whole file. So it is asserted on the scan's own output.
///
/// One encoded batch carries one chunk, so the scan's output rows *are* its
/// chunk count. The fixture's `lat` runs from about 38.8 to 48.8.
#[tokio::test]
async fn a_predicate_reaches_the_scan_and_skips_its_chunks() {
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

    let ctx = session();
    register(&ctx, "t", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    struct ScanRows(Option<usize>);
    impl ExecutionPlanVisitor for ScanRows {
        type Error = std::convert::Infallible;
        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.name().contains("DataSourceExec") {
                self.0 = plan.metrics().and_then(|metrics| metrics.output_rows());
                return Ok(false);
            }
            Ok(true)
        }
    }

    let chunks_read = async |sql: &str| {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();
        let mut visitor = ScanRows(None);
        datafusion::physical_plan::accept(plan.as_ref(), &mut visitor).unwrap();
        visitor.0.expect("the scan reports its output rows")
    };

    let whole = chunks_read("SELECT lat FROM t").await;
    assert!(whole > 1, "the fixture must hold several chunks");

    assert_eq!(
        chunks_read("SELECT lat FROM t WHERE lat > 1000").await,
        0,
        "a predicate no row can meet must leave the scan nothing to read"
    );
    assert_eq!(
        chunks_read("SELECT lat FROM t WHERE lat > 0").await,
        whole,
        "a predicate every row meets must not skip a chunk"
    );

    let partial = chunks_read("SELECT lat FROM t WHERE lat > 44").await;
    assert!(
        partial > 0 && partial < whole,
        "lat > 44 should read some of the {whole} chunks, it read {partial}"
    );
}

/// The count under a pruned scan matches the coordinate itself.
///
/// A bound that is too tight loses rows and reports a smaller count, and nothing
/// about that is an error. The expected answer comes from the coordinate column,
/// read with no predicate in the plan and therefore with no pruning.
#[tokio::test]
async fn a_pruned_scan_counts_what_the_coordinate_says_it_should() {
    use arrow::array::{Float32Array, Int64Array};

    let ctx = session();
    register(&ctx, "t", Backend::Rust, &netcdf_file(GRIDDED_FILE)).await;

    let lats: Vec<f32> = {
        let batch = collect(&ctx, "SELECT lat FROM t").await;
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("lat is f32")
            .iter()
            .flatten()
            .collect()
    };
    assert!(!lats.is_empty(), "the fixture must hold rows");

    let count = async |sql: &str| {
        collect(&ctx, sql)
            .await
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count")
            .value(0) as usize
    };

    for threshold in [0.0_f32, 44.0, 60.0] {
        let expected = lats.iter().filter(|lat| **lat > threshold).count();
        assert_eq!(
            count(&format!("SELECT count(*) FROM t WHERE lat > {threshold}")).await,
            expected,
            "lat > {threshold}: the pruned scan does not match the coordinate"
        );
    }

    // A disjunction implies no bound, so it must prune nothing and still answer.
    assert_eq!(
        count("SELECT count(*) FROM t WHERE lat > 0 OR lat > 60").await,
        lats.iter().filter(|lat| **lat > 0.0).count(),
        "a disjunction must not prune on one of its branches"
    );
}
