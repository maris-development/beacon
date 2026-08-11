//! Scale check for reading many netCDF files through the pure-Rust reader.
//!
//! `fast_object_scale` in beacon-datafusion-ext measures Parquet, which holds
//! descriptors sparingly and reads a footer to answer most questions. netCDF is
//! the format that made [issue #361][] a crash rather than a slowdown: the
//! default reader is netcdf-c, which opens a path itself, and a plan over sixty
//! thousand files was observed holding 57 367 descriptors.
//!
//! This runs the same shape over netCDF with `use_rust_reader` on, so `oxcdf`
//! reads byte ranges through the object store instead. Everything goes through a
//! real `RuntimeBuilder`, and the statistics store is filled by the real
//! collector rather than by hand.
//!
//! Ignored by default. Writing netCDF is not cheap and the files are not small,
//! so run it deliberately and in release:
//!
//! ```text
//! cargo test -p beacon-core --test netcdf_scale --release -- --ignored --nocapture
//! ```
//!
//! `BEACON_NETCDF_SCALE_FILES` sets the file count. `BEACON_NETCDF_SCALE_BACKEND`
//! takes `rust` (the default) or `netcdf-c`, so the two readers can be compared
//! at the same size.
//!
//! What one run reported, on a 12 core M-series Mac in release:
//!
//! ```text
//! write           26.60s  1767 MiB on disk (18.1 KiB/file)
//! analyze         12.76s  100000 files, peak 23 open fds
//! plan (all)       8.98s  peak 37 open fds
//! plan (prune)     9.10s  100000 considered, 99990 pruned
//! read a 1/4      10.37s  50000 rows from 25000 files, peak 38 open fds
//! re-plan #1       9.03s  same query, same files
//! re-plan #2       9.07s  same query, same files
//! rss              666.1 MiB
//! ```
//!
//! Read those by subtraction, because almost all of each line is the same fixed
//! cost. Pruning a hundred thousand files costs 0.12s, about 1.2 microseconds
//! each. Reading twenty five thousand of them costs 1.39s, about 56 microseconds
//! each. Everything else — nine seconds, 87% of the query that reads a quarter
//! of the store — is inferring the schema, at roughly 90 microseconds per file.
//!
//! And it is paid again on every query. `read_netcdf` builds its table per
//! query, building it calls `infer_schema` over the whole listing, and nothing
//! caches the result: the two re-plans cost what the first plan cost. Interning
//! schemas would take this query from 10.4s to under 1.5s. Nothing else here is
//! worth optimising first.
//!
//! The same corpus under `BEACON_SCALE_FORMAT=hdf5`, which writes the identical
//! bytes with an `.h5` extension so the HDF5 format and its Rust reader claim
//! them:
//!
//! ```text
//! analyze         10.77s  100000 files, peak 21 open fds
//! plan (all)       9.77s  peak 43 open fds
//! plan (prune)     9.74s  100000 considered, 99990 pruned
//! read a 1/4      11.78s  scan node opening=3.87s scanning=0.42s (155 µs/file)
//! re-plan #1       9.61s
//! rss              909.0 MiB
//! ```
//!
//! The same shape, a little more expensive: 155 microseconds to open a file
//! against netCDF's 131, and 909 MiB resident against 657. Inference still
//! dominates, pruning is still free, and opening still dwarfs reading. Neither
//! format's cost lives anywhere the other's does not.
//!
//! Before `infer_schema` was bounded this did not finish: descriptors tracked
//! the file count one for one (19 721 for 20 000 files), and 100 000 files died
//! with `Too many open files`. macOS caps a process at `kern.maxfilesperproc`,
//! 61 440 here, whatever `ulimit -n` reports — which is why the issue saw 57 367
//! during a 60 000 file plan. Bounding it also made planning and reading faster
//! and cut resident memory sevenfold, so the width was never buying throughput.
//!
//! The other half is that `use_rust_reader` is not an optimisation here but a
//! precondition: netcdf-c reports `Statistics::new_unknown`, so the collector
//! records every file with zero columns and a predicate prunes nothing.
//!
//! [issue #361]: https://github.com/maris-development/beacon/issues/361

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_arrow_hdf5::Hdf5Config;
use beacon_arrow_netcdf::datafusion::NetcdfConfig;
use beacon_common::FileStatsConfig;
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use beacon_datafusion_ext::listing_factory::RootStore;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::TryStreamExt;

/// Files the check runs over, unless `BEACON_NETCDF_SCALE_FILES` says otherwise.
const DEFAULT_FILES: usize = 100_000;

/// Files per directory, so the store is a tree rather than one flat directory.
const BUCKET_FILES: usize = 1_000;

/// Files the selective predicate is meant to leave standing.
const SURVIVORS: usize = 10;

fn file_count() -> usize {
    std::env::var("BEACON_NETCDF_SCALE_FILES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FILES)
}

/// Whether to read through the pure-Rust reader. `netcdf-c` selects the other.
fn use_rust_reader() -> bool {
    !matches!(
        std::env::var("BEACON_NETCDF_SCALE_BACKEND").as_deref(),
        Ok("netcdf-c")
    )
}

/// Which format reads the corpus.
///
/// A netCDF-4 file *is* an HDF5 file, so both run over bytes written the same
/// way. The extension decides which Beacon format claims them, and each format
/// brings its own reader, config and `read_*` function.
#[derive(Clone, Copy, PartialEq)]
enum Format {
    Netcdf,
    Hdf5,
}

impl Format {
    fn selected() -> Self {
        match std::env::var("BEACON_SCALE_FORMAT").as_deref() {
            Ok("hdf5") => Format::Hdf5,
            _ => Format::Netcdf,
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Format::Netcdf => "nc",
            Format::Hdf5 => "h5",
        }
    }

    fn read_fn(self) -> &'static str {
        match self {
            Format::Netcdf => "read_netcdf",
            Format::Hdf5 => "read_hdf5",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Format::Netcdf => "netCDF",
            Format::Hdf5 => "HDF5",
        }
    }
}

/// The glob every query in this test scans.
fn glob() -> String {
    format!("obs/*/*.{}", Format::selected().extension())
}

fn path_for(index: usize) -> String {
    format!(
        "obs/b{:04}/f{:07}.{}",
        index / BUCKET_FILES,
        index,
        Format::selected().extension()
    )
}

/// The two values file `index` holds: `index` and `index + 0.5`.
fn values_for(index: usize) -> [f64; 2] {
    [index as f64, index as f64 + 0.5]
}

/// A netCDF-4 file whose TEMP column spans `values_for(index)`.
///
/// Writes always go through netcdf-c: `oxcdf` reads and does not write.
fn write_netcdf(path: &Path, index: usize) {
    use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
    use beacon_arrow_netcdf::writer::ArrowRecordBatchWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, false),
        Field::new("DEPTH", DataType::Int64, false),
    ]));
    let [min, max] = values_for(index);
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

/// Write `count` netCDF files under `datasets`, and report the bytes on disk.
fn write_files(datasets: &Path, count: usize) -> u64 {
    for bucket in 0..count.div_ceil(BUCKET_FILES) {
        std::fs::create_dir_all(datasets.join(format!("obs/b{bucket:04}"))).unwrap();
    }
    // Serial on purpose. netcdf-c serializes on a global lock, so threads here
    // buy contention rather than throughput.
    let mut bytes = 0;
    for index in 0..count {
        let path = datasets.join(path_for(index));
        write_netcdf(&path, index);
        bytes += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    }
    bytes
}

fn builder(root: &Path) -> RuntimeBuilder {
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
        .with_netcdf_config(NetcdfConfig {
            use_rust_reader: use_rust_reader(),
            ..Default::default()
        })
        .with_hdf5_config(Hdf5Config {
            use_rust_reader: use_rust_reader(),
            ..Default::default()
        })
        .with_file_stats(FileStatsConfig {
            enable: true,
            // Far enough out that every pass is one this test asked for.
            interval_secs: 3_600,
            concurrency: std::thread::available_parallelism().map_or(4, |n| n.get()),
            batch_files: 10_000,
            target_group_files: 10_000,
            min_group_files: 500,
            prefix_depth: None,
            scan_prefix: String::new(),
            discovery_chunk: 10_000,
            // A pass has to reach every file for the store to be full.
            ..Default::default()
        })
        .with_vm_memory_limit(8 * 1024 * 1024 * 1024)
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

/// Rows a query produces, by draining its stream rather than formatting it.
async fn row_count(runtime: &Runtime, sql: &str) -> usize {
    let batches = runtime
        .run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_record_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
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
fn counter(analyzed: &str, name: &str) -> usize {
    let marker = format!("{name}=");
    analyzed
        .split(&marker)
        .skip(1)
        .map(parse_metric_value)
        .sum()
}

/// Assert a counter matches, allowing for the rounding the abbreviation costs.
///
/// `EXPLAIN ANALYZE` keeps four significant digits, so a count near a hundred
/// thousand is exact to within about five.
fn assert_close(actual: usize, expected: usize, what: &str, plan: &str) {
    let slack = (expected as f64 * 0.001).max(1.0);
    let off = (actual as f64) - (expected as f64);
    assert!(
        off.abs() <= slack,
        "{what}: expected about {expected}, got {actual}\n{plan}"
    );
}

/// A duration metric from `EXPLAIN ANALYZE`, in seconds.
///
/// Values arrive as `1.06ms`, `827.12µs` or `1.39s`, so the unit has to be read
/// along with the number.
fn metric_seconds(analyzed: &str, name: &str) -> f64 {
    let marker = format!("{name}=");
    analyzed
        .split(&marker)
        .skip(1)
        .map(|rest| {
            let digits: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            let Ok(value) = digits.parse::<f64>() else {
                return 0.0;
            };
            let unit = &rest[digits.len()..];
            if unit.starts_with("ns") {
                value / 1e9
            } else if unit.starts_with("µs") || unit.starts_with("us") {
                value / 1e6
            } else if unit.starts_with("ms") {
                value / 1e3
            } else {
                value
            }
        })
        .sum()
}

fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd")
        .map(|dir| dir.count())
        .unwrap_or(0)
}

fn fd_limit() -> String {
    std::process::Command::new("sh")
        .args(["-c", "ulimit -n"])
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn rss_bytes() -> Option<usize> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<usize>()
        .ok()
        .map(|kilobytes| kilobytes * 1024)
}

/// Samples open descriptors on its own thread, and remembers the highest.
struct FdWatch {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicUsize>,
    handle: std::thread::JoinHandle<()>,
}

impl FdWatch {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicUsize::new(open_fds()));
        let handle = {
            let stop = Arc::clone(&stop);
            let peak = Arc::clone(&peak);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(open_fds(), Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(2));
                }
                peak.fetch_max(open_fds(), Ordering::Relaxed);
            })
        };
        Self { stop, peak, handle }
    }

    fn finish(self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap();
        self.peak.load(Ordering::Relaxed)
    }
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// A hundred thousand analyzed files, planned and read.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "writes a large netCDF corpus; run explicitly with --ignored --nocapture"]
async fn a_hundred_thousand_files_plan_and_read() {
    let count = file_count();
    let format = Format::selected();
    let reader = if use_rust_reader() {
        "rust"
    } else {
        "netcdf-c"
    };
    let read_fn = format.read_fn();
    let glob = glob();
    let root = tempfile::tempdir().unwrap();
    println!(
        "\n=== {count} {} files, {reader} reader ===",
        format.label()
    );

    // --- The corpus --------------------------------------------------------
    let started = Instant::now();
    let bytes = write_files(&root.path().join("datasets"), count);
    println!(
        "write        {:>9.2?}  {:.0} MiB on disk ({:.1} KiB/file)",
        started.elapsed(),
        mib(bytes as usize),
        bytes as f64 / count as f64 / 1024.0,
    );

    let runtime = builder(root.path()).build().await.unwrap();

    // --- The collector fills the statistics store --------------------------
    //
    // netCDF reports no statistics through netcdf-c, so this is also where
    // `use_rust_reader` earns its keep: without it every file analyzes to zero
    // columns and nothing is prunable.
    let watch = FdWatch::start();
    let started = Instant::now();
    let mut analyzed = 0;
    loop {
        let pass = runtime.file_stats().unwrap().run_once().await.unwrap();
        analyzed += pass.analyzed;
        // A pass ends idle when the queue is empty and nothing new landed.
        if pass.analyzed == 0 && pass.failed == 0 {
            break;
        }
    }
    let analyze = started.elapsed();
    let analyze_fds = watch.finish();
    assert_eq!(analyzed, count, "the collector must reach every file");
    println!(
        "analyze      {analyze:>9.2?}  {analyzed} files, peak {analyze_fds} open fds (soft limit {})",
        fd_limit()
    );
    // The collector bounds its own concurrency, so this stays flat however many
    // files there are. It is the one phase that already behaves, and the number
    // to defend.
    assert!(
        analyze_fds < 256,
        "the collector held {analyze_fds} descriptors open for {count} files"
    );

    // --- Planning ----------------------------------------------------------
    let watch = FdWatch::start();
    let started = Instant::now();
    let all = query(
        &runtime,
        &format!("EXPLAIN SELECT \"TEMP\" FROM {read_fn}('{glob}')"),
    )
    .await;
    let plan_all = started.elapsed();
    let plan_fds = watch.finish();
    println!("plan (all)   {plan_all:>9.2?}  peak {plan_fds} open fds");
    assert!(
        all.contains(format.extension()),
        "the plan should name the files it reads:\n{}",
        &all[..all.len().min(400)]
    );

    // --- A predicate that keeps ten files ----------------------------------
    let cutoff = count - SURVIVORS;
    let started = Instant::now();
    let pruned = query(
        &runtime,
        &format!(
            "EXPLAIN ANALYZE SELECT \"TEMP\" FROM {read_fn}('{glob}') \
             WHERE \"TEMP\" >= {cutoff}"
        ),
    )
    .await;
    let plan_pruned = started.elapsed();
    let considered = counter(&pruned, "file_stats_files_considered");
    let dropped = counter(&pruned, "file_stats_files_pruned");
    println!("plan (prune) {plan_pruned:>9.2?}  {considered} considered, {dropped} pruned");
    if use_rust_reader() {
        assert_close(considered, count, "every file reaches the scan", &pruned);
        assert_close(
            dropped,
            count - SURVIVORS,
            "only the top files can match",
            &pruned,
        );
    } else {
        // netcdf-c reports `Statistics::new_unknown`, so the collector records
        // every file as analyzed with zero columns and nothing is prunable. The
        // Rust reader is not a faster way to prune netCDF; it is the only way.
        assert_eq!(
            (considered, dropped),
            (0, 0),
            "netcdf-c yields no ranges, so there is nothing to prune on:\n{pruned}"
        );
    }

    // --- A query that reads a quarter of the store -------------------------
    let reads = count / 4;
    let cutoff = count - reads;
    let watch = FdWatch::start();
    let started = Instant::now();
    let rows = row_count(
        &runtime,
        &format!("SELECT \"TEMP\" FROM {read_fn}('{glob}') WHERE \"TEMP\" >= {cutoff}"),
    )
    .await;
    let read = started.elapsed();
    let read_fds = watch.finish();
    // The same either way: pruning drops files that hold no matching row, and
    // the filter above the scan drops the rest. Only the work differs.
    assert_eq!(
        rows,
        reads * 2,
        "two rows from each file at or above the cutoff"
    );
    println!("read a 1/4   {read:>9.2?}  {rows} rows from {reads} files, peak {read_fds} open fds");

    // --- The same read, as the scan itself times it ------------------------
    //
    // The wall clock above includes planning, which dominates it. These are the
    // scan node's own counters, so they are the read cost without the
    // subtraction.
    let analyzed_read = query(
        &runtime,
        &format!(
            "EXPLAIN ANALYZE SELECT \"TEMP\" FROM {read_fn}('{glob}') \
             WHERE \"TEMP\" >= {cutoff}"
        ),
    )
    .await;
    let opening = metric_seconds(&analyzed_read, "time_elapsed_opening");
    let scanning = metric_seconds(&analyzed_read, "time_elapsed_scanning_total");
    println!(
        "  scan node reports opening={opening:.2}s scanning={scanning:.2}s \
         for {reads} files ({:.0} µs/file opening)",
        opening * 1e6 / reads as f64,
    );

    // --- What a second identical query pays -------------------------------
    //
    // `read_netcdf` builds its table per query, and building it infers the
    // schema by opening every file in the listing. Nothing caches that between
    // queries, so this measures whether the fixed cost above is paid again.
    for round in 1..=2 {
        let started = Instant::now();
        let repeat = query(
            &runtime,
            &format!("EXPLAIN SELECT \"TEMP\" FROM {read_fn}('{glob}')"),
        )
        .await;
        assert!(repeat.contains(format.extension()));
        println!(
            "re-plan #{round}  {:>9.2?}  same query, same files",
            started.elapsed()
        );
    }

    if let Some(rss) = rss_bytes() {
        println!("rss          {:>9.1} MiB", mib(rss));
    }

    // The crash issue #361 reports. `NetcdfFormat::infer_schema` used to build one
    // `fetch_schema` future per object and await them all through `try_join_all`,
    // so a table over a hundred thousand objects opened a hundred thousand files
    // and died with `Too many open files`. It is bounded by `meta_fetch_concurrency`
    // now, so this must not grow with the file count.
    //
    // macOS caps a process at `kern.maxfilesperproc` (61 440 here) whatever
    // `ulimit -n` reports, which is why the issue saw 57 367 during a 60 000 file
    // plan rather than a limit anyone had configured.
    println!(
        "\nschema inference held {plan_fds} descriptors for {count} files \
         ({:.3}/file), soft limit {}",
        plan_fds as f64 / count as f64,
        fd_limit(),
    );
    assert!(
        plan_fds < 1_024,
        "schema inference held {plan_fds} descriptors for {count} files; \
         it is meant to be bounded by meta_fetch_concurrency, not by the listing"
    );
    assert!(
        read_fds < 1_024,
        "reading {reads} files held {read_fds} descriptors open"
    );
}
