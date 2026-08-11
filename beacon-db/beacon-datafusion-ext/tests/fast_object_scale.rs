//! Scale check for [`FastObjectTable`]: 100 000 real files, every one analyzed.
//!
//! The unit tests prove the table is correct at two hundred files. This proves
//! what happens at the size [issue #361][] is about: how long a plan takes, how
//! many file descriptors it opens on the way, and how much the file list it
//! carries costs.
//!
//! Real Parquet objects on a real filesystem, because the failure the issue
//! reports is `Too many open files`, and an in-memory store cannot reproduce
//! it. The statistics store holds a recorded range for every one of the files,
//! laid out the way a collector pass would leave it: contiguous file-id ranges,
//! ten thousand files to a segment.
//!
//! Ignored by default. It writes ~100 MB and takes minutes in a debug build,
//! so run it in release:
//!
//! ```text
//! cargo test -p beacon-datafusion-ext --test fast_object_scale --release -- --ignored --nocapture
//! ```
//!
//! `BEACON_SCALE_FILES` sets the file count. Above 131 072 the prune splits
//! into parallel tasks, which 100 000 does not reach.
//!
//! What one run reported, on a 12 core M-series Mac in release, for scale
//! rather than as a threshold to defend:
//!
//! ```text
//! infer schema     3.46s  peak 43 open fds
//! plan (all)     499.04ms  100000 files in 12 groups, list 31.6 MiB
//! plan (prune)   558.07ms  10 of 100000 files survive
//! query plan     543.14ms  25000 files (25011 scan entries) in 12 groups
//! query read     823.34ms  50000 rows from 25000 files, peak 23 open fds
//! re-plan #1     555.25ms  same table, same predicate
//! re-plan #2     530.57ms
//! re-plan #3     538.81ms
//! rss             58.8 MiB -> 867.0 MiB
//! ```
//!
//! Five things to read from that. Descriptors stay flat through every phase,
//! including the one that reads twenty five thousand files, so the reader opens
//! them a few at a time rather than all at once. Planning costs the same
//! whether the predicate keeps ten files or all of them, because the listing is
//! materialized before pruning sees it. The file list is 331 B per file, which
//! is 0.92 GiB at three million, close to the 1.1 GB the issue reports. A plan
//! holds more scan entries than files, because DataFusion splits a few of them
//! into byte ranges to balance its partitions. And holding the table across
//! queries saves the inference, not the planning: the re-plans cost what the
//! first plan cost, because `scan` lists the store every time it is called.
//!
//! [issue #361]: https://github.com/maris-development/beacon/issues/361

use std::path::Path as FsPath;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::stats_cache::BeaconFileStatisticsCache;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_file_stats::registry::AnalyzedFile;
use beacon_file_stats::segment::{ColumnStat, SegmentBuilder};
use beacon_file_stats::{FileStatsStore, ObservedFile, Registry, StatScalar};
use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use object_store::local::LocalFileSystem;

/// Files the check runs over, unless `BEACON_SCALE_FILES` says otherwise.
const DEFAULT_FILES: usize = 100_000;

/// Files per directory, so the tree has the shape a real store has rather than
/// one directory holding every object.
const BUCKET_FILES: usize = 1_000;

/// Files per segment. This is `CollectorConfig::default().target_group_files`,
/// so the segment layout matches what a real collector pass leaves behind.
const SEGMENT_FILES: usize = 10_000;

/// Files the selective predicate is meant to leave standing.
const SURVIVORS: usize = 10;

const STORE_URL: &str = "scale://files/";

fn file_count() -> usize {
    std::env::var("BEACON_SCALE_FILES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FILES)
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
}

/// Where file `index` lives, relative to the store root.
///
/// Zero padded, so path order and index order are the same. That keeps the ids
/// the registry hands out ascending, which is what a segment needs.
fn path_for(index: usize) -> String {
    format!("obs/b{:04}/f{:07}.parquet", index / BUCKET_FILES, index)
}

/// The two values file `index` holds: `index` and `index + 0.5`.
///
/// One file per whole number means a predicate can name exactly the files it
/// should keep, and the test can check that it kept those.
fn values_for(index: usize) -> [f64; 2] {
    [index as f64, index as f64 + 0.5]
}

fn parquet_bytes(index: usize) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        value_schema(),
        vec![Arc::new(Float64Array::from(values_for(index).to_vec()))],
    )
    .unwrap();
    let mut bytes = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut bytes, value_schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

/// Write `count` Parquet files under `root`, and report what a listing would
/// have observed about each.
///
/// Split across threads: a hundred thousand encodes is the slowest phase of
/// the setup, and it parallelizes perfectly.
fn write_files(root: &FsPath, count: usize) -> Vec<ObservedFile> {
    for bucket in 0..count.div_ceil(BUCKET_FILES) {
        std::fs::create_dir_all(root.join(format!("obs/b{bucket:04}"))).unwrap();
    }

    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let chunk = count.div_ceil(threads);
    let chunks: Vec<Vec<ObservedFile>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|worker| {
                let start = worker * chunk;
                let end = (start + chunk).min(count);
                scope.spawn(move || {
                    let mut observed = Vec::with_capacity(end.saturating_sub(start));
                    for index in start..end {
                        let relative = path_for(index);
                        let full = root.join(&relative);
                        let bytes = parquet_bytes(index);
                        let size = bytes.len() as u64;
                        std::fs::write(&full, bytes).unwrap();
                        let modified = std::fs::metadata(&full)
                            .unwrap()
                            .modified()
                            .unwrap()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;
                        observed.push(ObservedFile::new(relative, size, modified));
                    }
                    observed
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    chunks.into_iter().flatten().collect()
}

/// Record a range for every file, in segments of [`SEGMENT_FILES`].
///
/// This is the collector's output without the collector: intern a batch, mark
/// it analyzed, and commit one segment covering it. Batching keeps each redb
/// transaction bounded, and leaves segments holding contiguous id ranges.
async fn fill_statistics(stats: &FileStatsStore, observed: &[ObservedFile]) {
    let column = stats.registry().intern_columns(&["v"]).unwrap()[0];

    for (batch, files) in observed.chunks(SEGMENT_FILES).enumerate() {
        let offset = batch * SEGMENT_FILES;
        let ids = stats.registry().intern_files(files).unwrap();

        let analyzed: Vec<AnalyzedFile<'_>> = ids
            .iter()
            .zip(files)
            .map(|(id, file)| AnalyzedFile {
                id: *id,
                format: "parquet",
                num_rows: Some(2),
                total_byte_size: Some(file.size),
                column_count: 1,
            })
            .collect();
        stats.registry().mark_analyzed_batch(&analyzed).unwrap();

        // `push_file` wants ascending ids, and interning in path order gives
        // them in path order.
        let mut builder = SegmentBuilder::new();
        for (position, id) in ids.iter().enumerate() {
            let [min, max] = values_for(offset + position);
            builder.push_file(
                *id,
                [(
                    column,
                    ColumnStat {
                        min: StatScalar::F64(min),
                        max: StatScalar::F64(max),
                        null_count: Some(0),
                        row_count: Some(2),
                        data_type: DataType::Float64,
                    },
                )],
            );
        }
        stats.commit_segment(builder).await.unwrap();
    }
}

/// Open the statistics store for a run under `root`.
async fn open_stats(root: &FsPath) -> Arc<FileStatsStore> {
    let registry = Arc::new(Registry::open(root.join("registry.redb")).unwrap());
    let segments = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
    Arc::new(
        FileStatsStore::open(
            registry,
            segments,
            object_store::path::Path::from("segments"),
        )
        .await
        .unwrap(),
    )
}

/// A session over the files under `root`, configured the way the server is.
///
/// Two settings are copied from `RuntimeBuilder`, because both change what is
/// measured here.
///
/// `listing_table_ignore_subdirectory` DataFusion defaults to true, which drops
/// every object below the first level of the prefix, and a store of this size is
/// a tree rather than one flat directory.
///
/// The cache manager decides what a second query over the same table pays for.
/// Statistics are cached and the listing is not, which is what makes reusing a
/// table save less than it looks like it should.
fn session(root: &FsPath, stats: Arc<FileStatsStore>) -> SessionContext {
    let handle = beacon_file_stats::new_file_stats_handle();
    handle.set(stats).ok();
    let mut config = SessionConfig::new()
        .with_extension(handle)
        .with_extension(ArrowTypeWidening::default_extension());
    config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;

    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig {
            table_files_statistics_cache: Some(Arc::new(BeaconFileStatisticsCache::default())),
            list_files_cache_limit: 0,
            ..Default::default()
        })
        .build_arc()
        .unwrap();

    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_object_store(
        ObjectStoreUrl::parse(STORE_URL).unwrap().as_ref(),
        Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
    );
    ctx
}

/// A table over every file written under `obs/`.
async fn table_over_obs(ctx: &SessionContext) -> Arc<FastObjectTable> {
    Arc::new(
        FastObjectTable::try_new(
            &ctx.state(),
            Arc::new(ParquetFormat::default()),
            vec![ListingTableUrl::parse(format!("{STORE_URL}obs/")).unwrap()],
        )
        .await
        .unwrap(),
    )
}

/// Distinct files the plan will read, and the scan entries covering them.
///
/// The two differ. DataFusion balances partitions by splitting a file into byte
/// ranges, which puts one entry per range under the same path, so counting
/// entries counts a split file twice.
fn planned_files_and_entries(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let paths = planned_files(plan);
    let entries = paths.len();
    let distinct: std::collections::HashSet<String> = paths.into_iter().collect();
    (distinct.len(), entries)
}

/// The file-scan configuration under a plan.
fn scan_config(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
    let mut node: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            return exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
                .expect("a FastObjectTable scan is a file scan");
        }
        let children = node.children();
        assert_eq!(
            children.len(),
            1,
            "expected a single-child chain to the scan"
        );
        node = children[0].as_ref();
    }
}

fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    scan_config(plan)
        .file_groups
        .iter()
        .flat_map(|group| group.iter().map(|f| f.object_meta.location.to_string()))
        .collect()
}

fn counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<usize> {
    plan.metrics()?
        .sum_by_name(name)
        .map(|value| value.as_usize())
}

/// What the plan's file list costs, in bytes.
///
/// The struct plus the bytes it points at. This is the number the issue quotes
/// as ~1.1 GB for three million files, so it is the one worth reporting.
fn file_list_bytes(config: &FileScanConfig) -> usize {
    config
        .file_groups
        .iter()
        .flat_map(|group| group.iter())
        .map(|file| {
            std::mem::size_of::<PartitionedFile>()
                + file.object_meta.location.as_ref().len()
                + file.object_meta.e_tag.as_ref().map_or(0, |tag| tag.len())
        })
        .sum()
}

/// Open file descriptors held by this process.
///
/// `/dev/fd` lists them on both macOS and Linux. Reading it opens one itself,
/// which the count includes.
fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd")
        .map(|dir| dir.count())
        .unwrap_or(0)
}

/// This process's soft descriptor limit, as a string.
///
/// Worth printing beside the peak, because whether a given peak is fatal is a
/// property of the machine, not of the code. A developer box often allows a
/// million; the server that produced issue #361 did not.
fn fd_limit() -> String {
    std::process::Command::new("sh")
        .args(["-c", "ulimit -n"])
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Resident memory, in bytes. `None` where `ps` cannot say.
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
///
/// The EMFILE the issue reports happens inside a plan, not at either end of it,
/// so the count has to be watched rather than read afterwards.
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

/// A hundred thousand analyzed files, planned three ways.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "writes ~100 MB and takes minutes; run explicitly with --ignored --nocapture"]
async fn a_hundred_thousand_files_plan_and_prune() {
    let count = file_count();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    println!("\n=== {count} files under {} ===", root.display());

    // --- Setup: the files, and a recorded range for each ------------------
    let started = Instant::now();
    let observed = write_files(&root, count);
    let written = started.elapsed();
    let stored: u64 = observed.iter().map(|file| file.size).sum();
    assert_eq!(observed.len(), count);
    println!(
        "write        {:>9.2?}  {:.0} MiB on disk",
        written,
        mib(stored as usize)
    );

    let stats = open_stats(&root).await;
    let started = Instant::now();
    fill_statistics(&stats, &observed).await;
    let analyzed = started.elapsed();

    // The statistics store holds every file, in the segment layout a collector
    // pass would have left.
    assert_eq!(
        stats.registry().num_files().unwrap(),
        count as u64,
        "every file must be interned"
    );
    assert_eq!(
        stats.registry().num_pending().unwrap(),
        0,
        "every file must be analyzed, not pending"
    );
    assert_eq!(
        stats.num_segments().await,
        count.div_ceil(SEGMENT_FILES),
        "one segment per batch"
    );
    println!(
        "analyze      {:>9.2?}  {} files, {} segments",
        analyzed,
        stats.registry().num_files().unwrap(),
        stats.num_segments().await
    );

    // --- A session over those files ---------------------------------------
    let ctx = session(&root, Arc::clone(&stats));
    let before = rss_bytes();

    // --- Schema inference: the cost that opens every file -----------------
    let watch = FdWatch::start();
    let started = Instant::now();
    let table = table_over_obs(&ctx).await;
    let inferred = started.elapsed();
    let inference_fds = watch.finish();
    println!(
        "infer schema {inferred:>9.2?}  peak {inference_fds} open fds (soft limit {})",
        fd_limit()
    );

    // Inference reads footers, so an empty schema means the listing found
    // nothing and every measurement below would be of an empty table.
    assert_eq!(
        table.schema().fields().len(),
        1,
        "inference should have found the `v` column; got {:?}",
        table.schema()
    );

    // The crash this replaces was `Too many open files` at 57 367 descriptors
    // during a 60 000 file plan. This asserts the count rather than the crash:
    // a box whose soft limit is a million never raises EMFILE however many
    // descriptors it holds, so the count is the part that transfers.
    assert!(
        inference_fds < 4_096,
        "schema inference held {inference_fds} descriptors open; \
         issue #361 is exactly this failure"
    );

    // --- Planning with no predicate ---------------------------------------
    let watch = FdWatch::start();
    let started = Instant::now();
    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    let planned = started.elapsed();
    let plan_fds = watch.finish();

    let config = scan_config(&plan);
    let listed = planned_files(&plan).len();
    let footprint = file_list_bytes(config);
    assert_eq!(listed, count, "the whole listing reaches the scan");
    println!(
        "plan (all)   {planned:>9.2?}  {listed} files in {} groups, list {:.1} MiB, peak {plan_fds} open fds",
        config.file_groups.len(),
        mib(footprint),
    );
    // A plan is built, not read. Opening files here would be the second half of
    // the same bug.
    assert!(
        plan_fds < 4_096,
        "planning held {plan_fds} descriptors open"
    );

    // --- Planning with a predicate that keeps ten files --------------------
    let cutoff = (count - SURVIVORS) as f64;
    let filters = vec![col("v").gt_eq(lit(cutoff))];
    let started = Instant::now();
    let plan = table
        .scan(&ctx.state(), None, &filters, None)
        .await
        .unwrap();
    let pruned = started.elapsed();

    let mut kept = planned_files(&plan);
    kept.sort();
    let expected: Vec<String> = (count - SURVIVORS..count).map(path_for).collect();
    assert_eq!(
        kept, expected,
        "pruning must keep exactly the files whose range reaches the cutoff \
         (a mismatch here usually means the registry paths and the listing \
         paths disagree, so nothing was found to prune)"
    );
    assert_eq!(counter(&plan, "file_stats_files_considered"), Some(count));
    assert_eq!(
        counter(&plan, "file_stats_files_pruned"),
        Some(count - SURVIVORS)
    );
    println!(
        "plan (prune) {:>9.2?}  {} of {} files survive, list {:.1} MiB",
        pruned,
        kept.len(),
        count,
        mib(file_list_bytes(scan_config(&plan))),
    );

    // --- And it still reads ------------------------------------------------
    let started = Instant::now();
    let rows: usize = collect(plan, ctx.task_ctx())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum();
    let read = started.elapsed();
    assert_eq!(rows, SURVIVORS * 2, "two rows from each surviving file");
    println!("execute      {read:>9.2?}  {rows} rows");

    // --- A query that actually reads a quarter of the store ----------------
    //
    // Ten files prove pruning is right. They prove nothing about reading, and
    // reading is where descriptors are held: a scan opens files, a plan mostly
    // does not. This goes through SQL rather than `scan` directly, so the
    // optimizer, the repartitioning and the filter that sits above an `Inexact`
    // pushdown are all in the path the rows travel.
    let reads = count / 4;
    let cutoff = (count - reads) as f64;
    ctx.register_table("obs", Arc::clone(&table) as Arc<dyn TableProvider>)
        .unwrap();

    let started = Instant::now();
    let plan = ctx
        .sql(&format!("SELECT v FROM obs WHERE v >= {cutoff}"))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let planned_query = started.elapsed();
    let unexpected: Vec<String> = planned_files(&plan)
        .into_iter()
        .filter(|path| *path < path_for(count - reads))
        .collect();
    assert!(
        unexpected.is_empty(),
        "{} files below the cutoff survived, first few {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(6)]
    );
    let (distinct, entries) = planned_files_and_entries(&plan);
    assert_eq!(
        distinct, reads,
        "a quarter of the files hold a value at or above the cutoff"
    );
    println!(
        "query plan   {planned_query:>9.2?}  {distinct} files ({entries} scan entries) in {} groups, list {:.1} MiB",
        scan_config(&plan).file_groups.len(),
        mib(file_list_bytes(scan_config(&plan))),
    );

    let watch = FdWatch::start();
    let started = Instant::now();
    let rows: usize = collect(plan, ctx.task_ctx())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum();
    let read = started.elapsed();
    let read_fds = watch.finish();
    assert_eq!(
        rows,
        reads * 2,
        "every row in a surviving file is at or above the cutoff, so all of \
         them come back"
    );
    println!("query read   {read:>9.2?}  {rows} rows from {reads} files, peak {read_fds} open fds");
    // Reading is the phase that holds descriptors, so this is the bound that
    // matters most.
    assert!(
        read_fds < 4_096,
        "reading {reads} files held {read_fds} descriptors open"
    );

    // --- What a reused table saves, and what it does not -------------------
    //
    // Building the table is what costs the schema inference above, and a table
    // held across queries pays it once. Planning is a different matter: `scan`
    // lists the store every time it is called, because the cache manager caches
    // file statistics and explicitly does not cache listings. So these repeats
    // are the steady-state cost of a query against a table that already exists.
    for round in 1..=3 {
        let started = Instant::now();
        let plan = table
            .scan(&ctx.state(), None, &filters, None)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(planned_files_and_entries(&plan).0, SURVIVORS);
        println!("re-plan #{round}  {elapsed:>9.2?}  same table, same predicate");
    }

    if let (Some(before), Some(after)) = (before, rss_bytes()) {
        println!(
            "rss          {:>9.1} MiB -> {:.1} MiB",
            mib(before),
            mib(after)
        );
    }

    // --- What one plan would cost at the sizes the issue names -------------
    let per_file = footprint as f64 / count as f64;
    println!(
        "\nfile list costs {per_file:.0} B/file: {:.0} MiB at 1M files, {:.2} GiB at 3M",
        per_file * 1_000_000.0 / (1024.0 * 1024.0),
        per_file * 3_000_000.0 / (1024.0 * 1024.0 * 1024.0),
    );
}

/// The same shape at a size that splits the prune into parallel tasks.
///
/// `prune_tasks` divides by 65 536, so 100 000 candidates is still one call and
/// the parallel path above it goes unmeasured. This is the smallest size that
/// reaches two tasks.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "larger than the 100k check; run explicitly"]
async fn the_parallel_prune_path_is_reached_above_131072_files() {
    // Deliberately not sharing the body above: this one only has to prove the
    // parallel branch produces the same answer the serial branch does.
    let count = 131_072 + SURVIVORS;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    println!("\n=== {count} files, the parallel prune path ===");

    let observed = write_files(&root, count);
    let stats = open_stats(&root).await;
    fill_statistics(&stats, &observed).await;
    assert_eq!(stats.registry().num_files().unwrap(), count as u64);

    let ctx = session(&root, Arc::clone(&stats));
    let table = table_over_obs(&ctx).await;

    let filters = vec![col("v").gt_eq(lit((count - SURVIVORS) as f64))];
    let started = Instant::now();
    let plan = table
        .scan(&ctx.state(), None, &filters, None)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    let mut kept = planned_files(&plan);
    kept.sort();
    let expected: Vec<String> = (count - SURVIVORS..count).map(path_for).collect();
    assert_eq!(
        kept, expected,
        "chunked pruning must answer what one call would have"
    );
    println!(
        "plan (prune) {:>9.2?}  {} of {count} survive",
        elapsed,
        kept.len()
    );
}
