//! The background pass, driven against a fake analyzer.
//!
//! A fake is the right tool here: the collector's job is batching, ordering,
//! failure handling, and durability, none of which involve reading a real file.
//! What a netCDF reader returns is the format layer's problem.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    CollectorConfig, FileAnalysis, FileAnalyzer, FileState, FileStatsStore, ObservedFile, Registry,
    Result, StatScalar, StatsCollector,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

/// Reports a fixed column set per file, and fails for any path holding "bad".
struct FakeAnalyzer {
    /// Column names per file path. Missing paths get a default pair.
    columns: HashMap<String, Vec<&'static str>>,
    calls: AtomicUsize,
    /// How often the collector announced the start of a pass.
    passes: AtomicUsize,
}

impl FakeAnalyzer {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
            calls: AtomicUsize::new(0),
            passes: AtomicUsize::new(0),
        }
    }

    fn with_columns(mut self, path: &str, names: Vec<&'static str>) -> Self {
        self.columns.insert(path.to_string(), names);
        self
    }
}

#[async_trait::async_trait]
impl FileAnalyzer for FakeAnalyzer {
    fn begin_pass(&self) {
        self.passes.fetch_add(1, Ordering::SeqCst);
    }

    async fn analyze(&self, record: &beacon_file_stats::FileRecord) -> Result<FileAnalysis> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if record.path.contains("bad") {
            return Err(beacon_file_stats::FileStatsError::Format(
                "cannot read this file".into(),
            ));
        }
        let names = self
            .columns
            .get(&record.path)
            .cloned()
            .unwrap_or_else(|| vec!["TEMP", "PSAL"]);
        Ok(FileAnalysis {
            format: "fake".into(),
            num_rows: Some(1_000),
            total_byte_size: Some(record.size),
            columns: names
                .into_iter()
                .map(|name| {
                    (
                        name.to_string(),
                        ColumnStat {
                            min: StatScalar::F64(0.0),
                            max: StatScalar::F64(10.0),
                            null_count: Some(0),
                            row_count: Some(1_000),
                            data_type: DataType::Float64,
                        },
                    )
                })
                .collect(),
        })
    }
}

async fn store() -> (Arc<FileStatsStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::open(dir.path().join("registry.redb")).unwrap());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = FileStatsStore::open(registry, object_store, Path::from("stats"))
        .await
        .unwrap();
    (Arc::new(store), dir)
}

/// The tests below pin grouping behaviour, so they fix the depth rather than
/// letting it be derived. `collector.rs`'s unit tests cover the derivation.
fn config(prefix_depth: usize) -> CollectorConfig {
    CollectorConfig {
        batch_files: 100,
        concurrency: 4,
        target_group_files: 10_000,
        min_group_files: 500,
        prefix_depth: Some(prefix_depth),
    }
}

/// The collector drains the queue, commits segments, and records the summary
/// the scan layer reads back per file.
#[tokio::test]
async fn a_pass_drains_the_queue_and_records_the_summary() {
    let (store, _dir) = store().await;
    let files: Vec<ObservedFile> = (0..5)
        .map(|i| ObservedFile::new(format!("argo/2024/{i}.nc"), 4096, 1))
        .collect();
    store.registry().intern_files(&files).unwrap();
    assert_eq!(store.registry().num_pending().unwrap(), 5);

    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), config(2));
    let report = collector.run_once().await.unwrap();

    assert_eq!(report.analyzed, 5);
    assert_eq!(report.failed, 0);
    assert_eq!(report.segments, 1);
    assert_eq!(store.registry().num_pending().unwrap(), 0);

    let record = store.registry().record(0).unwrap().unwrap();
    assert_eq!(record.state, FileState::Analyzed);
    assert_eq!(record.num_rows, Some(1_000));
    assert_eq!(record.format, "fake");

    let temp = store.column_stats_by_name("TEMP", (0, 4)).await.unwrap();
    assert_eq!(temp[0].file_ids, vec![0, 1, 2, 3, 4]);
}

/// The analyzer is told when a pass starts, once, and only when there is work.
///
/// It is the hook a whole-pass condition is reported through. A reader that can
/// produce no ranges is true of every file it opens, so the analyzer says it once
/// per pass rather than once per file. An idle tick is not a pass, or a server
/// that ticks all night would repeat a condition nothing acted on.
#[tokio::test]
async fn a_pass_with_work_in_it_is_announced_once() {
    let (store, _dir) = store().await;
    store
        .registry()
        .intern_files(&[ObservedFile::new("argo/2024/a.nc", 1, 1)])
        .unwrap();

    let analyzer = Arc::new(FakeAnalyzer::new());
    let collector = StatsCollector::new(store.clone(), analyzer.clone(), config(2));

    collector.run_once().await.unwrap();
    assert_eq!(analyzer.passes.load(Ordering::SeqCst), 1);

    // The queue is empty now, so this tick is not a pass.
    let idle = collector.run_once().await.unwrap();
    assert!(idle.is_idle());
    assert_eq!(analyzer.passes.load(Ordering::SeqCst), 1);

    // A new file makes the next tick a pass again.
    store
        .registry()
        .intern_files(&[ObservedFile::new("argo/2024/b.nc", 1, 1)])
        .unwrap();
    collector.run_once().await.unwrap();
    assert_eq!(analyzer.passes.load(Ordering::SeqCst), 2);
}

/// One segment per prefix group, not one per batch. This is the rule the
/// manifest's skip depends on.
#[tokio::test]
async fn a_batch_becomes_one_segment_per_prefix_group() {
    let (store, _dir) = store().await;
    let mut files = Vec::new();
    for family in ["argo/2024", "ctd/2024", "argo/2025"] {
        for i in 0..3 {
            files.push(ObservedFile::new(format!("{family}/{i}.nc"), 1, 1));
        }
    }
    store.registry().intern_files(&files).unwrap();

    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), config(2));
    let report = collector.run_once().await.unwrap();

    assert_eq!(report.groups, 3);
    assert_eq!(report.segments, 3);
    assert_eq!(store.num_segments().await, 3);
}

/// A file that cannot be read must not stop the batch, and must not stay on the
/// queue: a failure that re-queues itself is retried forever.
#[tokio::test]
async fn a_failing_file_leaves_the_queue_without_stopping_the_batch() {
    let (store, _dir) = store().await;
    store
        .registry()
        .intern_files(&[
            ObservedFile::new("argo/2024/good-0.nc", 1, 1),
            ObservedFile::new("argo/2024/bad.nc", 1, 1),
            ObservedFile::new("argo/2024/good-1.nc", 1, 1),
        ])
        .unwrap();

    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), config(2));
    let report = collector.run_once().await.unwrap();

    assert_eq!(report.analyzed, 2);
    assert_eq!(report.failed, 1);
    assert_eq!(store.registry().num_pending().unwrap(), 0);
    assert_eq!(store.registry().record(1).unwrap().unwrap().state, FileState::Failed);

    // The two readable files still made it into a segment.
    let temp = store.column_stats_by_name("TEMP", (0, 2)).await.unwrap();
    assert_eq!(temp[0].file_ids, vec![0, 2]);
}

/// Blocks must be sorted by file id. Analysis runs concurrently and returns out
/// of order, so the collector has to put them back.
#[tokio::test]
async fn concurrent_analysis_still_yields_sorted_blocks() {
    let (store, _dir) = store().await;
    let files: Vec<ObservedFile> = (0..40)
        .map(|i| ObservedFile::new(format!("argo/2024/{i:03}.nc"), 1, 1))
        .collect();
    store.registry().intern_files(&files).unwrap();

    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), config(2));
    collector.run_once().await.unwrap();

    let temp = store.column_stats_by_name("TEMP", (0, 39)).await.unwrap();
    let ids = &temp[0].file_ids;
    assert_eq!(ids.len(), 40);
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "blocks came back unsorted: {ids:?}"
    );
}

/// A changed file goes back on the queue, and a second pass writes fresh
/// statistics for it. The old segment keeps its row, so the reader has to prefer
/// the newest.
#[tokio::test]
async fn a_re_analyzed_file_appears_in_a_newer_segment() {
    let (store, _dir) = store().await;
    store
        .registry()
        .intern_files(&[ObservedFile::new("argo/2024/0.nc", 1, 1)])
        .unwrap();

    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), config(2));
    collector.run_once().await.unwrap();
    assert_eq!(store.num_segments().await, 1);

    // The file changes, so the registry re-queues it.
    store
        .registry()
        .intern_files(&[ObservedFile::new("argo/2024/0.nc", 999, 2)])
        .unwrap();
    assert_eq!(store.registry().num_pending().unwrap(), 1);

    let report = collector.run_once().await.unwrap();
    assert_eq!(report.analyzed, 1);
    assert_eq!(store.num_segments().await, 2);
    assert_eq!(store.registry().record(0).unwrap().unwrap().stats_epoch, 2);

    // Both segments now hold a row for file 0. Resolving that is the reader's
    // job, and it prefers the newest.
    let stats = store.column_stats_by_name("TEMP", (0, 0)).await.unwrap();
    assert_eq!(stats.len(), 2);
}

/// Running with nothing pending must be cheap and say so.
#[tokio::test]
async fn an_empty_queue_reports_idle_without_calling_the_analyzer() {
    let (store, _dir) = store().await;
    let analyzer = Arc::new(FakeAnalyzer::new());
    let collector = StatsCollector::new(store.clone(), analyzer.clone(), config(2));

    let report = collector.run_once().await.unwrap();
    assert!(report.is_idle());
    assert_eq!(store.num_segments().await, 0);
    assert_eq!(analyzer.calls.load(Ordering::SeqCst), 0);
}

/// `run_until_idle` keeps going across batches, and stops when the queue is dry.
#[tokio::test]
async fn run_until_idle_covers_more_files_than_one_batch() {
    let (store, _dir) = store().await;
    let files: Vec<ObservedFile> = (0..25)
        .map(|i| ObservedFile::new(format!("argo/2024/{i:03}.nc"), 1, 1))
        .collect();
    store.registry().intern_files(&files).unwrap();

    let small_batches = CollectorConfig {
        batch_files: 10,
        concurrency: 4,
        target_group_files: 10_000,
        min_group_files: 500,
        prefix_depth: Some(2),
    };
    let collector = StatsCollector::new(store.clone(), Arc::new(FakeAnalyzer::new()), small_batches);

    let report = collector.run_until_idle(10).await.unwrap();
    assert_eq!(report.analyzed, 25);
    assert_eq!(report.segments, 3, "10 + 10 + 5");
    assert_eq!(store.registry().num_pending().unwrap(), 0);
}

/// Files declaring different columns each land in their own block, and no file
/// contributes a row to a column it never declared.
#[tokio::test]
async fn a_file_only_reaches_the_columns_it_declares() {
    let (store, _dir) = store().await;
    store
        .registry()
        .intern_files(&[
            ObservedFile::new("argo/2024/0.nc", 1, 1),
            ObservedFile::new("argo/2024/1.nc", 1, 1),
        ])
        .unwrap();

    let analyzer = FakeAnalyzer::new()
        .with_columns("argo/2024/0.nc", vec!["TEMP"])
        .with_columns("argo/2024/1.nc", vec!["PSAL", "DEPTH"]);
    let collector = StatsCollector::new(store.clone(), Arc::new(analyzer), config(2));
    collector.run_once().await.unwrap();

    assert_eq!(
        store.column_stats_by_name("TEMP", (0, 1)).await.unwrap()[0].file_ids,
        vec![0]
    );
    assert_eq!(
        store.column_stats_by_name("PSAL", (0, 1)).await.unwrap()[0].file_ids,
        vec![1]
    );
    assert_eq!(
        store.column_stats_by_name("DEPTH", (0, 1)).await.unwrap()[0].file_ids,
        vec![1]
    );
    assert_eq!(store.registry().num_columns().unwrap(), 3);
}
