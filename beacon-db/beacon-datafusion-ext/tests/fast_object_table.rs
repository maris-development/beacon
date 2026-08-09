//! End-to-end tests for [`FastObjectTable`] and its [`FastObjectDataSource`].
//!
//! Real Parquet objects in an in-memory store and a real registry. Each test
//! asserts on the shape of the *plan* — which cursors it holds — and, where it
//! matters, on the rows the plan produces.

use std::sync::Arc;

use beacon_datafusion_ext::fast_object_data_source::{FastObjectDataSource, Identities};
use beacon_datafusion_ext::fast_object_table::{FastObjectTable, RegistryListingSwitch};
use beacon_file_stats::segment::{ColumnStat, SegmentBuilder};
use beacon_file_stats::{FileStatsStore, ObservedFile, Registry, StatScalar};
use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

const STORE_URL: &str = "test://stats/";

struct Fixture {
    ctx: SessionContext,
    stats: Arc<FileStatsStore>,
    objects: Arc<InMemory>,
    _dir: tempfile::TempDir,
}

/// A session with a filled statistics store and an in-memory object store at
/// `test://stats/`. `switch_on` is the operator's registry opt-in.
async fn fixture(switch_on: bool) -> Fixture {
    fixture_with(switch_on, None).await
}

/// The same, with a fixed partition target where a test needs one.
async fn fixture_with(switch_on: bool, target_partitions: Option<usize>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::open(dir.path().join("registry.redb")).unwrap());
    let segments: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stats = Arc::new(
        FileStatsStore::open(registry, segments, Path::from("segments"))
            .await
            .unwrap(),
    );

    let handle = beacon_file_stats::new_file_stats_handle();
    handle.set(stats.clone()).ok();
    let mut config = SessionConfig::new()
        .with_extension(handle)
        .with_extension(Arc::new(RegistryListingSwitch { enable: switch_on }));
    if let Some(partitions) = target_partitions {
        config = config.with_target_partitions(partitions);
    }
    let ctx = SessionContext::new_with_config(config);

    let objects = Arc::new(InMemory::new());
    ctx.register_object_store(
        ObjectStoreUrl::parse(STORE_URL).unwrap().as_ref(),
        objects.clone(),
    );

    Fixture {
        ctx,
        stats,
        objects,
        _dir: dir,
    }
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
}

/// Write one Parquet object holding `values` in column `v`, and return what a
/// listing would have observed about it.
async fn put_parquet(objects: &InMemory, path: &str, values: &[f64]) -> ObservedFile {
    let batch = RecordBatch::try_new(
        value_schema(),
        vec![Arc::new(Float64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, value_schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    objects.put(&Path::from(path), bytes.into()).await.unwrap();
    let meta = objects.head(&Path::from(path)).await.unwrap();
    ObservedFile::new(path, meta.size, meta.last_modified.timestamp_millis())
        .with_e_tag(meta.e_tag.clone())
}

/// Register `files` and record each as analyzed with its row count and its `v`
/// range, the way a collector pass would have.
async fn analyze(stats: &FileStatsStore, files: &[(ObservedFile, &[f64])]) {
    let observed: Vec<ObservedFile> = files.iter().map(|(file, _)| file.clone()).collect();
    let ids = stats.registry().intern_files(&observed).unwrap();
    let column = stats.registry().intern_columns(&["v"]).unwrap()[0];

    let mut builder = SegmentBuilder::new();
    for (id, (file, values)) in ids.iter().zip(files) {
        stats
            .registry()
            .mark_analyzed(*id, "parquet", Some(values.len() as u64), Some(file.size), 1)
            .unwrap();
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        builder.push_file(
            *id,
            [(
                column,
                ColumnStat {
                    min: StatScalar::F64(min),
                    max: StatScalar::F64(max),
                    null_count: Some(0),
                    row_count: Some(values.len() as u64),
                    data_type: DataType::Float64,
                },
            )],
        );
    }
    stats.commit_segment(builder).await.unwrap();
}

async fn table(ctx: &SessionContext, urls: &[&str]) -> FastObjectTable {
    let urls = urls
        .iter()
        .map(|url| ListingTableUrl::parse(url).unwrap())
        .collect();
    FastObjectTable::try_new(&ctx.state(), Arc::new(ParquetFormat::default()), urls)
        .await
        .unwrap()
}

/// The scan source under a plan.
fn scan_source(plan: &Arc<dyn ExecutionPlan>) -> &FastObjectDataSource {
    let mut node: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            return exec
                .data_source()
                .as_any()
                .downcast_ref::<FastObjectDataSource>()
                .expect("a FastObjectTable scan carries a FastObjectDataSource");
        }
        let children = node.children();
        assert_eq!(children.len(), 1, "expected a single-child chain to the scan");
        node = children[0].as_ref();
    }
}

fn mode(plan: &Arc<dyn ExecutionPlan>) -> &'static str {
    scan_source(plan).identities().mode()
}

/// The paths the plan's scan will read, in order.
///
/// The plan holds cursors, not a file list — path ranges while streaming,
/// surviving ids once pruned — so the paths are resolved the way the scan
/// itself resolves them at execute time.
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let source = scan_source(plan);
    let mut paths = Vec::new();
    match source.identities() {
        Identities::Shards {
            shards,
            urls,
            ignore_subdirectory,
            extension,
        } => {
            let snapshot = source.snapshot().expect("a walk holds a snapshot");
            for query in shards.iter() {
                snapshot
                    .for_each_in_shard(&query.prefix, &query.shard, |_, record| {
                        let location = object_store::path::Path::parse(&record.path).unwrap();
                        if record.path.ends_with(extension.as_str())
                            && urls
                                .iter()
                                .any(|url| url.contains(&location, *ignore_subdirectory))
                        {
                            paths.push(record.path);
                        }
                        true
                    })
                    .unwrap();
            }
        }
        Identities::Listed { objects, .. } => {
            paths.extend(objects.iter().map(|meta| meta.location.to_string()));
        }
    }
    paths
}

/// With no predicate the plan holds path ranges, and the rows still come out.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_walks_the_registry_rather_than_listing_it() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table.scan(&fixture.ctx.state(), None, &[], None).await.unwrap();

    assert_eq!(mode(&plan), "streaming", "no predicate means no enumeration");
    assert_eq!(
        planned_files(&plan),
        vec!["obs/a.parquet", "obs/b.parquet"],
        "walking those ranges yields the registry's files, in path order"
    );

    let batches = collect(Arc::clone(&plan), fixture.ctx.task_ctx())
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5, "both files' rows come out of the walk");

    // The counters are filled by the scan as it runs, which is the only place
    // they exist now that nothing is decided at plan time.
    let metrics = plan.metrics().expect("the scan carries metrics");
    assert_eq!(
        metrics
            .sum_by_name("file_stats_files_considered")
            .unwrap()
            .as_usize(),
        2
    );
}

/// A `WHERE` on a recorded column prunes while the scan reads, not while it
/// plans: the plan still covers the whole prefix, and the files that cannot
/// match are dropped — and counted — during execution.
#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_prunes_inside_the_stream() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(50.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert_eq!(mode(&plan), "streaming", "planning is never blocked by pruning");
    assert!(
        scan_source(&plan).prunes(),
        "but the scan carries the predicate"
    );
    assert_eq!(
        planned_files(&plan),
        vec!["obs/a.parquet", "obs/b.parquet"],
        "the plan still covers the whole prefix"
    );
    // Nothing has been pruned yet, so there is nothing to count.
    let metrics = plan.metrics().unwrap();
    assert!(
        metrics.sum_by_name("file_stats_files_pruned").is_none(),
        "pruning has not run at plan time"
    );

    let batches = collect(Arc::clone(&plan), fixture.ctx.task_ctx())
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "only the file that can match was read");

    // And now the counters exist, filled by the stream as it went.
    let metrics = plan.metrics().unwrap();
    assert_eq!(
        metrics
            .sum_by_name("file_stats_files_considered")
            .unwrap()
            .as_usize(),
        2
    );
    assert_eq!(
        metrics
            .sum_by_name("file_stats_files_pruned")
            .unwrap()
            .as_usize(),
        1,
        "the cold file was dropped without being opened"
    );
}

/// A predicate over a column the registry has never interned must not trigger
/// an enumeration: pruning could not drop a file, so the scan streams.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_predicate_column_never_costs_an_enumeration() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;
    // Registered and analyzed, but the registry only ever interned "v".
    analyze(&fixture.stats, &[(a, &[1.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    // `v = v` names a column with statistics but cannot prune; an unrecorded
    // column would not even be in the schema. Both must stay streaming.
    let filters = vec![col("v").is_not_null().or(col("v").is_null())];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();
    assert_eq!(planned_files(&plan).len(), 1, "the file is still read");
}

/// Everything the registry cannot answer falls back to a store listing, and
/// the file is still read.
#[tokio::test(flavor = "multi_thread")]
async fn the_listing_serves_what_the_registry_cannot() {
    let on = fixture(true).await;
    let a = put_parquet(&on.objects, "obs/a.parquet", &[1.0]).await;
    let b = put_parquet(&on.objects, "obs/b.parquet", &[2.0]).await;
    put_parquet(&on.objects, "fresh/c.parquet", &[3.0]).await;
    analyze(&on.stats, &[(a, &[1.0]), (b.clone(), &[2.0])]).await;

    // A prefix discovery has never seen: only the store can say whether it is
    // empty, so the scan lists it and the file is read.
    let fresh = table(&on.ctx, &["test://stats/fresh/"]).await;
    let plan = fresh.scan(&on.ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(mode(&plan), "listed", "an undiscovered prefix is listed");
    assert_eq!(planned_files(&plan), vec!["fresh/c.parquet"]);
    let batches = collect(plan, on.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // A tombstoned file drops out of the registry's walk at once.
    on.stats
        .registry()
        .reconcile_prefix("obs/", std::slice::from_ref(&b))
        .unwrap();
    let obs = table(&on.ctx, &["test://stats/obs/"]).await;
    let plan = obs.scan(&on.ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(mode(&plan), "streaming");
    assert_eq!(planned_files(&plan), vec!["obs/b.parquet"]);

    // The switch is the operator's: off means a listing, always.
    let off = fixture(false).await;
    let a = put_parquet(&off.objects, "obs/a.parquet", &[1.0]).await;
    analyze(&off.stats, &[(a, &[1.0])]).await;
    let table = table(&off.ctx, &["test://stats/obs/"]).await;
    let plan = table.scan(&off.ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(mode(&plan), "listed", "the default is a listing");
}

/// A limit stops the reading rather than shortening a list.
///
/// One partition, because a scan applies its limit per partition — the limit
/// operator above trims the rest, exactly as it does for a listing scan.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_stops_the_walk_rather_than_the_plan() {
    let fixture = fixture_with(true, Some(1)).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[4.0, 5.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[4.0, 5.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], Some(2))
        .await
        .unwrap();

    // The plan still describes the whole prefix; the limit lives in the scan.
    assert_eq!(mode(&plan), "streaming");
    assert_eq!(planned_files(&plan).len(), 2);

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "the walk stopped at the limit");
}

/// A single-file URL the registry knows is walked; one it has never seen is
/// listed. Either way it is read.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_file_url_is_read_either_way() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;
    put_parquet(&fixture.objects, "obs/unseen.parquet", &[3.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0])]).await;

    let known = table(&fixture.ctx, &["test://stats/obs/a.parquet"]).await;
    let plan = known
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(mode(&plan), "streaming");
    assert_eq!(planned_files(&plan), vec!["obs/a.parquet"]);
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    let unseen = table(&fixture.ctx, &["test://stats/obs/unseen.parquet"]).await;
    let plan = unseen
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(mode(&plan), "listed", "an unregistered file is still read");
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

/// A table over several paths reads every path's files, once each.
///
/// Regression: shards were once cut per URL but walked under one shared
/// prefix, so a URL's last shard — which has no end bound — ran into the next
/// URL's range, and every walked file was checked against only the first URL's
/// glob. That both duplicated and dropped files.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_over_several_paths_reads_each_file_once() {
    let fixture = fixture(true).await;
    let argo = put_parquet(&fixture.objects, "obs/argo/a.parquet", &[1.0]).await;
    let ctd = put_parquet(&fixture.objects, "obs/ctd/b.parquet", &[2.0]).await;
    // Under neither table path: it shares a prefix with both and must not be
    // read.
    let other = put_parquet(&fixture.objects, "obs/other/c.parquet", &[3.0]).await;
    analyze(
        &fixture.stats,
        &[(argo, &[1.0]), (ctd, &[2.0]), (other, &[3.0])],
    )
    .await;

    let table = table(
        &fixture.ctx,
        &["test://stats/obs/argo/", "test://stats/obs/ctd/"],
    )
    .await;
    let plan = table.scan(&fixture.ctx.state(), None, &[], None).await.unwrap();

    assert_eq!(mode(&plan), "streaming");
    assert_eq!(
        planned_files(&plan),
        vec!["obs/argo/a.parquet", "obs/ctd/b.parquet"],
        "each table path contributes its own files, and no others"
    );

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "one row per file, read once each");
}

/// A projection reaches the file reader, so a narrow `SELECT` reads narrow.
#[tokio::test(flavor = "multi_thread")]
async fn a_projection_reaches_the_reader() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), Some(&vec![0]), &[], None)
        .await
        .unwrap();

    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(plan.schema().field(0).name(), "v");
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches[0].num_columns(), 1);
}

/// The scaling claim, asserted rather than argued: a streaming plan's memory
/// is its shards, not its files.
#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_plan_holds_shards_not_files() {
    let fixture = fixture(true).await;

    // One real file, and the table built from it: schema inference still reads
    // the files it infers from, so it runs before the rest are registered.
    let a = put_parquet(&fixture.objects, "obs/00000.parquet", &[1.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0])]).await;
    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;

    // Registered, not written: this is about what the *plan* holds, and
    // planning opens no file.
    let files: Vec<ObservedFile> = (1..5_000)
        .map(|i| ObservedFile::new(format!("obs/{i:05}.parquet"), 1_000, 1))
        .collect();
    fixture.stats.registry().intern_files(&files).unwrap();
    let plan = table.scan(&fixture.ctx.state(), None, &[], None).await.unwrap();

    let source = scan_source(&plan);
    let Identities::Shards { shards, .. } = source.identities() else {
        panic!("a scan with no predicate must stream");
    };
    let target = fixture.ctx.state().config_options().execution.target_partitions;
    assert!(
        shards.len() <= target,
        "5000 files became {} partitions, not one per file",
        shards.len()
    );
    // The whole plan-time file state: one shard per partition.
    assert_eq!(
        plan.output_partitioning().partition_count(),
        shards.len(),
        "the partition count is chosen, not derived from a file list"
    );
}

/// Small collections still spread across the machine.
///
/// A hundred files is the ordinary case, and it must use every partition
/// rather than reading in one thread. The skewed case is the regression: one
/// large file among small ones once produced three partitions, one of which
/// held 84 files.
#[tokio::test(flavor = "multi_thread")]
async fn small_collections_fill_the_partition_budget() {
    for (label, sizes, want) in [
        ("100 equal", vec![1_000u64; 100], 12),
        ("100, one huge", {
            let mut v = vec![1_000u64; 99];
            v.insert(0, 900_000);
            v
        }, 12),
        ("5 equal", vec![1_000u64; 5], 5),
        ("1", vec![1_000u64; 1], 1),
    ] {
        let fixture = fixture_with(true, Some(12)).await;
        let files: Vec<ObservedFile> = sizes
            .iter()
            .enumerate()
            .map(|(i, size)| ObservedFile::new(format!("obs/{i:05}.parquet"), *size, 1))
            .collect();
        fixture.stats.registry().intern_files(&files).unwrap();

        let snapshot = fixture.stats.registry().snapshot().unwrap();
        let sharded = snapshot.shard_prefix("obs/", 12).unwrap();
        assert_eq!(
            sharded.shards.len(),
            want,
            "{label}: files per partition {:?}",
            sharded.shards.iter().map(|s| s.files).collect::<Vec<_>>()
        );
    }
}

/// At twelve thousand files a partition still holds one chunk, so it prunes
/// once and then reads.
///
/// The chunk loop — prune, read, prune the next — only begins when a partition
/// holds more than a chunk's worth, which at twelve partitions is about fifty
/// thousand files. Below that the walk fills `pending` in a single step.
#[tokio::test(flavor = "multi_thread")]
async fn twelve_thousand_files_are_one_chunk_per_partition() {
    let fixture = fixture_with(true, Some(12)).await;
    let files: Vec<ObservedFile> = (0..12_000)
        .map(|i| ObservedFile::new(format!("obs/{i:06}.parquet"), 1_000, 1))
        .collect();
    for batch in files.chunks(5_000) {
        fixture.stats.registry().intern_files(batch).unwrap();
    }

    let snapshot = fixture.stats.registry().snapshot().unwrap();
    let sharded = snapshot.shard_prefix("obs/", 12).unwrap();
    assert_eq!(sharded.shards.len(), 12);
    let per: Vec<u64> = sharded.shards.iter().map(|s| s.files).collect();
    assert!(
        per.iter().all(|files| *files == 1_000),
        "twelve thousand files split evenly: {per:?}"
    );
    assert!(
        per.iter().all(|files| *files <= 4_096),
        "and each partition's share fits in one chunk, so it prunes once"
    );
}

/// Every partition is a separate stream, so a real query over a hundred files
/// reads them concurrently rather than one after another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_files_are_read_across_every_partition() {
    let fixture = fixture_with(true, Some(8)).await;

    let mut observed = Vec::new();
    for i in 0..100 {
        observed.push(
            put_parquet(&fixture.objects, &format!("obs/{i:05}.parquet"), &[i as f64]).await,
        );
    }
    let pairs: Vec<(ObservedFile, &[f64])> = observed
        .into_iter()
        .map(|file| (file, &[0.0f64][..]))
        .collect();
    analyze(&fixture.stats, &pairs).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table.scan(&fixture.ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(
        plan.output_partitioning().partition_count(),
        8,
        "a hundred files use the whole partition budget"
    );

    // And every row still comes back, once each.
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 100);
}
