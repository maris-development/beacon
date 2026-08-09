//! End-to-end tests for planning a scan's file list from the file-statistics
//! registry.
//!
//! Real Parquet objects in an in-memory store, a real registry, a real
//! `ListingTable` — the only thing simulated is time. Each test asserts on the
//! shape of the *plan* (which files it lists) and, where it matters, on the
//! rows the plan produces.

use std::sync::Arc;

use beacon_datafusion_ext::registry_listing::{RegistryListingSwitch, try_scan_from_registry};
use beacon_datafusion_ext::registry_source::{Partitions, RegistryScanSource};
use beacon_file_stats::segment::{ColumnStat, SegmentBuilder};
use beacon_file_stats::{FileStatsStore, ObservedFile, Registry, StatScalar};
use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::catalog::TableProvider;
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

/// A session with the registry-listing switch on, a filled statistics store,
/// and an in-memory object store at `test://stats/`.
async fn fixture(switch_on: bool) -> Fixture {
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
    let config = SessionConfig::new()
        .with_extension(handle)
        .with_extension(Arc::new(RegistryListingSwitch { enable: switch_on }));
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

/// Register `files` and record each as analyzed with its row count and its
/// `v` range, the way a collector pass would have.
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

/// A `ListingTable` over `url`, its schema inferred the way `read_parquet`
/// infers it.
async fn listing_table(ctx: &SessionContext, url: &str) -> ListingTable {
    let url = ListingTableUrl::parse(url).unwrap();
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("")
        .with_collect_stat(true);
    let schema = options.infer_schema(&ctx.state(), &url).await.unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ListingTable::try_new(config).unwrap()
}

/// The registry source under a plan.
fn scan_source(plan: &Arc<dyn ExecutionPlan>) -> &RegistryScanSource {
    let mut node: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            return exec
                .data_source()
                .as_any()
                .downcast_ref::<RegistryScanSource>()
                .expect("a registry-planned scan carries a RegistryScanSource");
        }
        let children = node.children();
        assert_eq!(children.len(), 1, "expected a single-child chain to the scan");
        node = children[0].as_ref();
    }
}

/// The paths the plan's scan will read, in order.
///
/// The plan holds a *query*, not a file list — path ranges in streaming mode,
/// surviving ids in pruned mode — so the paths are resolved the way the scan
/// itself resolves them at execute time: by walking the same snapshot.
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let source = scan_source(plan);
    let snapshot = source.snapshot();
    let mut paths = Vec::new();
    match source.partitions() {
        Partitions::Streaming {
            shards,
            urls,
            ignore_subdirectory,
            extension,
        } => {
            for query in shards.iter() {
                snapshot
                    .for_each_in_shard(&query.prefix, &query.shard, |_, record| {
                        let location =
                            object_store::path::Path::parse(&record.path).unwrap();
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
        Partitions::Ids { ids, .. } => {
            paths.extend(
                snapshot
                    .records_for_ids(ids)
                    .unwrap()
                    .into_iter()
                    .map(|record| record.expect("a planned id resolves").path),
            );
        }
    }
    paths
}

/// Whether the plan streams its file list or carries pruned ids.
fn is_streaming(plan: &Arc<dyn ExecutionPlan>) -> bool {
    matches!(scan_source(plan).partitions(), Partitions::Streaming { .. })
}

/// The registry serves the file list, the plan carries the counters, and the
/// rows still come out.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_plans_its_file_list_from_the_registry() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])]).await;

    let table = listing_table(&fixture.ctx, "test://stats/obs/").await;
    let state = fixture.ctx.state();
    let plan = try_scan_from_registry(&state, &table, None, &[], None)
        .await
        .expect("the registry covers this prefix");

    assert!(
        is_streaming(&plan),
        "with no predicate the plan holds path ranges, not a file list"
    );
    assert_eq!(
        planned_files(&plan),
        vec!["obs/a.parquet", "obs/b.parquet"],
        "and walking those ranges yields the registry's files, in path order"
    );

    // The counters are the plan-time evidence `EXPLAIN ANALYZE` reports.
    let metrics = plan.metrics().expect("the scan carries metrics");
    let listed = metrics
        .sum_by_name("file_stats_files_listed")
        .expect("the listed counter is registered");
    assert_eq!(listed.as_usize(), 2);

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5, "both files' rows come out of the registry-planned scan");
}

/// A `WHERE` on a recorded column builds a file list sized to the survivors.
#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_prunes_before_the_file_list_is_built() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])]).await;

    let table = listing_table(&fixture.ctx, "test://stats/obs/").await;
    let state = fixture.ctx.state();
    let filters = vec![col("v").gt(lit(50.0))];
    let plan = try_scan_from_registry(&state, &table, None, &filters, None)
        .await
        .expect("the registry covers this prefix");

    assert!(
        !is_streaming(&plan),
        "a prunable predicate names its survivors"
    );
    assert_eq!(
        planned_files(&plan),
        vec!["obs/b.parquet"],
        "the file whose range cannot match never enters the list"
    );
    let metrics = plan.metrics().unwrap();
    assert_eq!(
        metrics.sum_by_name("file_stats_files_pruned").unwrap().as_usize(),
        1
    );

    // A predicate no statistics can answer keeps every file: fail open.
    let filters = vec![col("v").eq(col("v"))];
    let plan = try_scan_from_registry(&state, &table, None, &filters, None)
        .await
        .expect("an unprunable predicate still plans from the registry");
    assert_eq!(planned_files(&plan).len(), 2);
}

/// A predicate over a column the registry has never interned must not trigger
/// an enumeration: pruning could not drop a file, so the scan streams.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_predicate_column_never_costs_an_enumeration() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;
    // Registered and analyzed, but the registry only ever interned "v".
    analyze(&fixture.stats, &[(a, &[1.0])]).await;

    let url = ListingTableUrl::parse("test://stats/obs/").unwrap();
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("")
        .with_collect_stat(true);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Float64, false),
        Field::new("unseen", DataType::Float64, true),
    ]));
    let table = ListingTable::try_new(
        ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema),
    )
    .unwrap();

    let state = fixture.ctx.state();
    let filters = vec![col("unseen").gt(lit(1.0))];
    let plan = try_scan_from_registry(&state, &table, None, &filters, None)
        .await
        .expect("the registry covers this prefix");
    assert!(
        is_streaming(&plan),
        "a predicate on an uninterned column must not make the plan enumerate"
    );
}

/// The fallbacks: switch off, unknown prefix, and a tombstoned file.
#[tokio::test(flavor = "multi_thread")]
async fn the_listing_path_keeps_everything_the_registry_cannot_answer() {
    let on = fixture(true).await;
    let a = put_parquet(&on.objects, "obs/a.parquet", &[1.0]).await;
    let b = put_parquet(&on.objects, "obs/b.parquet", &[2.0]).await;
    put_parquet(&on.objects, "fresh/c.parquet", &[3.0]).await;
    analyze(&on.stats, &[(a, &[1.0]), (b.clone(), &[2.0])]).await;

    let state = on.ctx.state();

    // A prefix discovery has never seen: only the store can say whether it is
    // empty, so the registry declines and the listing path reads the file.
    let table = listing_table(&on.ctx, "test://stats/fresh/").await;
    assert!(
        try_scan_from_registry(&state, &table, None, &[], None)
            .await
            .is_none(),
        "an undiscovered prefix falls back to the listing"
    );

    // A tombstoned file drops out of the registry's list at once.
    on.stats
        .registry()
        .reconcile_prefix("obs/", std::slice::from_ref(&b))
        .unwrap();
    let table = listing_table(&on.ctx, "test://stats/obs/").await;
    let plan = try_scan_from_registry(&state, &table, None, &[], None)
        .await
        .expect("the surviving file still plans from the registry");
    assert_eq!(planned_files(&plan), vec!["obs/b.parquet"]);

    // The switch is the operator's: off means the listing path, always.
    let fixture_off = fixture(false).await;
    let a = put_parquet(&fixture_off.objects, "obs/a.parquet", &[1.0]).await;
    analyze(&fixture_off.stats, &[(a, &[1.0])]).await;
    let table = listing_table(&fixture_off.ctx, "test://stats/obs/").await;
    assert!(
        try_scan_from_registry(&fixture_off.ctx.state(), &table, None, &[], None)
            .await
            .is_none()
    );
}

/// A limit stops the reading rather than shortening a list.
///
/// The listing path cuts its file list at plan time using recorded row counts,
/// which needs the counts of every file. A streaming partition instead stops
/// walking once it has the rows, which is the same answer without the pass —
/// and it works when the row counts are unknown, where the plan-time cut
/// cannot.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_stops_the_walk_rather_than_the_plan() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[4.0, 5.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[4.0, 5.0])]).await;

    let table = listing_table(&fixture.ctx, "test://stats/obs/").await;
    let state = fixture.ctx.state();
    let plan = try_scan_from_registry(&state, &table, None, &[], Some(2))
        .await
        .expect("the registry covers this prefix");

    // The plan still describes the whole prefix; the limit lives in the scan.
    assert!(is_streaming(&plan));
    assert_eq!(planned_files(&plan).len(), 2);

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "the walk stopped at the limit");
}

/// A single-file URL the registry knows plans from it; one it has never seen
/// falls back.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_file_url_is_answered_only_when_known() {
    let fixture = fixture(true).await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;
    put_parquet(&fixture.objects, "obs/unseen.parquet", &[3.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0])]).await;

    let state = fixture.ctx.state();

    let table = listing_table(&fixture.ctx, "test://stats/obs/a.parquet").await;
    let plan = try_scan_from_registry(&state, &table, None, &[], None)
        .await
        .expect("a registered single file plans from the registry");
    assert_eq!(planned_files(&plan), vec!["obs/a.parquet"]);
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    let table = listing_table(&fixture.ctx, "test://stats/obs/unseen.parquet").await;
    assert!(
        try_scan_from_registry(&state, &table, None, &[], None)
            .await
            .is_none(),
        "a file the registry has never seen goes to the listing path, so it is still read"
    );
}

/// The scaling claim, asserted rather than argued: a streaming plan's memory
/// is its shards, not its files.
///
/// One shard is two path bounds and two counts — call it ~100 bytes with the
/// paths. A `PartitionedFile` is ~280 bytes plus a path, so the listing path
/// over these files would hold hundreds of times more, and the ratio grows
/// with the collection while this stays flat.
#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_plan_holds_shards_not_files() {
    let fixture = fixture(true).await;

    // Registered, not written: this is about what the *plan* holds, and no
    // file is opened at plan time.
    let files: Vec<ObservedFile> = (0..5_000)
        .map(|i| ObservedFile::new(format!("obs/{i:05}.parquet"), 1_000, 1))
        .collect();
    fixture.stats.registry().intern_files(&files).unwrap();

    // A table over one real file, so the schema infers; the shards then cover
    // every registered path under the prefix.
    let a = put_parquet(&fixture.objects, "obs/00000.parquet", &[1.0]).await;
    fixture.stats.registry().intern_files(&[a]).unwrap();

    let table = listing_table(&fixture.ctx, "test://stats/obs/00000.parquet").await;
    let url = ListingTableUrl::parse("test://stats/obs/").unwrap();
    let table = ListingTable::try_new(
        ListingTableConfig::new(url)
            .with_listing_options(
                ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension("")
                    .with_collect_stat(true)
                    .with_target_partitions(8),
            )
            .with_schema(table.schema()),
    )
    .unwrap();

    let state = fixture.ctx.state();
    let plan = try_scan_from_registry(&state, &table, None, &[], None)
        .await
        .expect("the registry covers this prefix");

    let source = scan_source(&plan);
    let Partitions::Streaming { shards, .. } = source.partitions() else {
        panic!("a scan with no predicate must stream");
    };
    assert!(
        shards.len() <= 8,
        "5000 files became {} partitions, not one per file",
        shards.len()
    );
    // The whole plan-time file state: eight shards.
    assert_eq!(
        plan.output_partitioning().partition_count(),
        shards.len(),
        "the partition count is chosen, not derived from a file list"
    );
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

    let urls = vec![
        ListingTableUrl::parse("test://stats/obs/argo/").unwrap(),
        ListingTableUrl::parse("test://stats/obs/ctd/").unwrap(),
    ];
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("")
        .with_collect_stat(true);
    let schema = options
        .infer_schema(&fixture.ctx.state(), &urls[0])
        .await
        .unwrap();
    let table = ListingTable::try_new(
        ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(schema),
    )
    .unwrap();

    let state = fixture.ctx.state();
    let plan = try_scan_from_registry(&state, &table, None, &[], None)
        .await
        .expect("the registry covers both prefixes");

    assert_eq!(
        planned_files(&plan),
        vec!["obs/argo/a.parquet", "obs/ctd/b.parquet"],
        "each table path contributes its own files, and no others"
    );

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "one row per file, read once each");
}
