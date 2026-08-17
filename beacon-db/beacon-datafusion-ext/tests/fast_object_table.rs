//! End-to-end tests for [`FastObjectTable`].
//!
//! Real Parquet objects in an in-memory store, with a real file-statistics
//! store behind the pruning. Pruning happens before the scan is built, so the
//! plan's own file groups are what the scan will read — which is what most of
//! these assert on, alongside the rows that come out.

use std::sync::Arc;

use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_file_stats::segment::{ColumnStat, SegmentBuilder};
use beacon_file_stats::{FileStatsStore, ObservedFile, Registry, StatScalar};
use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::FileScanConfig;
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

/// A session with a file-statistics store and an in-memory object store at
/// `test://stats/`.
async fn fixture() -> Fixture {
    fixture_with(None).await
}

/// The same, with a fixed partition target where a test needs one.
async fn fixture_with(target_partitions: Option<usize>) -> Fixture {
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
        // `FastObjectTable` merges its schemas through this. `RuntimeBuilder`
        // registers it for a server; a session built here registers it itself.
        .with_extension(ArrowTypeWidening::default_extension());
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

/// Register `files` and record each as analyzed with its `v` range, the way a
/// collector pass would have.
async fn analyze(stats: &FileStatsStore, files: &[(ObservedFile, &[f64])]) {
    let observed: Vec<ObservedFile> = files.iter().map(|(file, _)| file.clone()).collect();
    let ids = stats.registry().intern_files(&observed).unwrap();
    let column = stats.registry().intern_columns(&["v"]).unwrap()[0];

    let mut builder = SegmentBuilder::new();
    for (id, (file, values)) in ids.iter().zip(files) {
        stats
            .registry()
            .mark_analyzed(
                *id,
                "parquet",
                Some(values.len() as u64),
                Some(file.size),
                1,
            )
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

/// The file-scan configuration under a plan.
///
/// The scan is DataFusion's own now, so this is its `FileScanConfig` — found by
/// descending the single-child chain, because an nd format stacks decode and
/// broadcast nodes over it.
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

/// The files the plan will read, in order.
///
/// Pruning ran before the config was built, so this is the survivors — not the
/// listing.
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut paths: Vec<String> = scan_config(plan)
        .file_groups
        .iter()
        .flat_map(|group| group.iter().map(|f| f.object_meta.location.to_string()))
        .collect();
    paths.sort();
    paths
}

fn counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<usize> {
    plan.metrics()?
        .sum_by_name(name)
        .map(|value| value.as_usize())
}

async fn rows(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> usize {
    collect(plan, ctx.task_ctx())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// With no predicate the scan reads the whole listing.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_reads_the_listing() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();

    assert_eq!(
        planned_files(&plan),
        vec!["obs/a.parquet", "obs/b.parquet"],
        "the listing, in path order"
    );
    assert_eq!(
        counter(&plan, "file_stats_files_considered"),
        None,
        "no predicate means no pruning to report"
    );
    assert_eq!(rows(plan, &fixture.ctx).await, 5);
}

/// The point of the refactor: a `WHERE` on a recorded column drops files before
/// the scan is built, so the plan itself carries only the survivors.
#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_prunes_before_the_scan_is_built() {
    let fixture = fixture().await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(
        &fixture.stats,
        &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])],
    )
    .await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(50.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert_eq!(
        planned_files(&plan),
        vec!["obs/b.parquet"],
        "the file that cannot match never reaches the scan"
    );
    assert_eq!(counter(&plan, "file_stats_files_considered"), Some(2));
    assert_eq!(counter(&plan, "file_stats_files_pruned"), Some(1));
    assert_eq!(rows(plan, &fixture.ctx).await, 2);
}

/// Every file ruled out leaves nothing to scan.
#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_that_rules_out_everything_scans_nothing() {
    let fixture = fixture().await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(500.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert_eq!(rows(plan, &fixture.ctx).await, 0);
}

/// A file the collector has never seen has no statistics, so nothing may rule
/// it out.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_without_statistics_is_never_dropped() {
    let fixture = fixture().await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0])]).await;
    // Written after the pass, so it is in no segment.
    put_parquet(&fixture.objects, "obs/fresh.parquet", &[999.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(500.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert_eq!(
        planned_files(&plan),
        vec!["obs/fresh.parquet"],
        "the analyzed file is ruled out, the unanalyzed one survives"
    );
    assert_eq!(rows(plan, &fixture.ctx).await, 1);
}

/// The scan is DataFusion's own, so everything it does to one — repartitioning
/// a large file by row group, pushing a filter into the reader — still applies.
#[tokio::test(flavor = "multi_thread")]
async fn the_scan_is_a_file_scan_config() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();

    let config = scan_config(&plan);
    assert_eq!(config.file_source.file_type(), "parquet");
    assert!(
        datafusion::datasource::source::DataSource::repartitioned(config, 4, 1, None)
            .unwrap()
            .is_some()
            || config.file_groups.len() == 1,
        "the config is the one DataFusion knows how to divide"
    );
}

/// A limit reaches the scan.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_reaches_the_scan() {
    let fixture = fixture_with(Some(1)).await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    put_parquet(&fixture.objects, "obs/b.parquet", &[4.0, 5.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], Some(2))
        .await
        .unwrap();

    assert_eq!(scan_config(&plan).limit, Some(2));
    assert_eq!(rows(plan, &fixture.ctx).await, 2);
}

/// A single-file URL reads that file.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_file_url_is_read() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;
    put_parquet(&fixture.objects, "obs/other.parquet", &[3.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/a.parquet"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(planned_files(&plan), vec!["obs/a.parquet"]);
    assert_eq!(rows(plan, &fixture.ctx).await, 2);
}

/// A table over several paths reads every path's files, once each, and nothing
/// that merely shares a prefix with them.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_over_several_paths_reads_each_file_once() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/argo/a.parquet", &[1.0]).await;
    put_parquet(&fixture.objects, "obs/ctd/b.parquet", &[2.0]).await;
    put_parquet(&fixture.objects, "obs/other/c.parquet", &[3.0]).await;

    let table = table(
        &fixture.ctx,
        &["test://stats/obs/argo/", "test://stats/obs/ctd/"],
    )
    .await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();

    assert_eq!(
        planned_files(&plan),
        vec!["obs/argo/a.parquet", "obs/ctd/b.parquet"]
    );
    assert_eq!(rows(plan, &fixture.ctx).await, 2);
}

/// A projection reaches the file reader, so a narrow `SELECT` reads narrow.
#[tokio::test(flavor = "multi_thread")]
async fn a_projection_reaches_the_reader() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;

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

/// A hundred files are grouped across the partition budget, and every row comes
/// back once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_files_fill_the_partition_budget() {
    let fixture = fixture_with(Some(8)).await;
    for index in 0..100 {
        put_parquet(
            &fixture.objects,
            &format!("obs/{index:05}.parquet"),
            &[index as f64],
        )
        .await;
    }

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();

    assert_eq!(
        plan.output_partitioning().partition_count(),
        8,
        "the listing table's grouping fills the budget"
    );
    assert_eq!(rows(plan, &fixture.ctx).await, 100);
}

/// Pruning keeps exactly the matching files across many of them, and the plan
/// carries only those.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruning_keeps_exactly_the_matching_files() {
    let fixture = fixture_with(Some(4)).await;

    let mut written = Vec::new();
    for index in 0..200 {
        let value = if index % 10 == 0 { 900.0 } else { index as f64 };
        written.push((
            put_parquet(
                &fixture.objects,
                &format!("obs/{index:05}.parquet"),
                &[value],
            )
            .await,
            value,
        ));
    }
    let analyzed: Vec<(ObservedFile, &[f64])> = written
        .iter()
        .map(|(file, value)| {
            (
                file.clone(),
                if *value >= 900.0 {
                    &[900.0f64][..]
                } else {
                    &[0.0f64][..]
                },
            )
        })
        .collect();
    analyze(&fixture.stats, &analyzed).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(500.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert_eq!(planned_files(&plan).len(), 20, "a tenth of the files match");
    assert_eq!(counter(&plan, "file_stats_files_considered"), Some(200));
    assert_eq!(counter(&plan, "file_stats_files_pruned"), Some(180));
    assert_eq!(rows(plan, &fixture.ctx).await, 20);
}

/// Write one Parquet object with an arbitrary schema, for the tests that care
/// about columns rather than values.
async fn put_typed(
    objects: &InMemory,
    path: &str,
    schema: SchemaRef,
    columns: Vec<datafusion::arrow::array::ArrayRef>,
) {
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    objects.put(&Path::from(path), bytes.into()).await.unwrap();
}

/// The JSON query API names its columns up front, and narrowing the schema
/// before planning keeps a wide collection from carrying columns nobody asked
/// for.
#[tokio::test(flavor = "multi_thread")]
async fn a_projection_narrows_the_schema() {
    let fixture = fixture().await;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Float64, false),
    ]));
    put_typed(
        &fixture.objects,
        "obs/wide.parquet",
        Arc::clone(&schema),
        vec![
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(Float64Array::from(vec![3.0])),
        ],
    )
    .await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    assert_eq!(table.schema().fields().len(), 3);

    let narrowed = table
        .with_pushdown_projection(vec!["a".to_string(), "c".to_string()])
        .unwrap();
    let narrowed_schema = narrowed.schema();
    let names: Vec<&str> = narrowed_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["a", "c"], "only the named columns survive");

    // And it still reads.
    let plan = narrowed
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(rows(plan, &fixture.ctx).await, 1);
}

/// Two ways a projection asks for nothing useful, both of which leave the table
/// readable rather than producing one with no columns.
#[tokio::test(flavor = "multi_thread")]
async fn a_projection_that_names_nothing_leaves_the_schema_alone() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;
    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;

    // Empty: the caller asked for no projection at all.
    let empty = table.with_pushdown_projection(vec![]).unwrap();
    assert_eq!(empty.schema().fields().len(), 1);

    // Named, but nothing matches. Narrowing to zero columns would make the
    // table unreadable, so the schema is kept instead.
    let unknown = table
        .with_pushdown_projection(vec!["nope".to_string()])
        .unwrap();
    assert_eq!(unknown.schema().fields().len(), 1);
}

/// The merge rule is the caller's to choose. The session's rule refuses a column
/// two URLs describe differently; a caller that wants something else names its own
/// strategy.
///
/// Across URLs, note, not within one: a format merges the files behind a single
/// URL itself. It merges them through the same session rule, so a `read_*` over
/// files that disagree behaves the same however the URLs are spelled.
#[tokio::test(flavor = "multi_thread")]
async fn the_caller_can_name_the_merge_rule() {
    use beacon_datafusion_ext::type_widening::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening};
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::SchemaRef;

    /// A rule of its own: keep the first type seen for a column instead of
    /// refusing the second. Order-sensitive by construction, so it says so and
    /// the merge folds it over every schema in order.
    struct FirstTypeWins;

    impl ArrowTypeWideningStrategy for FirstTypeWins {
        fn merge_schemas(
            &self,
            schema_refs: &[SchemaRef],
        ) -> Result<SchemaRef, datafusion::arrow::error::ArrowError> {
            let mut fields: Vec<datafusion::arrow::datatypes::FieldRef> = Vec::new();
            for schema in schema_refs {
                for field in schema.fields() {
                    if !fields.iter().any(|kept| kept.name() == field.name()) {
                        fields.push(field.clone());
                    }
                }
            }
            Ok(Arc::new(Schema::new(fields)))
        }

        fn is_order_independent(&self) -> bool {
            false
        }
    }

    let fixture = fixture().await;
    put_typed(
        &fixture.objects,
        "obs/small.parquet",
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await;
    put_typed(
        &fixture.objects,
        "obs/big.parquet",
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .await;

    // One URL each, so the conflict reaches the strategy rather than being
    // settled inside the format's own merge.
    let urls = vec![
        ListingTableUrl::parse("test://stats/obs/small.parquet").unwrap(),
        ListingTableUrl::parse("test://stats/obs/big.parquet").unwrap(),
    ];
    let state = fixture.ctx.state();

    // The session's rule has no answer for a column with two types, and neither
    // does naming that same rule explicitly.
    assert!(
        FastObjectTable::try_new(&state, Arc::new(ParquetFormat::default()), urls.clone())
            .await
            .is_err(),
        "the session's rule refuses a column with two types"
    );
    assert!(
        FastObjectTable::try_new_with_widening(
            &state,
            Arc::new(ParquetFormat::default()),
            urls.clone(),
            &DefaultArrowTypeWidening,
        )
        .await
        .is_err()
    );

    // A caller that names its own rule gets it: this one keeps the first type it
    // saw, so the table reports the Int32 the first URL declared.
    let first_wins = FastObjectTable::try_new_with_widening(
        &state,
        Arc::new(ParquetFormat::default()),
        urls,
        &FirstTypeWins,
    )
    .await
    .unwrap();
    assert_eq!(
        first_wins.schema().field_with_name("v").unwrap().data_type(),
        &DataType::Int32
    );
}

/// Everything the wrapped listing table declares reaches the caller through the
/// wrapper, so a table built by `CREATE EXTERNAL TABLE` keeps what its DDL said.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrapped_listing_table_keeps_what_it_declared() {
    use datafusion::common::Constraints;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};

    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;

    let options = ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension("");
    let url = ListingTableUrl::parse("test://stats/obs/").unwrap();
    let schema = options
        .infer_schema(&fixture.ctx.state(), &url)
        .await
        .unwrap();
    let inner = ListingTable::try_new(
        ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema),
    )
    .unwrap()
    .with_definition(Some("CREATE EXTERNAL TABLE obs ...".to_string()))
    .with_constraints(Constraints::default());

    let table = FastObjectTable::from_listing_table(inner);

    assert_eq!(
        table.get_table_definition(),
        Some("CREATE EXTERNAL TABLE obs ..."),
        "the definition passes through"
    );
    assert!(
        table.get_logical_plan().is_none(),
        "a listing table has none"
    );
    assert!(table.get_column_default("v").is_none());
    assert_eq!(table.schema().fields().len(), 1);

    // And the wrapper still scans.
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(rows(plan, &fixture.ctx).await, 1);
}
