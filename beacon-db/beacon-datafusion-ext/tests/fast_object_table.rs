//! End-to-end tests for [`FastObjectTable`] and its [`FastObjectDataSource`].
//!
//! Real Parquet objects in an in-memory store, with a real file-statistics
//! store behind the pruning. Each test asserts on the shape of the plan and,
//! where it matters, on the rows the plan produces.

use std::sync::Arc;

use beacon_datafusion_ext::fast_object_data_source::{FastObjectDataSource, Partition};
use beacon_datafusion_ext::fast_object_table::FastObjectTable;
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
    let mut config = SessionConfig::new().with_extension(handle);
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

/// The paths the plan's scan will consider, in order.
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    scan_source(plan)
        .objects()
        .iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// A scan lists the store once and reads it. The plan holds the listing's own
/// metadata, and no predicate means no pruning to set up.
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
    assert!(!scan_source(&plan).prunes());

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5);
}

/// A `WHERE` on a recorded column is applied while the scan reads: the plan
/// still covers every file, and the ones that cannot match are dropped — and
/// counted — during execution.
#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_prunes_inside_the_stream() {
    let fixture = fixture().await;
    let a = put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    let b = put_parquet(&fixture.objects, "obs/b.parquet", &[100.0, 200.0]).await;
    analyze(&fixture.stats, &[(a, &[1.0, 2.0, 3.0]), (b, &[100.0, 200.0])]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let filters = vec![col("v").gt(lit(50.0))];
    let plan = table
        .scan(&fixture.ctx.state(), None, &filters, None)
        .await
        .unwrap();

    assert!(scan_source(&plan).prunes(), "the scan carries the predicate");
    assert_eq!(
        planned_files(&plan).len(),
        2,
        "planning is never blocked by pruning, so both files are still listed"
    );
    let metrics = plan.metrics().unwrap();
    assert!(
        metrics.sum_by_name("file_stats_files_pruned").is_none(),
        "nothing is pruned at plan time"
    );

    let batches = collect(Arc::clone(&plan), fixture.ctx.task_ctx())
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "only the file that can match was read");

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

/// No predicate means no pruning is set up, and a file with no statistics is
/// never dropped by one that is.
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

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1, "the unanalyzed file survives and is read");
}

/// A limit stops the reading rather than shortening the listing.
///
/// One partition, because a scan applies its limit per partition — the limit
/// operator above trims the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_stops_the_reading() {
    let fixture = fixture_with(Some(1)).await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0, 3.0]).await;
    put_parquet(&fixture.objects, "obs/b.parquet", &[4.0, 5.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], Some(2))
        .await
        .unwrap();

    assert_eq!(planned_files(&plan).len(), 2, "the plan lists both");
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "the reading stopped at the limit");
}

/// A single-file URL is read like any other.
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
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
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
        vec!["obs/argo/a.parquet", "obs/ctd/b.parquet"],
        "each table path contributes its own files, and no others"
    );
    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
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

/// A hundred files fill the partition budget and are read concurrently, once
/// each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_files_are_read_across_every_partition() {
    let fixture = fixture_with(Some(8)).await;
    for i in 0..100 {
        put_parquet(&fixture.objects, &format!("obs/{i:05}.parquet"), &[i as f64]).await;
    }

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(
        plan.output_partitioning().partition_count(),
        8,
        "a hundred files use the whole partition budget"
    );

    let batches = collect(plan, fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 100);
}

/// One large Parquet file is split across partitions, so a single big file
/// still fills the machine.
///
/// `ListingTable` does this through `FileGroupPartitioner`; a source that
/// declined all repartitioning would scan a 500 MB file on one thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_large_parquet_file_is_split_across_partitions() {
    let fixture = fixture_with(Some(4)).await;
    // Comfortably past the 10 MB `repartition_file_min_size` default, and
    // enough row groups to divide.
    let values: Vec<f64> = (0..2_000_000).map(|i| i as f64).collect();
    put_parquet(&fixture.objects, "obs/big.parquet", &values).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();

    // One file, so the scan starts with one partition...
    assert_eq!(plan.output_partitioning().partition_count(), 1);

    // ...and repartitioning splits it into byte ranges over the same listing.
    let source = scan_source(&plan);
    let split = datafusion::datasource::source::DataSource::repartitioned(
        source,
        4,
        10 * 1024 * 1024,
        None,
    )
    .unwrap()
    .expect("a large parquet file can be split");
    assert!(
        split.output_partitioning().partition_count() > 1,
        "a big file must reach more than one partition"
    );

    let split = split
        .as_any()
        .downcast_ref::<FastObjectDataSource>()
        .expect("the split source is still ours");
    assert_eq!(
        split.objects().len(),
        1,
        "the listing is shared, not duplicated per part"
    );
    assert!(
        split
            .partitions()
            .iter()
            .all(|p| matches!(p, Partition::Parts(parts) if !parts.is_empty())),
        "every partition reads a part of the file"
    );

    // And the rows are neither lost nor duplicated by the split: every
    // partition is read and the total is exact.
    let exec = DataSourceExec::new(
        datafusion::datasource::source::DataSource::repartitioned(
            scan_source(&plan),
            4,
            10 * 1024 * 1024,
            None,
        )
        .unwrap()
        .unwrap(),
    );
    let batches = collect(Arc::new(exec), fixture.ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2_000_000, "every row read exactly once");
}

/// A format that declines splitting is never split, however large its files.
///
/// netCDF, HDF5, ODV and TIFF all return `Ok(None)` from
/// `FileSource::repartitioned` because their readers ignore a byte range —
/// splitting one would have every partition read the whole file and return its
/// rows again. Note that `supports_repartitioning()` is *not* that answer: it
/// defaults to true, including for those formats, so the decision has to be
/// delegated to the format itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_format_that_declines_splitting_is_never_split() {
    use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};

    /// A `FileSource` that answers like netCDF's: never split me.
    #[derive(Clone)]
    struct Undividable(Arc<dyn FileSource>);

    impl std::fmt::Debug for Undividable {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Undividable")
        }
    }

    impl FileSource for Undividable {
        fn create_file_opener(
            &self,
            store: Arc<dyn ObjectStore>,
            config: &FileScanConfig,
            partition: usize,
        ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::physical_plan::FileOpener>>
        {
            self.0.create_file_opener(store, config, partition)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn table_schema(&self) -> &datafusion::datasource::table_schema::TableSchema {
            self.0.table_schema()
        }
        fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(Undividable(self.0.with_batch_size(batch_size)))
        }
        fn metrics(&self) -> &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet {
            self.0.metrics()
        }
        fn file_type(&self) -> &str {
            "undividable"
        }
        // The override every nd format carries.
        fn repartitioned(
            &self,
            _target_partitions: usize,
            _repartition_file_min_size: usize,
            _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
            _config: &FileScanConfig,
        ) -> datafusion::error::Result<Option<FileScanConfig>> {
            Ok(None)
        }
    }

    let fixture = fixture_with(Some(4)).await;
    let values: Vec<f64> = (0..2_000_000).map(|i| i as f64).collect();
    let meta = {
        put_parquet(&fixture.objects, "obs/big.parquet", &values).await;
        fixture
            .objects
            .head(&Path::from("obs/big.parquet"))
            .await
            .unwrap()
    };
    assert!(
        meta.size > 10 * 1024 * 1024,
        "the file must be past the split threshold to make this meaningful"
    );

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    let source = scan_source(&plan);

    // The same listing, behind a source that declines to be divided.
    let undividable = FastObjectDataSource::new(
        Arc::new(Undividable(Arc::clone(source.file_source()))),
        ObjectStoreUrl::parse(STORE_URL).unwrap(),
        plan.schema(),
        Arc::clone(source.objects()),
        Arc::new(source.partitions().to_vec()),
        None,
        None,
        datafusion::common::Statistics::new_unknown(&plan.schema()),
    );

    assert!(
        datafusion::datasource::source::DataSource::repartitioned(
            &undividable,
            4,
            10 * 1024 * 1024,
            None,
        )
        .unwrap()
        .is_none(),
        "a format that declines splitting must keep its one partition"
    );
}
