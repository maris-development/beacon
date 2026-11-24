//! End-to-end tests of the NetCDF scan: SQL over a session, from the table to the rows.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload};

use crate::datafusion::options::NetcdfOptions;
use crate::datafusion::test_util::*;
use crate::datafusion::NetcdfFormat;

/// The scan at the bottom of `plan`.
fn leaf_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut node = Arc::clone(plan);
    while let Some(child) = node.children().first() {
        node = Arc::clone(child);
    }
    node
}

/// The partition count of the scan at the bottom of `plan`.
///
/// DataFusion can add a round-robin repartition above a single-partition scan,
/// so the root does not show whether the scan itself was split.
fn scan_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    leaf_scan(plan).output_partitioning().partition_count()
}

/// The sum of the scan counter `name` over all partitions of the leaf scan.
fn scan_counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    leaf_scan(plan)
        .metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |value| value.as_usize())
}

async fn error_of(ctx: &SessionContext, sql: &str) -> DataFusionError {
    match ctx.sql(sql).await {
        Err(e) => e,
        Ok(frame) => frame.collect().await.expect_err("the query fails"),
    }
}

/// The rows that `SELECT *` gives, and the `count("V0")`, for each `target_partitions`.
///
/// When the file-group count differs from `target_partitions`, DataFusion adds
/// a `RepartitionExec(RoundRobinBatch)` that merges small batches. Each encoded
/// batch is one row that carries one file, so a decoder that read only row 0
/// lost every other file of a merged batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_reads_every_file_whatever_the_target_partitions() {
    const FILES: usize = 32;
    const ROWS: usize = 4000;

    let source = tempfile::tempdir().unwrap();
    let file = write_regular_netcdf(source.path(), "source.nc", ROWS);
    let dir = tempfile::tempdir().unwrap();
    for i in 0..FILES {
        std::fs::copy(&file, dir.path().join(format!("part_{i:04}.nc"))).unwrap();
    }
    let expected = ROWS * FILES;

    // 8, 16 and 32 give one file group per partition. 9, 24 and 31 do not, so
    // a round-robin repartition merges batches. 33 is more than the files.
    for target in [1usize, 8, 9, 16, 24, 31, 32, 33] {
        let ctx = session(target, 8192);
        register(&ctx, "nc", dir.path()).await;

        let scanned: usize = ctx
            .sql("SELECT * FROM nc")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        let counted = ctx
            .sql(r#"SELECT count("V0") FROM nc"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0) as usize;

        assert_eq!(scanned, expected, "rows at target_partitions={target}");
        assert_eq!(counted, expected, "count at target_partitions={target}");
    }
}

/// The reader opens a file by its content, so an alias extension reads the same rows.
#[tokio::test]
async fn a_file_with_an_alias_extension_reads_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let canonical = write_regular_netcdf(dir.path(), "regular.nc", 4000);
    let alias = dir.path().join("regular.nc4");
    std::fs::copy(&canonical, &alias).unwrap();

    let ctx = one_partition_session();
    register(&ctx, "canonical", &canonical).await;
    register(&ctx, "alias", &alias).await;

    let count = |table: &'static str| {
        let ctx = ctx.clone();
        async move {
            let batches = ctx
                .sql(&format!(r#"SELECT COUNT("V0") AS n FROM {table}"#))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string()
        }
    };
    assert_eq!(count("alias").await, count("canonical").await);
}

/// Register `path` as `mixed`, on a format with `options`.
async fn register_mixed(
    ctx: &SessionContext,
    options: NetcdfOptions,
    path: &std::path::Path,
) -> datafusion::error::Result<()> {
    let format = NetcdfFormat::new(Arc::new(ListingFactory::dynamic()), options);
    register_with(ctx, "mixed", path, format).await
}

/// `WOD_FILE` is ragged, so no dimension list fits it. Without
/// `skip_unbroadcastable` it fails the table. With it, the file is skipped
/// and the gridded file next to it is read whole.
#[tokio::test]
async fn the_skip_option_reads_past_a_file_the_dimension_list_does_not_fit() {
    let dir = tempfile::tempdir().unwrap();
    for file in [GRIDDED_FILE, WOD_FILE] {
        std::fs::copy(test_file(file), dir.path().join(file)).unwrap();
    }
    let dims = Some(vec![
        "time".to_string(),
        "lat".to_string(),
        "lon".to_string(),
    ]);
    let ctx = one_partition_session();

    let strict = NetcdfOptions {
        read_dimensions: dims.clone(),
        ..NetcdfOptions::default()
    };
    let error = register_mixed(&ctx, strict, dir.path())
        .await
        .expect_err("the ragged file fits no list")
        .to_string();
    assert!(error.contains("ragged"), "{error}");

    let lenient = NetcdfOptions {
        read_dimensions: dims,
        skip_unbroadcastable: true,
        ..NetcdfOptions::default()
    };
    register_mixed(&ctx, lenient, dir.path()).await.unwrap();
    let batches = ctx
        .sql("SELECT lat FROM mixed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let read: usize = batches.iter().map(|batch| batch.num_rows()).sum();

    assert_eq!(
        read,
        rows_in_one_file(&ctx, "lat").await,
        "the gridded file alone"
    );
}

/// A file the reader cannot open fails the table and names the file.
/// With `skip_invalid_files`, the scan reads past it.
#[tokio::test]
async fn the_skip_option_reads_past_a_file_the_reader_cannot_open() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(test_file(GRIDDED_FILE), dir.path().join(GRIDDED_FILE)).unwrap();
    std::fs::write(dir.path().join("broken.nc"), b"not a netcdf file").unwrap();
    let ctx = one_partition_session();

    // Act
    let error = register_mixed(&ctx, NetcdfOptions::default(), dir.path())
        .await
        .expect_err("random bytes are not a NetCDF file")
        .to_string();
    let lenient = NetcdfOptions {
        skip_invalid_files: true,
        ..NetcdfOptions::default()
    };
    register_mixed(&ctx, lenient, dir.path()).await.unwrap();
    let batches = ctx
        .sql("SELECT lat FROM mixed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let read: usize = batches.iter().map(|batch| batch.num_rows()).sum();

    // Assert
    assert!(
        error.contains("broken.nc"),
        "the error must name the file: {error}"
    );
    assert_eq!(
        read,
        rows_in_one_file(&ctx, "lat").await,
        "the gridded file alone"
    );
}

/// The rows one copy of `GRIDDED_FILE` contributes to `column`.
async fn rows_in_one_file(ctx: &SessionContext, column: &str) -> usize {
    register(ctx, "one", &test_file(GRIDDED_FILE)).await;
    let batches = ctx
        .sql(&format!("SELECT {column} FROM one"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// How many columns [`write_large_netcdf`] writes. They are named `V0`..,
/// and the tests read `V0`.
const LARGE_COLUMNS: usize = 20;
/// How many rows it writes in each.
const LARGE_ROWS: usize = 100_000;

/// Write a netCDF-4 file of 20 columns and 100k rows, and return its path.
///
/// It is written **wide** rather than long, and that is not a free choice.
/// `oxcdf` cannot read this writer's output once a single variable grows
/// past roughly 200k values: it fails with "chunk at […] was neither cached
/// nor fetched" while netcdf-c reads the same bytes. The limit follows one
/// variable's chunk count, not the file's size, so 20 columns of 100k values
/// gives a file of several chunks that still reads back. Measured:
/// 20x100k is 16.3 MB and reads; 12x200k is 19.4 MB and does not.
///
/// The caller holds the [`TempDir`](tempfile::TempDir) for as long as the
/// table is registered.
fn write_large_netcdf() -> (tempfile::TempDir, PathBuf) {
    let columns = (0..LARGE_COLUMNS)
        .map(|column| {
            let values = (0..LARGE_ROWS)
                .map(|row| (row + column) as f64 * 0.25)
                .collect();
            (format!("V{column}"), values)
        })
        .collect();

    let dir = tempfile::tempdir().expect("a temp directory");
    let path = dir.path().join("large.nc");
    write_netcdf(&path, columns);
    (dir, path)
}

/// `count` netCDF files in one directory, each holding `rows` rows of a
/// single column, numbered so that no two files hold the same values.
///
/// Uneven on purpose: a scan of equal files divides the same way however it
/// is divided, and would hide a scan that read one file twice.
fn write_netcdf_collection(count: usize, rows: usize) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temp directory");
    for file in 0..count {
        // Each file is a different length and a different range of values.
        let held = rows + file % 7;
        let base = (file * 10_000) as f64;
        let values = (0..held).map(|row| base + row as f64).collect();
        let path = dir.path().join(format!("part-{file:04}.nc"));
        write_netcdf(&path, vec![("V0".to_string(), values)]);
    }

    let path = dir.path().to_path_buf();
    (dir, path)
}

/// A collection scanned on many partitions returns exactly what one
/// partition returns.
///
/// The partitions take files from one shared list as they become free, so
/// nothing at plan time says which partition reads what. Only the answer
/// says whether a file was read twice or not at all. `count(*)` catches both. `sum` catches a
/// file read from the wrong place, and `min`/`max` catch a partial read.
/// None of those raise an error on their own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_scanned_on_many_partitions_returns_the_same_rows() {
    const FILES: usize = 40;
    const ROWS: usize = 64;
    const QUERY: &str =
        r#"SELECT count(*), count("V0"), min("V0"), max("V0"), sum("V0") FROM many"#;

    let (_dir, dir) = write_netcdf_collection(FILES, ROWS);

    let whole = one_partition_session();
    register(&whole, "many", &dir).await;

    let split = session(8, 1024);
    register(&split, "many", &dir).await;

    let plan = split
        .sql(r#"SELECT "V0" FROM many"#)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(
        scan_partitions(&plan),
        8,
        "a collection scans on every partition:\n{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );

    let summary = async |ctx: &SessionContext| {
        let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
        format!("{:?}", batches[0].columns())
    };

    let whole_summary = summary(&whole).await;
    assert_eq!(
        summary(&split).await,
        whole_summary,
        "eight partitions read the collection exactly once"
    );
}

/// A collection where only some files carry the column the query selects.
///
/// Real collections are not uniform: of one CORA year, 2% of the files hold
/// no `TEMP` and 10% no `DEPH`. `SELECT "TEMP"` therefore meets files that
/// have none of what it asked for, and those must be skipped, not fail the
/// scan.
///
/// The failure this guards is not a wrong number, it is a dead query:
/// planning such a file as a `COUNT(*)` — its projection resolves empty
/// either way — built a batch of no columns against a one-field schema and
/// returned "number of columns(0) must match number of fields(1)".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_where_only_some_files_have_the_column_still_reads() {
    const ROWS: usize = 32;
    const WITH: usize = 3;
    const WITHOUT: usize = 2;

    let dir = tempfile::tempdir().expect("a temp directory");

    // Files that hold "V0", and files that hold only "OTHER".
    for (count, column, base) in [(WITH, "V0", 0.0), (WITHOUT, "OTHER", 1000.0)] {
        for file in 0..count {
            let values = (0..ROWS)
                .map(|row| base + (file * ROWS + row) as f64)
                .collect();
            let path = dir.path().join(format!("{column}-{file}.nc"));
            write_netcdf(&path, vec![(column.to_string(), values)]);
        }
    }

    let ctx = session(4, 1024);
    register(&ctx, "mixed", dir.path()).await;

    // Every file lacking "V0" is skipped; the ones holding it are read whole.
    let batches = ctx
        .sql(r#"SELECT count("V0"), min("V0"), max("V0") FROM mixed"#)
        .await
        .expect("the query plans")
        .collect()
        .await
        .expect("a file without the column does not fail the scan");

    let summary = format!("{:?}", batches[0].columns());
    assert!(
        summary.contains(&format!("{}", WITH * ROWS)),
        "every row of the {WITH} files that have the column: {summary}"
    );
    assert!(
        summary.contains(&format!("{}", (WITH * ROWS - 1) as f64)),
        "and its largest value, so nothing was truncated: {summary}"
    );
}

/// The writer gives a string variable no `_FillValue`, so a null string reads back as "".
#[tokio::test]
async fn a_null_string_reads_back_as_an_empty_string() {
    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::encoders::default::DefaultEncoder;
    use crate::writer::ArrowRecordBatchWriter;

    let dir = tempfile::tempdir().expect("a temp directory");
    let path = dir.path().join("names.nc");
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let names = StringArray::from(vec![Some("a"), None]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(names)]).unwrap();
    let mut writer =
        ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema).expect("a netCDF writer");
    writer.write_record_batch(batch).expect("write the batch");
    writer.finish().expect("finish the file");

    let ctx = one_partition_session();
    register(&ctx, "names", &path).await;
    let batches = ctx
        .sql(r#"SELECT "name" FROM names"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut read = Vec::new();
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        read.extend(
            column
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::to_string)),
        );
    }
    assert_eq!(read, [Some("a".to_string()), Some(String::new())]);
}

/// One file scans in several partitions, and returns
/// the same rows it returns in one.
///
/// The partition count is the point of the feature. The row check is the
/// guard on it: `count(*)` catches a slice that overlapped another or a gap
/// between two, and `min`/`max` catch a slice that read the wrong region.
/// None of those raise an error on their own.
///
/// This test reaches the slices the way a query does: a real file, planned
/// and executed through SQL.
#[tokio::test]
async fn a_large_file_splits_and_returns_the_same_rows() {
    const QUERY: &str = r#"SELECT count(*), count("V0"), min("V0"), max("V0") FROM one"#;

    let (_dir, file) = write_large_netcdf();

    let whole = one_partition_session();
    register(&whole, "one", &file).await;

    let split = session(4, 8192);
    register(&split, "one", &file).await;

    let plan = split
        .sql(r#"SELECT "V0" FROM one"#)
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

    let summary = async |ctx: &SessionContext| {
        let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
        format!("{:?}", batches[0].columns())
    };

    let whole_summary = summary(&whole).await;
    assert_eq!(summary(&split).await, whole_summary);
}

/// A pruned scan counts what the coordinate itself says it should.
///
/// The scan skips chunks whose coordinates cannot meet the predicate, so a
/// bound that is too tight loses rows and reports a smaller count. Nothing
/// about that raises an error, and comparing the two readers would not catch
/// it — they share the pruning. So the expected answer comes from the
/// coordinate column itself, read with no predicate in the plan and
/// therefore with no pruning.
///
/// The fixture's `lat` runs from about 38.8 to 48.8, so the thresholds below
/// prune nothing, some, and everything in turn. The middle one is the one
/// that matters: a chunk half inside the bound has to be read whole.
#[tokio::test]
async fn a_pruned_scan_counts_what_the_coordinate_says_it_should() {
    use arrow::array::{Float32Array, Int64Array};

    let ctx = one_partition_session();
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;

    // Every lat the file holds, with no predicate in the plan.
    let batches = ctx
        .sql("SELECT lat FROM gridded")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let lats: Vec<f32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("lat is f32")
                .iter()
                .flatten()
                .collect::<Vec<f32>>()
        })
        .collect();
    assert!(!lats.is_empty(), "the fixture must hold rows");

    let count = async |sql: &str| {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count")
            .value(0) as usize
    };

    for threshold in [0.0_f32, 44.0, 60.0] {
        let expected = lats.iter().filter(|lat| **lat > threshold).count();
        assert_eq!(
            count(&format!(
                "SELECT count(*) FROM gridded WHERE lat > {threshold}"
            ))
            .await,
            expected,
            "lat > {threshold}: the pruned scan does not match the coordinate"
        );
    }

    // The middle threshold has to be a partial prune, or the assertion above
    // it never exercised a chunk that straddles the bound.
    let partial = lats.iter().filter(|lat| **lat > 44.0).count();
    assert!(
        partial > 0 && partial < lats.len(),
        "44.0 must cut the fixture in two, it kept {partial} of {}",
        lats.len()
    );

    // A disjunction bounds nothing, so it must prune nothing and still
    // answer. `lat > 60` is inside `lat > 0`, so the two agree. The bounds
    // used to be intersected as if the `OR` were an `AND`, which pruned
    // every chunk under 60 — the whole file, since nothing reaches it.
    assert_eq!(
        count("SELECT count(*) FROM gridded WHERE lat > 0 OR lat > 60").await,
        lats.iter().filter(|lat| **lat > 0.0).count(),
        "a disjunction must not prune on one of its branches"
    );

    // The same for a negation, which means the opposite of its child.
    assert_eq!(
        count("SELECT count(*) FROM gridded WHERE NOT (lat <= 44)").await,
        partial,
        "a negation must not prune on the child it negates"
    );
}

/// The predicate reaches the file source through the nd pipeline.
///
/// The scan sits under an `NdSourceExec` and an `NdBroadcastExec`, and a
/// node that does not forward filters leaves the source with nothing to
/// prune on. That failure is invisible in a result — every row still comes
/// back, the scan just reads the whole file — so it is asserted on the
/// scan's own output instead.
///
/// One encoded batch carries one chunk, so the scan's output rows *are* its
/// chunk count.
#[tokio::test]
async fn a_predicate_reaches_the_scan_and_skips_its_chunks() {
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

    let ctx = one_partition_session();
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;

    /// The chunk count of the scan at the bottom of an executed plan.
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

    let whole = chunks_read("SELECT analysed_sst, lat FROM gridded").await;
    assert!(whole > 1, "the fixture must hold several chunks");

    // `lat` runs from about 38.8 to 48.8, so nothing can satisfy this and
    // every chunk is skipped before it is fetched.
    assert_eq!(
        chunks_read("SELECT analysed_sst, lat FROM gridded WHERE lat > 1000").await,
        0,
        "a predicate no row can meet must leave the scan nothing to read"
    );

    // A bound the whole file satisfies prunes nothing.
    assert_eq!(
        chunks_read("SELECT analysed_sst, lat FROM gridded WHERE lat > 0").await,
        whole,
        "a predicate every row meets must not skip a chunk"
    );

    // And one that cuts the file reads some of it.
    let partial = chunks_read("SELECT analysed_sst, lat FROM gridded WHERE lat > 44").await;
    assert!(
        partial > 0 && partial < whole,
        "lat > 44 should read some of the {whole} chunks, it read {partial}"
    );
}

/// The scan reports what each partition read, and what the predicate saved.
///
/// These four counters are the only view of the sharing there is: the scan's
/// own `output_rows` counts encoded batches, which says nothing about how a
/// file divided between the partitions or how much of it was skipped. They
/// went unrecorded once already, so this holds them in place.
#[tokio::test]
async fn the_scan_reports_what_it_read_and_what_it_skipped() {
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

    let ctx = one_partition_session();
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;

    /// The named counter, summed over the scan's partitions.
    struct Counter(&'static str, usize);
    impl ExecutionPlanVisitor for Counter {
        type Error = std::convert::Infallible;
        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.name().contains("DataSourceExec") {
                self.1 = plan
                    .metrics()
                    .map(|metrics| {
                        metrics
                            .iter()
                            .filter(|metric| metric.value().name() == self.0)
                            .map(|metric| metric.value().as_usize())
                            .sum()
                    })
                    .unwrap_or(0);
                return Ok(false);
            }
            Ok(true)
        }
    }

    let counters = async |sql: &str| {
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
        let read = |name: &'static str| {
            let mut counter = Counter(name, 0);
            datafusion::physical_plan::accept(plan.as_ref(), &mut counter).unwrap();
            counter.1
        };
        (
            read("chunks_read"),
            read("rows_read"),
            read("chunks_pruned"),
            read("rows_pruned"),
        )
    };

    // No predicate: everything is read, nothing is skipped, and the rows are
    // the grid's own — not the one-row-per-chunk the scan emits.
    let (chunks, rows, pruned, pruned_rows) =
        counters("SELECT analysed_sst, lat FROM gridded").await;
    assert!(chunks > 1, "the fixture must hold several chunks");
    assert!(
        rows > chunks,
        "rows_read must count broadcast rows ({rows}), not batches ({chunks})"
    );
    assert_eq!((pruned, pruned_rows), (0, 0), "nothing to skip");

    // A predicate nothing can meet: every chunk is skipped before it is
    // read, and the rows it would have produced are accounted for.
    let (chunks, rows, pruned, pruned_rows) =
        counters("SELECT analysed_sst, lat FROM gridded WHERE lat > 1000").await;
    assert_eq!((chunks, rows), (0, 0), "nothing is read");
    assert!(pruned > 0 && pruned_rows > 0, "and the skip is reported");
}

/// A pushed-down predicate keeps the rows that match. The query reads only
/// `lat`, so the grid is the `lat` axis: 458 of its 1208 values are above 45.
#[tokio::test]
async fn a_pushed_down_predicate_keeps_the_matching_rows() {
    let ctx = one_partition_session();
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;

    let batches = ctx
        .sql("SELECT count(*) FROM gridded WHERE lat > 45")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 458);
}

#[tokio::test]
async fn a_scan_with_no_file_column_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "data.nc", 4000);
    let ctx = session(1, 1000);
    register(&ctx, "t", &path).await;

    let error = error_of(&ctx, "SELECT count(*) FROM t").await;

    let text = error.to_string();
    assert!(
        text.contains("needs at least one file column") && text.contains("COUNT(column)"),
        "{text}"
    );

    let counted = ctx
        .sql(r#"SELECT count("V0") FROM t"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        counted[0].column(0).as_primitive::<Int64Type>().value(0),
        4000
    );
}

#[tokio::test]
async fn a_ragged_file_is_refused_at_schema_and_at_scan() {
    let ctx = session(1, 1000);
    let error = register_with(&ctx, "inferred", &test_file(WOD_FILE), format())
        .await
        .expect_err("a ragged file has no schema");
    assert!(
        matches!(error.find_root(), DataFusionError::NotImplemented(_)),
        "{error}"
    );
    let text = error.to_string();
    assert!(text.contains("ragged") && text.contains(WOD_FILE), "{text}");

    // A table with a given schema skips inference, so the scan gives the error.
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("z", arrow::datatypes::DataType::Float32, true),
    ]));
    let options = ListingOptions::new(Arc::new(format()))
        .with_file_extension("")
        .with_collect_stat(false);
    let url = ListingTableUrl::parse(test_file(WOD_FILE).to_string_lossy()).unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("ragged", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();
    let error = error_of(&ctx, "SELECT z FROM ragged").await;
    let text = error.to_string();
    assert!(text.contains("ragged") && text.contains(WOD_FILE), "{text}");
}

#[tokio::test]
async fn a_collection_with_one_ragged_file_names_that_file() {
    let dir = tempfile::tempdir().unwrap();
    write_regular_netcdf(dir.path(), "a_regular.nc", 4000);
    std::fs::copy(test_file(WOD_FILE), dir.path().join(WOD_FILE)).unwrap();
    let ctx = session(1, 1000);

    let error = register_with(&ctx, "mixed", dir.path(), format())
        .await
        .expect_err("the ragged file fails the schema");

    assert!(error.to_string().contains(WOD_FILE), "{error}");
}

#[tokio::test]
async fn one_file_on_four_partitions_returns_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "data.nc", 40_000);
    let queries = [
        r#"SELECT count("V0"), min("V0"), max("V0"), sum("V0") FROM t"#,
        r#"SELECT count("V0"), min("V0"), max("V0"), sum("V0") FROM t WHERE "V0" > 25000"#,
    ];

    for sql in queries {
        let one = session(1, 1000);
        register(&one, "t", &path).await;
        let four = session(4, 1000);
        register(&four, "t", &path).await;

        let expected = one.sql(sql).await.unwrap().collect().await.unwrap();
        let plan = four
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let got = collect(Arc::clone(&plan), four.task_ctx()).await.unwrap();

        assert_eq!(got, expected, "{sql}");
        assert_eq!(
            scan_partitions(&plan),
            4,
            "the file is cut into 4 slices: {sql}"
        );
    }
}

#[tokio::test]
async fn a_limit_reads_only_the_first_chunks() {
    let ctx = session(1, 4096);
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;
    let plan = ctx
        .sql("SELECT sea_ice_fraction FROM gridded LIMIT 5")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
    // The array is contiguous: 2 rows of `lon` per chunk at 4096, so 604 chunks.
    let read = scan_counter(&plan, "chunks_read");
    assert!(read <= 3, "read {read} of 604 chunks for LIMIT 5");
}

#[tokio::test]
async fn a_dropped_stream_stops_the_scan() {
    let ctx = session(1, 4096);
    register(&ctx, "gridded", &test_file(GRIDDED_FILE)).await;

    let plan = ctx
        .sql("SELECT sea_ice_fraction FROM gridded")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let first = stream
        .next()
        .await
        .expect("the scan yields a batch")
        .unwrap();
    assert!(first.num_rows() > 0);
    drop(stream);

    // The array is contiguous: 2 rows of `lon` per chunk at 4096, so 604 chunks.
    let read = scan_counter(&plan, "chunks_read");
    assert!(read <= 3, "read {read} of 604 chunks after the drop");

    // The session still answers after the drop.
    let again = ctx
        .sql("SELECT count(lat) FROM gridded")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        again[0].column(0).as_primitive::<Int64Type>().value(0),
        1208
    );
}

#[tokio::test]
async fn a_removed_file_fails_with_its_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "gone.nc", 4000);
    let ctx = session(1, 1000);
    register(&ctx, "t", &path).await;
    // The plan lists the file. It goes away before the scan opens it.
    let plan = ctx
        .sql(r#"SELECT "V0" FROM t"#)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    std::fs::remove_file(&path).unwrap();

    let error = collect(plan, ctx.task_ctx())
        .await
        .expect_err("the file is gone");

    assert!(error.to_string().contains("gone.nc"), "{error}");
}

#[tokio::test]
async fn the_scan_reads_from_an_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "data.nc", 4000);
    let ctx = session(2, 1000);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("bucket/data.nc"),
            PutPayload::from(std::fs::read(&path).unwrap()),
        )
        .await
        .unwrap();
    let memory = ObjectStoreUrl::parse("memory://").unwrap();
    ctx.register_object_store(memory.as_ref(), store);

    let url = ListingTableUrl::parse("memory:///bucket/data.nc").unwrap();
    let options = ListingOptions::new(Arc::new(format()))
        .with_file_extension("")
        .with_collect_stat(false);
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .unwrap();
    ctx.register_table("remote", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();

    let batches = ctx
        .sql(r#"SELECT count("V0"), sum("V0") FROM remote"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        4000
    );
    assert_eq!(
        batches[0].column(1).as_primitive::<Float64Type>().value(0),
        (0..4000).sum::<i64>() as f64
    );
}

#[tokio::test]
async fn a_plan_that_runs_again_returns_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "data.nc", 40_000);
    let ctx = session(4, 1000);
    register(&ctx, "t", &path).await;
    let plan = ctx
        .sql(r#"SELECT count("V0"), sum("V0") FROM t"#)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let first = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    // The plan keeps its drained file queue until its state is reset.
    let plan = reset_plan_states(plan).unwrap();
    let second = collect(plan, ctx.task_ctx()).await.unwrap();

    assert_eq!(first, second);
}

/// Write a file with `time` in days since 1970 and a value `v` on one dimension of 8.
fn write_time_netcdf(dir: &std::path::Path) -> PathBuf {
    use arrow::array::Float64Array;
    use oxcdf::AttributeValue;

    let path = dir.join("time.nc");
    write_cf_netcdf(
        &path,
        &[("t", Some(8))],
        vec![
            TestVariable {
                name: "time",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from_iter_values((0..8).map(f64::from))),
                attributes: vec![(
                    "units",
                    AttributeValue::Str("days since 1970-01-01".to_string()),
                )],
            },
            TestVariable {
                name: "v",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from_iter_values((0..8).map(f64::from))),
                attributes: vec![],
            },
        ],
    );
    path
}

/// A one-partition session with no filter pushdown, so the scan prunes nothing.
fn session_without_pushdown(batch_size: usize) -> SessionContext {
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::prelude::SessionConfig;

    let rules = PhysicalOptimizer::new()
        .rules
        .into_iter()
        .filter(|rule| !rule.name().starts_with("FilterPushdown"))
        .collect();
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size)
        .with_extension(
            beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
        );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
}

#[tokio::test]
async fn a_time_filter_keeps_the_same_rows_with_the_prune() {
    const SQL: &str = "SELECT v FROM t WHERE time >= TIMESTAMP '1970-01-03' ORDER BY v";
    let dir = tempfile::tempdir().unwrap();
    let path = write_time_netcdf(dir.path());

    let pushed = session(1, 2);
    register(&pushed, "t", &path).await;
    let plain = session_without_pushdown(2);
    register(&plain, "t", &path).await;

    let plan = pushed
        .sql(SQL)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let with_prune = collect(Arc::clone(&plan), pushed.task_ctx()).await.unwrap();
    let plain_plan = plain
        .sql(SQL)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let without_prune = collect(Arc::clone(&plain_plan), plain.task_ctx())
        .await
        .unwrap();

    let values = |batches: &[arrow::record_batch::RecordBatch]| -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Float64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    };
    assert_eq!(values(&with_prune), [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
    assert_eq!(values(&with_prune), values(&without_prune));
    // Chunks of 2 rows: the chunk of days 0 and 1 is skipped.
    assert_eq!(scan_counter(&plan, "chunks_pruned"), 1);
    assert_eq!(scan_counter(&plain_plan, "chunks_pruned"), 0);
}

#[tokio::test]
async fn a_decoded_time_column_is_microseconds() {
    use arrow::datatypes::{DataType, TimeUnit};

    let dir = tempfile::tempdir().unwrap();
    let path = write_time_netcdf(dir.path());
    let ctx = one_partition_session();
    register(&ctx, "t", &path).await;

    let schema = ctx.table("t").await.unwrap().schema().as_arrow().clone();

    assert_eq!(
        schema.field_with_name("time").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

#[tokio::test]
async fn statistics_are_never_an_error() {
    use datafusion::datasource::file_format::FileFormat;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let location = Path::from("broken.nc");
    store
        .put(&location, PutPayload::from(b"not a netcdf file".to_vec()))
        .await
        .unwrap();
    let object = store.head(&location).await.unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, true),
    ]));
    let ctx = one_partition_session();

    let statistics = format()
        .with_enable_statistics(true)
        .infer_stats(&ctx.state(), &store, schema, &object)
        .await
        .expect("statistics are never an error");

    assert!(statistics
        .column_statistics
        .iter()
        .all(|column| column.min_value.get_value().is_none()));
}
