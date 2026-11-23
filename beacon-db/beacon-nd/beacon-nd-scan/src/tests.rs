//! The end-to-end tests of the scan: DataSourceExec, NdSourceExec, NdBroadcastExec.

use std::sync::Arc;

use arrow::array::{AsArray, Int32Array};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
use beacon_nd_execution::exec::{NdBroadcastExec, NdSourceExec, nd_scan_plan};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;

use crate::opener::NdScanOptions;
use crate::schema::file_schema;
use crate::source::{NdSource, encoded_table_schema};
use crate::testing::{Entry, MemoryOpener, entry, grid_dataset, leveled_dataset, ramp};

/// The store URL of each test scan.
const STORE: &str = "memory://";

/// A session with batch size 12 and an in-memory store at [`STORE`].
fn session() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(12));
    let url = ObjectStoreUrl::parse(STORE).unwrap();
    ctx.register_object_store(url.as_ref(), Arc::new(InMemory::new()));
    ctx
}

/// The scan that a test plans.
struct Scan {
    opener: Arc<MemoryOpener>,
    /// The logical file schema.
    schema: Schema,
    files: Vec<PartitionedFile>,
    /// The projected columns. `None` projects all columns.
    columns: Option<Vec<String>>,
    target_partitions: usize,
    limit: Option<usize>,
    options: NdScanOptions,
}

impl Scan {
    /// A scan of all columns of `schema` on one partition.
    fn new(opener: Arc<MemoryOpener>, schema: Schema, files: Vec<PartitionedFile>) -> Self {
        Self {
            opener,
            schema,
            files,
            columns: None,
            target_partitions: 1,
            limit: None,
            options: NdScanOptions::default(),
        }
    }

    fn columns(mut self, columns: &[&str]) -> Self {
        self.columns = Some(columns.iter().map(|name| name.to_string()).collect());
        self
    }

    fn partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    /// The scan config: one group with all files, repartitioned when there are more partitions.
    fn config(self) -> FileScanConfig {
        let table_schema = encoded_table_schema(&self.schema);
        let names: Vec<String> = table_schema
            .table_schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let source = NdSource::new(self.opener, self.options, table_schema);
        let indices = self.columns.map(|columns| {
            columns
                .iter()
                .map(|column| names.iter().position(|name| name == column).unwrap())
                .collect::<Vec<_>>()
        });
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::parse(STORE).unwrap(), Arc::new(source))
                .with_file_group(FileGroup::new(self.files))
                .with_limit(self.limit)
                .with_projection_indices(indices)
                .unwrap()
                .build();
        if self.target_partitions <= 1 {
            return config;
        }
        config
            .file_source()
            .repartitioned(self.target_partitions, 0, None, &config)
            .unwrap()
            .expect("the scan repartitions")
    }

    fn plan(self) -> Arc<dyn ExecutionPlan> {
        nd_scan_plan(self.config()).unwrap()
    }
}

/// A plan that scans `files` of `opener`: DataSourceExec, NdSourceExec, NdBroadcastExec.
/// `columns` are the projected columns; `None` projects all of `schema`.
fn plan(
    opener: Arc<MemoryOpener>,
    schema: Schema,
    files: Vec<PartitionedFile>,
    columns: Option<&[&str]>,
    target_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    let scan = Scan::new(opener, schema, files).partitions(target_partitions);
    match columns {
        Some(columns) => scan.columns(columns),
        None => scan,
    }
    .plan()
}

/// Run `plan` on each of its partitions at the same time and collect the rows.
async fn try_collect(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
    let task = session().task_ctx();
    let partitions = plan.properties().partitioning.partition_count();
    let streams = (0..partitions)
        .map(|partition| plan.execute(partition, Arc::clone(&task)))
        .collect::<Result<Vec<_>>>()?;
    let batches = futures::future::try_join_all(
        streams
            .into_iter()
            .map(|stream| stream.try_collect::<Vec<_>>()),
    )
    .await?;
    Ok(batches.into_iter().flatten().collect())
}

/// Run `plan` on each of its partitions and collect the rows.
async fn collect(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    try_collect(plan).await.unwrap()
}

/// The schema of the grid fixture.
fn grid_schema() -> Schema {
    file_schema(&grid_dataset("a.nd"))
}

/// The test opener with the grid fixture at each of `paths`.
fn grid_opener(paths: &[&str]) -> Arc<MemoryOpener> {
    let opener = paths.iter().fold(MemoryOpener::default(), |opener, path| {
        opener.with(path, Entry::Dataset(grid_dataset(path)))
    });
    Arc::new(opener)
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The Float64 values of column `name`, nulls as `None`.
fn values(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Float64Type>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The `temp` values, sorted.
fn sorted_temps(batches: &[RecordBatch]) -> Vec<f64> {
    let mut temps: Vec<f64> = values(batches, "temp").into_iter().flatten().collect();
    temps.sort_by(f64::total_cmp);
    temps
}

/// The sum of the scan counter `name` over all partitions of the scan at the bottom of `plan`.
fn scan_counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut node = Arc::clone(plan);
    while let Some(child) = node.children().first() {
        node = Arc::clone(child);
    }
    node.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |value| value.as_usize())
}

#[tokio::test]
async fn a_scan_returns_the_rows_of_the_file() {
    let plan = plan(
        grid_opener(&["a.nd"]),
        grid_schema(),
        vec![entry("a.nd")],
        Some(&["temp", "lat", "lon"]),
        1,
    );

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 48);
    let temp = values(&batches, "temp");
    let lat = values(&batches, "lat");
    let lon = values(&batches, "lon");
    let row = temp.iter().position(|value| *value == Some(27.0)).unwrap();
    assert_eq!(lat[row], Some(4.0));
    assert_eq!(lon[row], Some(3.0));
}

#[tokio::test]
async fn one_file_on_four_partitions_returns_the_same_rows() {
    let expected: Vec<f64> = (0..48).map(|value| value as f64).collect();
    for partitions in [1, 4] {
        let plan = plan(
            grid_opener(&["a.nd"]),
            grid_schema(),
            vec![entry("a.nd")],
            None,
            partitions,
        );
        assert_eq!(plan.properties().partitioning.partition_count(), partitions);

        let batches = collect(plan).await;

        assert_eq!(sorted_temps(&batches), expected, "{partitions} partitions");
    }
}

#[tokio::test]
async fn a_collection_on_many_partitions_returns_each_row_once() {
    let paths = ["a.nd", "b.nd", "c.nd"];
    let plan = plan(
        grid_opener(&paths),
        grid_schema(),
        paths.iter().map(|path| entry(path)).collect(),
        None,
        4,
    );

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 144);
    let mut expected: Vec<f64> = (0..48).flat_map(|value| [value as f64; 3]).collect();
    expected.sort_by(f64::total_cmp);
    assert_eq!(sorted_temps(&batches), expected);
}

#[tokio::test]
async fn a_filter_above_the_scan_keeps_the_matching_rows() {
    let scan = plan(
        grid_opener(&["a.nd"]),
        grid_schema(),
        vec![entry("a.nd")],
        None,
        1,
    );
    let schema = scan.schema();
    let predicate = binary(
        col("lat", &schema).unwrap(),
        Operator::Gt,
        lit(3.5),
        &schema,
    )
    .unwrap();
    let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan).unwrap());
    // The rule hands the predicate to `NdSource::try_pushdown_filters`.
    let plan = FilterPushdown::new()
        .optimize(filter, &ConfigOptions::default())
        .unwrap();
    assert_eq!(plan.name(), "FilterExec", "the filter stays above the scan");

    let batches = collect(Arc::clone(&plan)).await;

    assert_eq!(rows(&batches), 24);
    assert!(
        values(&batches, "lat")
            .iter()
            .all(|lat| lat.is_some_and(|lat| lat > 3.5))
    );
    assert_eq!(scan_counter(&plan, "chunks_pruned"), 2);
}

/// `scan` under the filter `column op value`, after the filter pushdown rule.
fn filtered(
    scan: Arc<dyn ExecutionPlan>,
    column: &str,
    op: Operator,
    value: f64,
) -> Arc<dyn ExecutionPlan> {
    let schema = scan.schema();
    let predicate = binary(col(column, &schema).unwrap(), op, lit(value), &schema).unwrap();
    let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan).unwrap());
    FilterPushdown::new()
        .optimize(filter, &ConfigOptions::default())
        .unwrap()
}

#[tokio::test]
async fn a_filter_on_a_column_with_a_wider_table_type_prunes_in_the_table_type() {
    // The file has `lat` in Int32, and the table has it in Float64.
    let lat = Int32Array::from_iter_values(0..24);
    let dimensions = Dimensions::try_new([Dimension::new("lat", 24)]).unwrap();
    let lat = MemoryVariable::new(
        "lat",
        NdArrowArray::try_new(Arc::new(lat), dimensions).unwrap(),
    );
    let dataset = MemoryDataset::try_new("a.nd", vec![lat, ramp("temp", &[("lat", 24)])]).unwrap();
    let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Dataset(dataset)));
    let schema = Schema::new(vec![
        Field::new("lat", DataType::Float64, true),
        Field::new("temp", DataType::Float64, true),
    ]);
    let scan = plan(opener, schema, vec![entry("a.nd")], None, 1);
    let plan = filtered(scan, "lat", Operator::Gt, 11.5);

    let batches = collect(Arc::clone(&plan)).await;

    assert_eq!(rows(&batches), 12);
    assert!(
        values(&batches, "lat")
            .iter()
            .all(|lat| lat.is_some_and(|lat| lat > 11.5))
    );
    assert_eq!(scan_counter(&plan, "chunks_pruned"), 1);
}

#[tokio::test]
async fn a_filter_on_a_column_that_one_file_does_not_have_prunes_that_file() {
    let without_temp = MemoryDataset::try_new(
        "b.nd",
        vec![ramp("lat", &[("lat", 8)]), ramp("lon", &[("lon", 6)])],
    )
    .unwrap();
    let opener = Arc::new(
        MemoryOpener::default()
            .with("a.nd", Entry::Dataset(grid_dataset("a.nd")))
            .with("b.nd", Entry::Dataset(without_temp)),
    );
    let scan = plan(
        opener,
        grid_schema(),
        vec![entry("a.nd"), entry("b.nd")],
        Some(&["temp", "lat"]),
        1,
    );
    let plan = filtered(scan, "temp", Operator::GtEq, 0.0);

    let batches = collect(Arc::clone(&plan)).await;

    // File 2 has only null `temp` values, so the scan reads no chunk of it.
    assert_eq!(rows(&batches), 48);
    assert_eq!(scan_counter(&plan, "chunks_pruned"), 1);
}

#[tokio::test]
async fn a_filter_on_a_rank_0_variable_skips_the_files_that_fail_it() {
    let opener = Arc::new(
        MemoryOpener::default()
            .with("a.nd", Entry::Dataset(leveled_dataset("a.nd", Some(1.0))))
            .with("b.nd", Entry::Dataset(leveled_dataset("b.nd", Some(2.0)))),
    );
    let schema = file_schema(&leveled_dataset("a.nd", Some(1.0)));
    let scan = plan(
        opener,
        schema,
        vec![entry("a.nd"), entry("b.nd")],
        Some(&["temp", "level"]),
        1,
    );
    let plan = filtered(scan, "level", Operator::Eq, 2.0);

    let batches = collect(Arc::clone(&plan)).await;

    // Only file 2 has the level 2, so the scan reads no chunk of file 1.
    assert_eq!(rows(&batches), 48);
    assert!(
        values(&batches, "level")
            .iter()
            .all(|level| *level == Some(2.0))
    );
    assert_eq!(scan_counter(&plan, "chunks_pruned"), 4);
    assert_eq!(scan_counter(&plan, "chunks_read"), 4);
}

#[tokio::test]
async fn a_column_that_one_file_does_not_have_is_null_there() {
    let without_temp = MemoryDataset::try_new(
        "b.nd",
        vec![ramp("lat", &[("lat", 8)]), ramp("lon", &[("lon", 6)])],
    )
    .unwrap();
    let opener = Arc::new(
        MemoryOpener::default()
            .with("a.nd", Entry::Dataset(grid_dataset("a.nd")))
            .with("b.nd", Entry::Dataset(without_temp)),
    );
    let plan = plan(
        opener,
        grid_schema(),
        vec![entry("a.nd"), entry("b.nd")],
        Some(&["temp", "lat"]),
        1,
    );

    let batches = collect(plan).await;

    // File 1 gives 48 rows with a value, file 2 gives 8 rows with a null.
    let temp = values(&batches, "temp");
    assert_eq!(temp.len(), 56);
    assert_eq!(temp.iter().filter(|value| value.is_some()).count(), 48);
    assert_eq!(temp.iter().filter(|value| value.is_none()).count(), 8);
    let lat = values(&batches, "lat");
    assert_eq!(lat.iter().flatten().count(), 56);
    let mut lat_of_null: Vec<f64> = temp
        .iter()
        .zip(&lat)
        .filter(|(temp, _)| temp.is_none())
        .filter_map(|(_, lat)| *lat)
        .collect();
    lat_of_null.sort_by(f64::total_cmp);
    let expected: Vec<f64> = (0..8).map(|value| value as f64).collect();
    assert_eq!(lat_of_null, expected);
}

#[tokio::test]
async fn a_count_reads_no_data() {
    let dataset = grid_dataset("a.nd");
    let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Dataset(dataset.clone())));
    let plan = plan(opener, grid_schema(), vec![entry("a.nd")], Some(&[]), 1);

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 48);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    let reads: usize = ["temp", "lat", "lon"]
        .iter()
        .map(|name| dataset.reads(name))
        .sum();
    assert_eq!(reads, 0);
}

#[tokio::test]
async fn a_count_on_four_partitions_counts_each_row_once() {
    let plan = plan(
        grid_opener(&["a.nd"]),
        grid_schema(),
        vec![entry("a.nd")],
        Some(&[]),
        4,
    );

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 48);
}

#[tokio::test]
async fn a_limit_reads_only_the_first_chunks() {
    let mut scan = Scan::new(grid_opener(&["a.nd"]), grid_schema(), vec![entry("a.nd")]);
    scan.limit = Some(5);
    let plan = scan.plan();

    let batches = collect(Arc::clone(&plan)).await;

    assert_eq!(rows(&batches), 12);
    assert_eq!(scan_counter(&plan, "chunks_read"), 1);
}

#[tokio::test]
async fn a_dropped_stream_stops_the_reads() {
    let plan = plan(
        grid_opener(&["a.nd"]),
        grid_schema(),
        vec![entry("a.nd")],
        None,
        1,
    );
    let mut stream = execute_stream(Arc::clone(&plan), session().task_ctx()).unwrap();

    let first = stream
        .next()
        .await
        .expect("the scan yields a batch")
        .unwrap();
    assert!(first.num_rows() > 0);
    drop(stream);
    // Each waiting task gets one turn, so a late read shows in the counter.
    tokio::task::yield_now().await;

    let read = scan_counter(&plan, "chunks_read");
    assert!(read <= 2, "read {read} of 4 chunks after the drop");
}

#[tokio::test]
async fn a_plan_that_runs_again_returns_the_same_rows() {
    let plan = plan(
        grid_opener(&["a.nd"]),
        grid_schema(),
        vec![entry("a.nd")],
        None,
        4,
    );

    let first = collect(Arc::clone(&plan)).await;
    // The plan keeps its drained file queue until its state is reset.
    let plan = reset_plan_states(plan).unwrap();
    let second = collect(plan).await;

    assert_eq!(rows(&first), 48);
    assert_eq!(rows(&second), 48);
}

#[tokio::test]
async fn a_round_robin_repartition_keeps_each_row() {
    let config = Scan::new(grid_opener(&["a.nd"]), grid_schema(), vec![entry("a.nd")]).config();
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    let repartition =
        Arc::new(RepartitionExec::try_new(scan, Partitioning::RoundRobinBatch(3)).unwrap());
    let source = Arc::new(NdSourceExec::try_new(repartition).unwrap());
    let plan: Arc<dyn ExecutionPlan> = Arc::new(NdBroadcastExec::try_new(source).unwrap());
    assert_eq!(plan.properties().partitioning.partition_count(), 3);

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 48);
    let expected: Vec<f64> = (0..48).map(|value| value as f64).collect();
    assert_eq!(sorted_temps(&batches), expected);
}

/// A scan of the files good, bad and good, where the bad file does not open.
fn scan_with_a_bad_file() -> Scan {
    let opener = Arc::new(
        MemoryOpener::default()
            .with("a.nd", Entry::Dataset(grid_dataset("a.nd")))
            .with("bad.nd", Entry::Invalid("bad header"))
            .with("c.nd", Entry::Dataset(grid_dataset("c.nd"))),
    );
    let files = vec![entry("a.nd"), entry("bad.nd"), entry("c.nd")];
    Scan::new(opener, grid_schema(), files)
}

#[tokio::test]
async fn the_skip_options_read_past_a_bad_file() {
    let mut scan = scan_with_a_bad_file();
    scan.options.skip_invalid_files = true;
    let plan = scan.plan();

    let batches = collect(Arc::clone(&plan)).await;

    assert_eq!(rows(&batches), 96);
    assert_eq!(scan_counter(&plan, "files_skipped"), 1);
}

#[tokio::test]
async fn a_bad_file_without_the_skip_fails_with_its_path() {
    let error = try_collect(scan_with_a_bad_file().plan())
        .await
        .expect_err("the bad file fails the scan");

    assert!(error.to_string().contains("bad.nd"), "{error}");
}

#[tokio::test]
async fn no_file_gives_no_row() {
    let plan = plan(grid_opener(&[]), grid_schema(), Vec::new(), None, 4);
    assert_eq!(plan.properties().partitioning.partition_count(), 4);

    let batches = collect(plan).await;

    assert_eq!(rows(&batches), 0);
}
