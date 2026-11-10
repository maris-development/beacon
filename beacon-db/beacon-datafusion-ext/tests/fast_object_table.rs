//! End-to-end tests for [`FastObjectTable`].
//!
//! Real Parquet objects in an in-memory store. The plan's own file groups are
//! what the scan will read — which is what most of these assert on, alongside
//! the rows that come out.

use std::sync::Arc;

use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
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
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, path::Path};

const STORE_URL: &str = "test://stats/";

struct Fixture {
    ctx: SessionContext,
    objects: Arc<InMemory>,
}

/// A session with an in-memory object store at `test://stats/`.
async fn fixture() -> Fixture {
    fixture_with(None).await
}

/// The same, with a fixed partition target where a test needs one.
async fn fixture_with(target_partitions: Option<usize>) -> Fixture {
    // `FastObjectTable` merges its schemas through this. `RuntimeBuilder`
    // registers it for a server; a session built here registers it itself.
    let mut config = SessionConfig::new().with_extension(ArrowTypeWidening::default_extension());
    if let Some(partitions) = target_partitions {
        config = config.with_target_partitions(partitions);
    }
    let ctx = SessionContext::new_with_config(config);

    let objects = Arc::new(InMemory::new());
    ctx.register_object_store(
        ObjectStoreUrl::parse(STORE_URL).unwrap().as_ref(),
        objects.clone(),
    );

    Fixture { ctx, objects }
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
}

/// Write one Parquet object holding `values` in column `v`.
async fn put_parquet(objects: &InMemory, path: &str, values: &[f64]) {
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
        if let Some(exec) = node.downcast_ref::<DataSourceExec>() {
            return exec
                .data_source()
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
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut paths: Vec<String> = scan_config(plan)
        .file_groups
        .iter()
        .flat_map(|group| group.iter().map(|f| f.object_meta.location.to_string()))
        .collect();
    paths.sort();
    paths
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
    assert_eq!(rows(plan, &fixture.ctx).await, 5);
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

/// The merge rule is the caller's to choose. The rule of the session widens a
/// numeric column that two URLs give two widths. A caller that wants another
/// answer names its own strategy.
///
/// Across URLs, note, not within one: a format merges the files behind a single
/// URL itself. It merges them through the same session rule, so a `read_*` over
/// files that disagree behaves the same however the URLs are spelled.
#[tokio::test(flavor = "multi_thread")]
async fn the_caller_can_name_the_merge_rule() {
    use beacon_datafusion_ext::type_widening::{
        ArrowTypeWideningStrategy, DefaultArrowTypeWidening, LabeledSchema,
    };
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::SchemaRef;

    /// A rule of its own: keep the first type seen for a column instead of
    /// refusing the second. Order-sensitive by construction, so it says so and
    /// the merge folds it over every schema in order.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct FirstTypeWins;

    #[typetag::serde]
    impl ArrowTypeWideningStrategy for FirstTypeWins {
        fn merge_schemas(
            &self,
            schemas: &[LabeledSchema],
        ) -> Result<SchemaRef, datafusion::arrow::error::ArrowError> {
            let mut fields: Vec<datafusion::arrow::datatypes::FieldRef> = Vec::new();
            for labeled in schemas {
                for field in labeled.schema.fields() {
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

        fn physical_expr_adapter_factory(
            &self,
        ) -> Arc<dyn datafusion::physical_expr_adapter::PhysicalExprAdapterFactory> {
            Arc::new(datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory)
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

    // The rule of the session widens the column, and the same rule by name gives
    // the same answer.
    for table in [
        FastObjectTable::try_new(&state, Arc::new(ParquetFormat::default()), urls.clone())
            .await
            .unwrap(),
        FastObjectTable::try_new_with_widening(
            &state,
            Arc::new(ParquetFormat::default()),
            urls.clone(),
            &DefaultArrowTypeWidening::new(),
        )
        .await
        .unwrap(),
    ] {
        assert_eq!(
            table.schema().field_with_name("v").unwrap().data_type(),
            &DataType::Int64,
            "an Int32 column beside an Int64 one needs an Int64"
        );
    }

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
        first_wins
            .schema()
            .field_with_name("v")
            .unwrap()
            .data_type(),
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

/// A path that matches no object names the path, not the merge rule.
///
/// A caller who mistypes a path is the most common failure here, and the message
/// used to come from whatever the format did with zero schemas. Parquet, CSV,
/// Arrow IPC, ODV and BBF merged an empty list, so `DefaultArrowTypeWidening`
/// reported "No schemas provided for merging" — the merge rule's own state, which
/// names neither the path nor the cause. netCDF, HDF5, GeoParquet and TIFF
/// answered with an empty schema instead, so a mistyped path returned zero rows
/// and zero columns and no error at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_that_matches_nothing_names_the_path() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;

    for url in [
        "test://stats/nothing/",             // a prefix that holds no object
        "test://stats/no/such/file.parquet", // a file that is not there
    ] {
        let urls = vec![ListingTableUrl::parse(url).unwrap()];
        let error = FastObjectTable::try_new(
            &fixture.ctx.state(),
            Arc::new(ParquetFormat::default()),
            urls,
        )
        .await
        .expect_err("a path that matches no object must fail")
        .to_string();

        assert!(
            error.contains("no file matched"),
            "the message must say what happened, got: {error}"
        );
        assert!(
            error.contains(url.trim_end_matches('/')) || error.contains(url),
            "the message must name the path, got: {error}"
        );
        assert!(
            !error.contains("No schemas provided for merging"),
            "the merge rule's own state must not reach the caller, got: {error}"
        );
    }
}

/// A glob stays in the message.
///
/// `ListingTableUrl` parses a glob off the path and holds it beside the prefix, so
/// the prefix alone would show `obs/` for `obs/*.tsv` and read as the directory
/// being wrong. The mistyped part is the glob.
#[tokio::test(flavor = "multi_thread")]
async fn a_glob_that_matches_nothing_keeps_the_glob_in_the_message() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0]).await;

    let urls = vec![ListingTableUrl::parse("test://stats/obs/*.tsv").unwrap()];
    let error = FastObjectTable::try_new(
        &fixture.ctx.state(),
        Arc::new(ParquetFormat::default()),
        urls,
    )
    .await
    .expect_err("a glob that matches no object must fail")
    .to_string();

    assert!(
        error.contains("obs/*.tsv"),
        "the glob is the part the caller mistyped, got: {error}"
    );
}

/// A path that does hold objects keeps working, so the check above costs a
/// successful query nothing and changes no answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_that_matches_objects_is_unaffected() {
    let fixture = fixture().await;
    put_parquet(&fixture.objects, "obs/a.parquet", &[1.0, 2.0]).await;

    let table = table(&fixture.ctx, &["test://stats/obs/"]).await;
    let plan = table
        .scan(&fixture.ctx.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(rows(plan, &fixture.ctx).await, 2);
}
