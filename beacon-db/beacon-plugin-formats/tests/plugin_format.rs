//! The plugin format wrapper: read path, through the FFI tables.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int64Type};
use arrow::util::pretty::pretty_format_batches;
use beacon_datafusion_ext::format_ext::new_file_format_registry_handle;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_plugin_formats::{PluginFileFormatFactory, plugin_options};
use beacon_plugin_sdk::file_format::FormatFactory;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use url::Url;

#[path = "plugin_format/test_formats.rs"]
mod test_formats;

use beacon_datafusion_ext::nd::{NdFilterPushdown, NdProjectionPushdown};
use datafusion::execution::SessionStateBuilder;
use test_formats::{GridText, LineText, Seen, foreign};

struct Fixture {
    ctx: SessionContext,
    store: Arc<InMemory>,
}

impl Fixture {
    fn new() -> Self {
        Self::build(false)
    }

    /// A session with the nd optimizer rules, as a server with `nd_pipeline` on has.
    fn with_nd_rules() -> Self {
        Self::build(true)
    }

    fn build(nd_rules: bool) -> Self {
        let config = SessionConfig::new()
            .with_extension(new_file_format_registry_handle())
            .with_extension(ArrowTypeWidening::default_extension());
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if nd_rules {
            builder = builder
                .with_physical_optimizer_rule(Arc::new(NdProjectionPushdown::new()))
                .with_physical_optimizer_rule(Arc::new(NdFilterPushdown::new()));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        let store = Arc::new(InMemory::new());
        ctx.register_object_store(
            &Url::parse("memory://").unwrap(),
            store.clone() as Arc<dyn ObjectStore>,
        );
        Self { ctx, store }
    }

    async fn put(&self, path: &str, text: &str) {
        self.store
            .put(
                &Path::from(path),
                PutPayload::from(text.as_bytes().to_vec()),
            )
            .await
            .unwrap();
    }

    /// Registers table `t` over `memory:///<prefix>` with the plugin format.
    async fn table(
        &self,
        factory: Arc<dyn FormatFactory>,
        prefix: &str,
        options: &[(&str, &str)],
        partition_cols: Vec<(String, DataType)>,
    ) -> Result<()> {
        let extension = format!(".{}", factory.extensions()[0]);
        let plugin = PluginFileFormatFactory::new(foreign(factory));
        let options: HashMap<String, String> = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let state = self.ctx.state();
        let format = plugin.create(&state, &options)?;
        let listing = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_table_partition_cols(partition_cols);
        let url = ListingTableUrl::parse(format!("memory:///{prefix}"))?;
        let config = ListingTableConfig::new(url)
            .with_listing_options(listing)
            .infer_schema(&state)
            .await?;
        self.ctx.deregister_table("t")?;
        self.ctx
            .register_table("t", Arc::new(ListingTable::try_new(config)?))?;
        Ok(())
    }

    async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.ctx.sql(query).await?.collect().await
    }
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn text(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

fn line_text() -> (Arc<dyn FormatFactory>, Arc<Seen>) {
    let format = LineText::default();
    let seen = format.seen.clone();
    (Arc::new(format), seen)
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_one_file() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64,b:Utf8\n1,x\n2,y\n").await;
    let (format, _) = line_text();
    fx.table(format, "t/", &[], vec![]).await.unwrap();
    let batches = fx.sql("SELECT a, b FROM t ORDER BY a").await.unwrap();
    assert_eq!(rows(&batches), 2);
    assert!(text(&batches).contains("| 2 | y |"), "{}", text(&batches));
}

#[tokio::test(flavor = "multi_thread")]
async fn merges_two_files_with_widening() {
    let fx = Fixture::new();
    fx.put("t/1.ltxt", "a:Int32\n1\n").await;
    fx.put("t/2.ltxt", "b:Utf8,a:Int64\nz,5000000000\n").await;
    let (format, _) = line_text();
    fx.table(format, "t/", &[], vec![]).await.unwrap();
    let schema = fx.ctx.table("t").await.unwrap().schema().as_arrow().clone();
    assert_eq!(
        schema.field_with_name("a").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("b").unwrap().data_type(),
        &DataType::Utf8
    );
    let batches = fx.sql("SELECT a, b FROM t ORDER BY a").await.unwrap();
    let rendered = text(&batches);
    assert!(rendered.contains("| 1          |   |"), "{rendered}");
    assert!(rendered.contains("| 5000000000 | z |"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_and_limit() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64,b:Utf8\n1,x\n2,y\n3,z\n").await;
    let (format, seen) = line_text();
    fx.table(format, "t/", &[], vec![]).await.unwrap();
    let batches = fx.sql("SELECT b FROM t LIMIT 1").await.unwrap();
    assert_eq!(rows(&batches), 1);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(*seen.requested.lock().unwrap(), vec![vec!["b".to_string()]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_reach_the_plugin_and_results_stay_correct() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64,b:Utf8\n1,x\n2,y\n3,z\n").await;
    let (format, seen) = line_text();
    fx.table(format, "t/", &[], vec![]).await.unwrap();
    // The line format ignores the filter; Beacon applies it again.
    let batches = fx
        .sql("SELECT a FROM t WHERE a > 1 ORDER BY a")
        .await
        .unwrap();
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    assert_eq!(values, vec![2, 3]);
    assert_eq!(*seen.filters.lock().unwrap(), vec![1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_columns_are_added() {
    let fx = Fixture::new();
    fx.put("t/part=x/a.ltxt", "a:Int64\n1\n").await;
    fx.put("t/part=y/a.ltxt", "a:Int64\n2\n").await;
    let (format, _) = line_text();
    fx.table(
        format,
        "t/",
        &[],
        vec![("part".to_string(), DataType::Utf8)],
    )
    .await
    .unwrap();
    let batches = fx.sql("SELECT a, part FROM t ORDER BY a").await.unwrap();
    let rendered = text(&batches);
    assert!(rendered.contains("| 1 | x    |"), "{rendered}");
    assert!(rendered.contains("| 2 | y    |"), "{rendered}");
    let only = fx.sql("SELECT part FROM t WHERE part = 'y'").await.unwrap();
    assert_eq!(rows(&only), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_error_names_format_and_file() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64\n1\n").await;
    let (format, _) = line_text();
    fx.table(format, "t/", &[("fail_open", "true")], vec![])
        .await
        .unwrap();
    let error = fx.sql("SELECT * FROM t").await.unwrap_err().to_string();
    assert!(error.contains("linetext"), "{error}");
    assert!(error.contains("t/a.ltxt"), "{error}");
    assert!(error.contains("cannot open this file"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_file_gives_no_rows() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64\n1\n").await;
    fx.put("t/empty.ltxt", "").await;
    fx.put("t/header.ltxt", "a:Int64\n").await;
    let (format, _) = line_text();
    fx.table(format, "t/", &[], vec![]).await.unwrap();
    let batches = fx.sql("SELECT a FROM t").await.unwrap();
    assert_eq!(rows(&batches), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_panic_is_an_error() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64\n1\n").await;
    let (format, _) = line_text();
    fx.table(format, "t/", &[("panic_poll", "true")], vec![])
        .await
        .unwrap();
    let error = fx.sql("SELECT * FROM t").await.unwrap_err().to_string();
    assert!(error.contains("panic in plugin stream"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_option_is_a_plan_error() {
    let fx = Fixture::new();
    let (format, _) = line_text();
    let error = fx
        .table(format, "t/", &[("format.bad", "x")], vec![])
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("linetext"), "{error}");
    assert!(error.contains("unknown option bad"), "{error}");
}

#[test]
fn options_are_normalized() {
    let raw: HashMap<String, String> = [
        ("format.delimiter", ";"),
        ("execution.keep_partition_by_columns", "true"),
        ("format.single_file_output", "true"),
        ("mode", "x"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let (options, single) = plugin_options(&raw).unwrap();
    let expected: HashMap<String, String> = [("delimiter", ";"), ("mode", "x")]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert_eq!(options, expected);
    assert_eq!(single, Some(true));
}

#[test]
fn bad_single_file_output_is_an_error() {
    let raw = HashMap::from([("single_file_output".to_string(), "maybe".to_string())]);
    assert!(plugin_options(&raw).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn options_reach_create_as_bare_keys() {
    let fx = Fixture::new();
    fx.put("t/a.ltxt", "a:Int64\n1\n").await;
    let (format, seen) = line_text();
    fx.table(format, "t/", &[("format.delimiter", ";")], vec![])
        .await
        .unwrap();
    let options = seen.options.lock().unwrap().clone();
    assert_eq!(
        options.last().unwrap(),
        &HashMap::from([("delimiter".to_string(), ";".to_string())])
    );
}

fn grid_text() -> (Arc<dyn FormatFactory>, Arc<Seen>) {
    let format = GridText::default();
    let seen = format.seen.clone();
    (Arc::new(format), seen)
}

/// A 2 x 3 grid: `time` and `depth` coordinates, and `temp` over both.
const GRID: &str = "var time Int64 time=2 : 1,2\n\
var depth Int64 depth=3 : 10,20,30\n\
var temp Float64 time=2,depth=3 : 1,2,3,4,5,6\n";

/// The same shape, with a `salinity` variable and no `temp`.
const GRID_SALT: &str = "var time Int64 time=1 : 9\n\
var depth Int64 depth=1 : 40\n\
var salinity Float64 time=1,depth=1 : 35\n";

async fn explain(fx: &Fixture, query: &str) -> String {
    let batches = fx.sql(&format!("EXPLAIN {query}")).await.unwrap();
    text(&batches)
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_plan_has_source_and_broadcast() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let plan = explain(&fx, "SELECT * FROM t").await;
    assert!(plan.contains("NdSourceExec"), "{plan}");
    assert!(plan.contains("NdBroadcastExec"), "{plan}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_rows_are_broadcast() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let batches = fx
        .sql("SELECT time, depth, temp FROM t ORDER BY time, depth")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 6);
    let rendered = text(&batches);
    assert!(rendered.contains("| 1    | 10    | 1.0  |"), "{rendered}");
    assert!(rendered.contains("| 2    | 30    | 6.0  |"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_projection_reaches_the_plugin() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    let (format, seen) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let batches = fx.sql("SELECT depth FROM t").await.unwrap();
    assert_eq!(rows(&batches), 3);
    assert_eq!(
        *seen.requested.lock().unwrap(),
        vec![vec!["depth".to_string()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_filter_pushdown_with_nd_rules() {
    let query = "SELECT time, depth, temp FROM t WHERE temp > 4 ORDER BY temp";
    let with_rules = Fixture::with_nd_rules();
    let without = Fixture::new();
    for fx in [&with_rules, &without] {
        fx.put("g/a.grid", GRID).await;
        let (format, _) = grid_text();
        fx.table(format, "g/", &[], vec![]).await.unwrap();
    }
    let plan = explain(&with_rules, query).await;
    assert!(plan.contains("NdFilterExec"), "{plan}");
    let a = with_rules.sql(query).await.unwrap();
    let b = without.sql(query).await.unwrap();
    assert_eq!(text(&a), text(&b));
    assert_eq!(rows(&a), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_missing_column_broadcasts_null() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    fx.put("g/b.grid", GRID_SALT).await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let batches = fx
        .sql("SELECT time, temp, salinity FROM t ORDER BY time, depth")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 7);
    let rendered = text(&batches);
    assert!(
        rendered.contains("| 9    |      | 35.0     |"),
        "{rendered}"
    );
    assert!(
        rendered.contains("| 1    | 1.0  |          |"),
        "{rendered}"
    );
}

async fn nd_partition_fixture(fx: &Fixture) {
    fx.put("g/run=x/a.grid", GRID).await;
    fx.put("g/run=y/b.grid", GRID_SALT).await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![("run".to_string(), DataType::Utf8)])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_partition_columns_are_broadcast() {
    let fx = Fixture::new();
    nd_partition_fixture(&fx).await;
    let batches = fx
        .sql("SELECT run, time, depth FROM t ORDER BY time, depth")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 7);
    let rendered = text(&batches);
    assert!(rendered.contains("| x   | 2    | 30    |"), "{rendered}");
    assert!(rendered.contains("| y   | 9    |"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_partition_columns_alone_keep_the_grid_size() {
    let fx = Fixture::new();
    nd_partition_fixture(&fx).await;
    assert_eq!(rows(&fx.sql("SELECT run FROM t").await.unwrap()), 7);
    let counts = fx
        .sql("SELECT run, count(*) AS n FROM t GROUP BY run ORDER BY run")
        .await
        .unwrap();
    let rendered = text(&counts);
    assert!(rendered.contains("| x   | 6 |"), "{rendered}");
    assert!(rendered.contains("| y   | 1 |"), "{rendered}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_partition_filters_with_nd_rules() {
    let query = "SELECT run, time, temp FROM t WHERE run = 'x' AND temp > 4 ORDER BY temp";
    let with_rules = Fixture::with_nd_rules();
    let without = Fixture::new();
    for fx in [&with_rules, &without] {
        nd_partition_fixture(fx).await;
    }
    let a = with_rules.sql(query).await.unwrap();
    let b = without.sql(query).await.unwrap();
    assert_eq!(text(&a), text(&b));
    assert_eq!(rows(&a), 2);
    let only_y = with_rules
        .sql("SELECT time FROM t WHERE run = 'y'")
        .await
        .unwrap();
    assert_eq!(rows(&only_y), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_count_reads_the_grid_size() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    fx.put("g/b.grid", GRID_SALT).await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let count = fx.sql("SELECT count(*) AS n FROM t").await.unwrap();
    assert!(text(&count).contains("| 7 |"), "{}", text(&count));
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_empty_batch_gives_no_rows() {
    let fx = Fixture::new();
    fx.put("g/a.grid", GRID).await;
    fx.put("g/empty.grid", "").await;
    let (format, _) = grid_text();
    fx.table(format, "g/", &[], vec![]).await.unwrap();
    let batches = fx.sql("SELECT time, temp FROM t").await.unwrap();
    assert_eq!(rows(&batches), 6);

    let zero = Fixture::new();
    zero.put("g/a.grid", GRID).await;
    let (format, _) = grid_text();
    zero.table(format, "g/", &[("zero_rows", "true")], vec![])
        .await
        .unwrap();
    let batches = zero.sql("SELECT time, temp FROM t").await.unwrap();
    assert_eq!(rows(&batches), 0);
}
