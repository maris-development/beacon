//! Tests over a Lance warehouse and a data store, both in memory.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_datafusion_ext::format_ext::{FileFormatFactoryExt, FileFormatRegistry};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_external_table::statistics_cache::{
    ExternalTableStatisticsProvider, StatisticsRegistration,
};
use beacon_external_table::{
    ExternalTableBuilder, ExternalTableDefinition, RefreshPolicy, SchemaSource,
};
use beacon_lance::LanceWarehouse;
use datafusion::catalog::TableProvider;
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext, ident, lit};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use tokio::sync::Semaphore;

use crate::table::TableStatistics;
use crate::{Config, FileStatistics, SessionCell};

/// The data store, the session over it, and a Lance warehouse.
struct Fixture {
    data: Arc<dyn ObjectStore>,
    ctx: Arc<SessionContext>,
    cell: SessionCell,
    warehouse: Arc<LanceWarehouse>,
}

impl Fixture {
    fn new() -> Self {
        let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![Arc::new(ParquetFormatFactory)];
        let registry = OnceLock::new();
        assert!(registry.set(FileFormatRegistry::new(&formats)).is_ok());
        let config = SessionConfig::new()
            .with_extension(Arc::new(registry))
            .with_extension(Arc::new(ListingFactory::new(None)));
        let ctx = Arc::new(SessionContext::new_with_config(config));
        ctx.register_object_store(store_url().as_ref(), Arc::clone(&data));
        let cell: SessionCell = Arc::new(OnceLock::new());
        assert!(cell.set(Arc::downgrade(&ctx)).is_ok());
        Self {
            data,
            ctx,
            cell,
            warehouse: Arc::new(LanceWarehouse::new(Arc::new(InMemory::new()))),
        }
    }

    /// Writes a Parquet file at `data/<name>` whose `TEMP` goes from `min` to `min + 10`.
    async fn put(&self, name: &str, min: f64) -> ObjectMeta {
        let schema = file_schema("TEMP", "DEPTH");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![min, min + 10.0, min + 5.0])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 4000, 250])),
            ],
        )
        .expect("a valid batch");
        self.put_batch(name, batch).await
    }

    async fn put_batch(&self, name: &str, batch: RecordBatch) -> ObjectMeta {
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).expect("a writer");
        writer.write(&batch).expect("the batch is written");
        writer.close().expect("the file is closed");
        let location = Path::from(format!("data/{name}"));
        self.data
            .put(&location, bytes.into())
            .await
            .expect("the file is stored");
        self.data.head(&location).await.expect("the file exists")
    }

    /// Stores a file that is not Parquet.
    async fn put_broken(&self, name: &str) -> ObjectMeta {
        let location = Path::from(format!("data/{name}"));
        self.data
            .put(&location, PutPayload::from_static(b"no parquet"))
            .await
            .expect("the file is stored");
        self.data.head(&location).await.expect("the file exists")
    }

    async fn open(&self, registration: StatisticsRegistration) -> TableStatistics {
        TableStatistics::open(Arc::clone(&self.warehouse), registration)
            .await
            .expect("the statistics table opens")
    }

    async fn refresh(&self, statistics: &TableStatistics) -> crate::RefreshSummary {
        statistics
            .refresh(
                Arc::clone(&self.ctx),
                &Semaphore::new(4),
                &Config::default(),
            )
            .await
            .expect("the refresh succeeds")
    }

    fn engine(&self) -> FileStatistics {
        self.engine_with(Config::default())
    }

    fn engine_with(&self, config: Config) -> FileStatistics {
        FileStatistics::new(
            config,
            tokio::runtime::Handle::current(),
            Arc::clone(&self.cell),
            Arc::clone(&self.warehouse),
        )
    }
}

fn store_url() -> ObjectStoreUrl {
    ObjectStoreUrl::parse("memory://").expect("a valid store url")
}

fn file_schema(temp: &str, depth: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(temp, DataType::Float64, false),
        Field::new(depth, DataType::Int64, false),
    ]))
}

/// The registration of the table `obs` over `data/`.
fn registration(schema: SchemaRef, columns: Option<Vec<String>>) -> StatisticsRegistration {
    StatisticsRegistration {
        table: "obs".to_string(),
        store_url: store_url(),
        urls: vec![ListingTableUrl::parse("memory:///data/").expect("a valid url")],
        file_type: "parquet".to_string(),
        format_options: HashMap::new(),
        schema,
        columns,
        refresh_interval: Duration::from_secs(3600),
        type_widening: Arc::new(DefaultArrowTypeWidening::new()),
    }
}

fn temp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "TEMP",
        DataType::Float64,
        true,
    )]))
}

/// The `TEMP` minimum of each container.
fn mins(statistics: &dyn PruningStatistics) -> Vec<Option<f64>> {
    let min = statistics
        .min_values(&Column::from_name("TEMP"))
        .expect("a TEMP column");
    min.as_primitive::<Float64Type>().iter().collect()
}

async fn rows(statistics: &TableStatistics) -> usize {
    statistics
        .snapshot()
        .count_rows(None)
        .await
        .expect("a row count")
}

async fn fetch(
    statistics: &TableStatistics,
    objects: &[&ObjectMeta],
) -> crate::read::StampStatistics {
    let stamps: Vec<u64> = objects
        .iter()
        .map(|object| crate::stamp::stamp(&store_url(), object))
        .collect();
    statistics
        .fetch(&stamps, &temp_schema())
        .await
        .expect("the statistics read")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refresh_analyzes_new_files_and_skips_unchanged_ones() {
    // Arrange
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    fixture.put("high.parquet", 100.0).await;
    fixture.put_broken("broken.parquet").await;
    let statistics = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;

    // Act
    let first = fixture.refresh(&statistics).await;
    let version = statistics.snapshot().version().version;
    let second = fixture.refresh(&statistics).await;

    // Assert
    assert_eq!((first.listed, first.analyzed, first.removed), (3, 3, 0));
    assert_eq!((second.analyzed, second.removed), (0, 0));
    assert_eq!(
        statistics.snapshot().version().version,
        version,
        "a refresh with no change writes nothing"
    );
    assert_eq!(rows(&statistics).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_follows_the_request_and_reads_unknown_files_as_null() {
    // Arrange
    let fixture = Fixture::new();
    let low = fixture.put("low.parquet", 0.0).await;
    let high = fixture.put("high.parquet", 100.0).await;
    let broken = fixture.put_broken("broken.parquet").await;
    let statistics = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;
    fixture.refresh(&statistics).await;
    let unknown = fixture.put("unknown.parquet", 0.0).await;
    let changed = fixture.put("low.parquet", 50.0).await;

    // Act
    let read = fetch(
        &statistics,
        &[&high, &unknown, &low, &broken, &changed, &high],
    )
    .await;

    // Assert
    assert_eq!(read.num_containers(), 6);
    assert_eq!(
        mins(&read),
        [Some(100.0), None, Some(0.0), None, None, Some(100.0)],
        "unknown, failed and changed files read as null"
    );
    let rows = read.row_counts().expect("row counts");
    assert_eq!(rows.null_count(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_file_replaces_only_its_row_and_a_deleted_file_loses_it() {
    // Arrange
    let fixture = Fixture::new();
    let low = fixture.put("low.parquet", 0.0).await;
    fixture.put("high.parquet", 100.0).await;
    fixture.put("gone.parquet", 7.0).await;
    let statistics = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;
    fixture.refresh(&statistics).await;

    // Act
    let high = fixture.put("high.parquet", 200.0).await;
    fixture
        .data
        .delete(&Path::from("data/gone.parquet"))
        .await
        .expect("deleted");
    let summary = fixture.refresh(&statistics).await;

    // Assert
    assert_eq!(
        (summary.listed, summary.analyzed, summary.removed),
        (2, 1, 2)
    );
    assert_eq!(rows(&statistics).await, 2);
    assert_eq!(
        mins(&fetch(&statistics, &[&low, &high]).await),
        [Some(0.0), Some(200.0)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_statistics_columns_rebuild_the_table_and_the_same_ones_keep_it() {
    // Arrange
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    let statistics = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;
    fixture.refresh(&statistics).await;

    // Act
    let same = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;
    let rows_same = rows(&same).await;
    let fewer = fixture
        .open(registration(
            file_schema("TEMP", "DEPTH"),
            Some(vec!["TEMP".to_string()]),
        ))
        .await;

    // Assert
    assert_eq!(rows_same, 1, "the same columns keep the rows");
    assert_eq!(rows(&fewer).await, 0, "other columns start a new table");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn names_with_a_dot_or_a_space_keep_their_statistics() {
    // Arrange
    let fixture = Fixture::new();
    let schema = file_schema("sea water.temp", "depth below");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Float64Array::from(vec![4.0, 6.0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("a valid batch");
    let object = fixture.put_batch("odd.parquet", batch).await;
    let statistics = fixture.open(registration(schema, None)).await;
    fixture.refresh(&statistics).await;

    // Act
    let wanted = Schema::new(vec![
        Field::new("sea water.temp", DataType::Float64, true),
        Field::new("depth below", DataType::Int64, true),
    ]);
    let stamp = crate::stamp::stamp(&store_url(), &object);
    let read = statistics.fetch(&[stamp], &wanted).await.expect("a read");

    // Assert
    let temp = read
        .max_values(&Column::from_name("sea water.temp"))
        .expect("a dotted column");
    assert_eq!(temp.as_primitive::<Float64Type>().value(0), 6.0);
    let depth = read
        .max_values(&Column::from_name("depth below"))
        .expect("a spaced column");
    assert_eq!(
        depth.as_primitive::<arrow::datatypes::Int64Type>().value(0),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_engine_refuses_too_many_columns_and_drops_statistics() {
    // Arrange
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    let engine = fixture.engine();
    let wide: Vec<Field> = (0..=crate::MAX_STATISTICS_COLUMNS)
        .map(|index| Field::new(format!("c{index}"), DataType::Float64, true))
        .collect();

    // Act
    let refused = engine
        .register_table(registration(Arc::new(Schema::new(wide)), None))
        .await;
    engine
        .register_table(registration(file_schema("TEMP", "DEPTH"), None))
        .await
        .expect("the table registers");
    engine.refresh("obs").await.expect("a refresh");
    engine.drop_table("obs").await.expect("the statistics drop");
    let again = fixture
        .open(registration(file_schema("TEMP", "DEPTH"), None))
        .await;

    // Assert
    let error = refused.expect_err("too many columns").to_string();
    assert!(error.contains("statistics_columns"), "{error}");
    assert_eq!(rows(&again).await, 0, "the drop deleted the rows");
    assert!(
        engine.refresh("obs").await.is_err(),
        "the table is no longer registered"
    );
}

/// A declared external table `obs` over `memory:///data/` that prunes with statistics.
fn external_definition() -> ExternalTableDefinition {
    ExternalTableDefinition {
        name: "obs".to_string(),
        location: "memory:///data/".to_string(),
        file_type: "parquet".to_string(),
        schema: file_schema("TEMP", "DEPTH"),
        schema_source: SchemaSource::Declared,
        definition: None,
        partition_cols: Vec::new(),
        options: HashMap::new(),
        if_not_exists: false,
        refresh_policy: RefreshPolicy::default(),
        type_widening: Arc::new(DefaultArrowTypeWidening::new()),
        parallel_schema_inference: false,
        skip_invalid_files_on_inference: false,
        skip_invalid_files_on_scan: false,
        use_listing_cache: false,
        use_statistics_pruning: true,
        use_statistics_cache: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_table_prunes_a_file_out_of_range() {
    // Arrange
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    fixture.put("high.parquet", 100.0).await;
    let engine = fixture.engine();
    let table = ExternalTableBuilder::new(external_definition())
        .with_statistics_provider(Arc::new(engine.clone()))
        .build(&fixture.ctx.state())
        .await
        .expect("the table builds");
    engine.refresh("obs").await.expect("a refresh");
    fixture.put("unknown.parquet", 0.0).await;

    // Act
    let state = fixture.ctx.state();
    // `ident` keeps the upper case, which `col` folds away.
    let plan = table
        .scan(&state, None, &[ident("TEMP").gt(lit(50.0))], None)
        .await
        .expect("a plan");
    let batches = collect(Arc::clone(&plan), fixture.ctx.task_ctx())
        .await
        .expect("the scan runs");

    // Assert: the low file cannot match; the unknown file has no statistics yet.
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 6, "the high and the unknown file are read");
    let pruned = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("statistics_files_pruned"))
        .map(|value| value.as_usize());
    assert_eq!(pruned, Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_statistics_table_is_readable_with_sql() {
    // Arrange
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    fixture.put("high.parquet", 100.0).await;
    let engine = fixture.engine();
    engine
        .register_table(registration(file_schema("TEMP", "DEPTH"), None))
        .await
        .expect("the table registers");
    engine.refresh("obs").await.expect("a refresh");
    assert_eq!(engine.tables(), ["obs"]);

    // Act
    let provider = engine.statistics_table("obs").expect("a provider");
    fixture
        .ctx
        .register_table("obs_statistics", provider)
        .expect("registered");
    let batches = fixture
        .ctx
        .sql(r#"SELECT _path, "TEMP"['min'] AS temp_min FROM obs_statistics ORDER BY _path"#)
        .await
        .expect("a plan")
        .collect()
        .await
        .expect("the query runs");

    // Assert
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("a batch");
    let paths = batch.column(0).as_string::<i32>();
    assert_eq!(paths.value(0), "data/high.parquet");
    assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(1), 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requested_refresh_runs_in_the_background() {
    // Arrange: the first refresh is done, then a file arrives.
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    let engine = fixture.engine();
    engine
        .register_table(registration(file_schema("TEMP", "DEPTH"), None))
        .await
        .expect("the table registers");
    engine.refresh("obs").await.expect("a refresh");
    fixture.put("high.parquet", 100.0).await;
    let statistics = engine.statistics("obs").expect("registered");

    // Act: the interval is an hour, so only the request can read the new file.
    engine.request_refresh("obs").expect("a request");

    // Assert
    tokio::time::timeout(Duration::from_secs(10), async {
        while rows(&statistics).await < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the requested refresh reads the new file");
    assert!(engine.request_refresh("nope").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refresh_waits_for_a_free_refresh_slot() {
    // Arrange: one slot, which another table holds now.
    let fixture = Fixture::new();
    fixture.put("low.parquet", 0.0).await;
    let engine = fixture.engine_with(Config::default().with_max_refreshes(1));
    engine
        .register_table(registration(file_schema("TEMP", "DEPTH"), None))
        .await
        .expect("the table registers");
    let slot = engine
        .inner
        .refreshes
        .acquire()
        .await
        .expect("an open semaphore");

    // Act
    let waiting = tokio::spawn({
        let engine = engine.clone();
        async move { engine.refresh("obs").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waited = !waiting.is_finished();
    drop(slot);
    let summary = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("the refresh ends")
        .expect("no panic")
        .expect("a refresh");

    // Assert
    assert!(waited, "the refresh waits while no slot is free");
    assert_eq!(summary.listed, 1);
}
