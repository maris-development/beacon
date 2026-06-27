//! Verifies the crawler's **event-driven** trigger: with filesystem events
//! enabled, dropping a new file under the watched prefix triggers an incremental
//! crawl (debounced) with no `RUN CRAWLER` and no schedule.
//!
//! In its own test binary because it needs `BEACON_ENABLE_FS_EVENTS=true`, which
//! the other crawler integration tests turn off.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;

async fn run(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(Query::sql(sql.to_string()), beacon_core::AuthIdentity::system())
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .unwrap_or_else(|e| panic!("expected stream: {sql}\n{e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("stream failed: {sql}\n{e}"))
}

async fn try_count(runtime: &Runtime, table: &str) -> Option<i64> {
    let result = runtime
        .run_query(Query::sql(format!("SELECT count(*) FROM {table}")), beacon_core::AuthIdentity::system())
        .await
        .ok()?;
    let batches: Vec<RecordBatch> = result.into_record_stream().ok()?.try_collect().await.ok()?;
    Some(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0),
    )
}

fn write_parquet(path: &Path, schema: &Arc<Schema>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn event_driven_crawler_runs_on_new_files() {
    let tmp = std::env::temp_dir().join(format!("beacon_crawl_events_{}", std::process::id()));
    std::env::set_var("BEACON_DATA_DIR", &tmp);
    std::env::set_var("BEACON_ENABLE_FS_EVENTS", "true"); // events must be able to fire
    std::env::set_var("BEACON_FLIGHT_SQL_ENABLE", "false");

    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");
    let datasets = runtime.config().storage.datasets_dir.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Event-driven crawler with NO schedule: it only acts on storage events.
    run(
        &runtime,
        "CREATE CRAWLER ev ON 'ev_src/' WITH ('event_driven' 'true')",
    )
    .await;

    // Drop a file under the watched prefix -> FS event -> debounce -> incremental crawl.
    write_parquet(&datasets.join("ev_src/a.parquet"), &schema);

    // Poll for the event-triggered crawl to register the table (debounce is ~2s,
    // plus FS-event latency). No RUN CRAWLER and no schedule were used, so the
    // table can only appear because the storage event drove the crawl.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(1) = try_count(&runtime, "ev_src").await {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "event-driven crawler did not register the table within 30s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
