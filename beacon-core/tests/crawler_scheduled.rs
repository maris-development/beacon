//! Verifies the crawler's **scheduled** trigger: a crawler defined with a short
//! interval re-crawls on its own (no `RUN CRAWLER`) and picks up newly added data.
//!
//! In its own test binary so its process-global env (`BEACON_*`) is isolated from
//! the other crawler integration tests.

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
        .run_query(
            Query::sql(sql.to_string()),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .unwrap_or_else(|e| panic!("expected stream: {sql}\n{e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("stream failed: {sql}\n{e}"))
}

/// Try `SELECT count(*) FROM <table>`; returns `None` while the table does not
/// exist yet (so we can poll for the scheduled crawl to register it).
async fn try_count(runtime: &Runtime, table: &str) -> Option<i64> {
    let result = runtime
        .run_query(
            Query::sql(format!("SELECT count(*) FROM {table}")),
            beacon_core::AuthIdentity::system(),
        )
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
async fn scheduled_crawler_runs_without_manual_trigger() {
    let tmp = std::env::temp_dir().join(format!("beacon_crawl_sched_{}", std::process::id()));
    std::env::set_var("BEACON_DATA_DIR", &tmp);
    std::env::set_var("BEACON_ENABLE_FS_EVENTS", "false");
    std::env::set_var("BEACON_FLIGHT_SQL_ENABLE", "false");

    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");
    let datasets = runtime.config().storage.datasets_dir.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Define a crawler with a 1s schedule but DO NOT run it manually.
    run(
        &runtime,
        "CREATE CRAWLER sched ON 'sched_src/' WITH ('schedule' '1s')",
    )
    .await;

    // Now drop data in — a subsequent scheduled tick must discover and register it.
    write_parquet(&datasets.join("sched_src/a.parquet"), &schema);
    write_parquet(&datasets.join("sched_src/b.parquet"), &schema);

    // Poll for the scheduled crawl to register the table (no RUN CRAWLER issued).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(2) = try_count(&runtime, "sched_src").await {
            break; // scheduled crawl picked up both files
        }
        assert!(
            Instant::now() < deadline,
            "scheduled crawler did not register the table within 20s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
