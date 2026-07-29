//! Verifies the crawler's **scheduled** trigger: a crawler defined with a short
//! interval re-crawls on its own (no `RUN CRAWLER`) and picks up newly added data.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{runtime, scalar_i64, TestRuntime};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;

/// Try `SELECT count(*) FROM <table>`; returns `None` while the table does not
/// exist yet (so we can poll for the scheduled crawl to register it).
async fn try_count(rt: &TestRuntime, table: &str) -> Option<i64> {
    let identity = rt.admin().await;
    let batches = rt
        .try_sql_as(&format!("SELECT count(*) FROM {table}"), identity)
        .await
        .ok()?;
    Some(scalar_i64(&batches))
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
    // The default runtime already has the crawler subsystem enabled, so a crawler
    // defined with a schedule spawns its background tick.
    let rt = runtime("crawl-sched").await;
    let datasets = rt.datasets_dir().to_path_buf();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Define a crawler with a 1s schedule but DO NOT run it manually.
    rt.sql("CREATE CRAWLER sched ON 'sched_src/' WITH ('schedule' '1s')")
        .await;

    // Now drop data in — a subsequent scheduled tick must discover and register it.
    write_parquet(&datasets.join("sched_src/a.parquet"), &schema);
    write_parquet(&datasets.join("sched_src/b.parquet"), &schema);

    // Poll for the scheduled crawl to register the table (no RUN CRAWLER issued).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(2) = try_count(&rt, "sched_src").await {
            break; // scheduled crawl picked up both files
        }
        assert!(
            Instant::now() < deadline,
            "scheduled crawler did not register the table within 20s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
