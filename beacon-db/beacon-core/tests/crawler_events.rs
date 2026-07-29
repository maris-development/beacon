//! Verifies the fallback behaviour of an `event_driven` crawler.
//!
//! Event-driven crawling (a crawler that subscribes to storage events and runs the
//! instant a file lands) is **not currently implemented** — the storage-event source
//! went away with `DatasetsStore` (see `docs/design/datasets-store-removal-decisions.md`),
//! and `event_driven` is preserved only as a forward-compatibility placeholder. So an
//! `event_driven` crawler with no explicit schedule subscribes to nothing; the manager
//! runs it at the crawler subsystem's `default_interval_secs` poll instead (see
//! `apply_event_driven_fallback` in `beacon-core/src/crawler/manager.rs`).
//!
//! This test pins that fallback: a runtime configured with a short
//! `default_interval_secs` picks up files dropped under an `event_driven` crawler's
//! prefix on the next poll tick, with no `RUN CRAWLER` and no `WITH ('schedule' ...)`.
//! It is deliberately distinct from `crawler_scheduled.rs`, which exercises an
//! explicit SQL `'schedule'` rather than the event-driven fallback path.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_core::crawler::CrawlerConfig;
use common::{runtime_with, scalar_i64, TestRuntime};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;

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
async fn event_driven_crawler_falls_back_to_default_interval() {
    // A short default poll interval is what an `event_driven` crawler falls back to
    // now that storage events are gone.
    let rt = runtime_with("crawl-events", |b| {
        b.with_crawler(CrawlerConfig {
            enable: true,
            default_interval_secs: 1,
        })
    })
    .await;
    let datasets = rt.datasets_dir().to_path_buf();
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Event-driven crawler with NO schedule: with events removed, the manager falls
    // back to a `default_interval_secs` (1s) poll.
    rt.sql("CREATE CRAWLER ev ON 'ev_src/' WITH ('event_driven' 'true')")
        .await;

    // Drop a file under the watched prefix; the fallback poll must discover it.
    write_parquet(&datasets.join("ev_src/a.parquet"), &schema);

    // Poll for the fallback crawl to register the table. No RUN CRAWLER and no SQL
    // schedule were used, so the table can only appear via the interval fallback.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(1) = try_count(&rt, "ev_src").await {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "event-driven crawler did not fall back to interval polling within 30s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
