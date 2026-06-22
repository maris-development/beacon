//! Regression test: `EXPLAIN ANALYZE` over an external NetCDF table.
//!
//! `EXPLAIN ANALYZE` runs the full plan and then formats it with per-node
//! metrics, aggregating them by name across partitions. The NetCDF reader used
//! to register its batch counter as a generic `counter("output_batches", ..)`,
//! which collides with the typed `MetricValue::OutputBatches` that DataFusion's
//! `FileStream` already records under that reserved name. Aggregating the two
//! different variants panicked, aborting the query mid-stream (over HTTP the
//! response body was dropped). This drives the same path through the runtime to
//! ensure it now completes and returns the annotated plan.

use std::sync::Arc;

use beacon_core::runtime::Runtime;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

async fn collect(runtime: &Runtime, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(runtime
        .run_query(beacon_core::query::Query::sql(sql.to_string()), true)
        .await?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_over_external_netcdf_returns_metrics() {
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    // Copy the WOD CTD fixture into the datasets dir so it can back an external
    // table addressed by a datasets-relative location.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc");
    let rel = format!("explain_analyze_external_{}.nc", std::process::id());
    let dst = beacon_config::DATASETS_DIR_PATH.join(&rel);
    std::fs::copy(&src, &dst).expect("copy NetCDF fixture into datasets dir");

    let table = format!("wod_explain_{}", std::process::id());
    let _ = collect(&runtime, &format!("DROP TABLE IF EXISTS {table}")).await;
    collect(
        &runtime,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS NC LOCATION '{rel}'"),
    )
    .await
    .expect("create external NetCDF table");

    // Sanity: a plain scan works (this never panicked — only the analyze path did).
    collect(&runtime, &format!("SELECT * FROM {table} LIMIT 3"))
        .await
        .expect("plain SELECT over the external table should work");

    // The regression: this used to panic in metric aggregation and abort.
    let batches = collect(
        &runtime,
        &format!("EXPLAIN ANALYZE SELECT * FROM {table} LIMIT 10"),
    )
    .await
    .expect("EXPLAIN ANALYZE over the external table should complete");

    // It should yield the annotated plan ("plan_type", "plan"): one row whose
    // plan text carries the collected metrics.
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "EXPLAIN ANALYZE should return a single plan row");
    let plan_text = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("plan column should be Utf8")
        .value(0)
        .to_string();
    assert!(
        plan_text.contains("metrics=") && plan_text.contains("output_batches"),
        "annotated plan should include collected metrics, got:\n{plan_text}"
    );

    // Best-effort cleanup so the shared on-disk tables store does not leak.
    let _ = collect(&runtime, &format!("DROP TABLE {table}")).await;
    let _ = std::fs::remove_file(&dst);
}
