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

mod common;

use common::{runtime, total_rows};
use datafusion::arrow::array::{Array, StringArray};

/// The WOD CTD fixture shipped with the NetCDF reader.
fn netcdf_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc")
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_over_external_netcdf_returns_metrics() {
    let rt = runtime("explain-analyze-nc").await;

    // Copy the WOD CTD fixture into the datasets dir so it can back an external
    // table addressed by a datasets-relative location.
    let rel = "explain_analyze_external.nc";
    std::fs::copy(netcdf_fixture(), rt.datasets_dir().join(rel))
        .expect("copy NetCDF fixture into datasets dir");

    let table = "wod_explain";
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS NC LOCATION '{rel}'"
    ))
    .await;

    // Sanity: a plain scan works (this never panicked — only the analyze path did).
    rt.sql(&format!("SELECT * FROM {table} LIMIT 3")).await;

    // The regression: this used to panic in metric aggregation and abort.
    let batches = rt
        .sql(&format!("EXPLAIN ANALYZE SELECT * FROM {table} LIMIT 10"))
        .await;

    // It should yield the annotated plan ("plan_type", "plan"): one row whose
    // plan text carries the collected metrics.
    assert_eq!(
        total_rows(&batches),
        1,
        "EXPLAIN ANALYZE should return a single plan row"
    );
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
}
