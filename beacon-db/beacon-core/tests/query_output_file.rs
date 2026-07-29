//! Applying an output format to a query writes the result to a temporary file and
//! returns it as a file download (rather than a row stream).
//!
//! This drives `run_query_to_file` -> `Output::parse` -> `QueryOutputFile` end to
//! end — the temp-output-file path — which no other test currently exercises.

mod common;

use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use common::runtime;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn query_with_csv_output_produces_a_nonempty_file() {
    let rt = runtime("query-output-file").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    // SQL query body plus an output format => the runtime wraps it in a `COPY TO`
    // a temp file and hands back the file rather than a stream.
    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "csv" })).expect("valid output"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("query with an output format should run");

    match result.query_output {
        QueryOutput::File(file) => {
            assert!(
                file.path().exists(),
                "the output file should exist on disk while the result is held"
            );
            assert!(
                file.size().expect("output file size") > 0,
                "the CSV output file should contain the exported rows"
            );
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}

/// NetCDF output goes through a custom `DataSink` that opens a real local file
/// natively (netcdf-c cannot stream through an object store). This asserts the
/// native writer lands on the `TempObject`'s path — under the configured tmp store
/// root, not the OS temp dir — and is non-empty. It is the regression net for the
/// three-way `tmp_dir` coupling the redesign made explicit: the COPY target
/// `tmp://<name>` and the sink's `output_dir.join(<name>)` must resolve to the same
/// file `QueryOutputFile` reads back.
#[tokio::test(flavor = "multi_thread")]
async fn query_with_netcdf_output_writes_under_configured_tmp() {
    let rt = runtime("query-output-file-netcdf").await;
    rt.sql("CREATE TABLE t (a BIGINT, b DOUBLE)").await;
    rt.sql("INSERT INTO t VALUES (1, 1.5), (2, 2.5)").await;

    let mut query = Query::sql("SELECT a, b FROM t".to_string());
    query.output =
        Some(serde_json::from_value(json!({ "format": "netcdf" })).expect("valid output"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("netcdf query with an output format should run");

    match result.query_output {
        QueryOutput::File(file) => {
            assert!(
                file.path().exists(),
                "the netcdf output file should exist on disk while the result is held"
            );
            // The native sink must land the file under the configured tmp store
            // root (canonicalized: tempfile roots resolve through symlinks on macOS).
            let got = std::fs::canonicalize(file.path().parent().expect("output has a parent"))
                .expect("canonicalize output parent");
            let want = std::fs::canonicalize(rt.tmp_dir()).expect("canonicalize tmp dir");
            assert_eq!(
                got, want,
                "netcdf output should be written under the configured tmp dir"
            );
            assert!(
                file.size().expect("output file size") > 0,
                "the netcdf output file should contain the exported rows"
            );
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}
