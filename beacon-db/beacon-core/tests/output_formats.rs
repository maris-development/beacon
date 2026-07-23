//! Output formats beyond CSV/NetCDF (which `query_output_file` covers): the
//! result-file path for Parquet and Arrow IPC, verified down to the file magic
//! so a mislabeled writer cannot pass.

mod common;

use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use common::runtime;
use serde_json::json;

/// Runs `SELECT a FROM t` with the given output format and returns the bytes of
/// the produced file.
async fn output_bytes(tag: &str, output: serde_json::Value) -> Vec<u8> {
    let rt = runtime(tag).await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2), (3)").await;

    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output = Some(serde_json::from_value(output).expect("valid output spec"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("query with an output format should run");

    match result.query_output {
        QueryOutput::File(file) => {
            std::fs::read(file.path()).expect("the output file should be readable")
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_output_is_a_real_parquet_file() {
    let bytes = output_bytes("output-parquet", json!({ "format": "parquet" })).await;
    assert!(
        bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"),
        "parquet files start and end with the PAR1 magic"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_output_is_a_real_ipc_file() {
    // "arrow" is the serde alias for the IPC variant — the name clients use.
    let bytes = output_bytes("output-arrow", json!({ "format": "arrow" })).await;
    assert!(
        bytes.starts_with(b"ARROW1"),
        "IPC files start with the ARROW1 magic"
    );
}
