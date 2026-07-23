//! `Runtime::run_query` rejects an output format on a statement that produces no
//! result set (DDL/DML/`SET`) with a clear error, instead of letting the
//! `COPY TO` wrapping fail with a cryptic planner error — while still honoring an
//! output format on a row-producing `SELECT`.

mod common;

use beacon_core::query::output::{Output, OutputFormat};
use beacon_core::query::{InnerQuery, Query};
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime::Runtime;
use common::TestRuntime;

fn csv_query(sql: &str) -> Query {
    Query {
        inner: InnerQuery::Sql(sql.to_string()),
        output: Some(Output {
            format: OutputFormat::Csv,
        }),
    }
}

/// Run a CSV-output query expecting it to fail, returning the error message.
/// (`QueryResult` is not `Debug`, so `Result::expect_err` is unavailable.)
async fn expect_output_error(runtime: &Runtime, sql: &str) -> String {
    match runtime
        .run_query(csv_query(sql), beacon_core::AuthIdentity::system())
        .await
    {
        Ok(_) => panic!("expected an error for: {sql}"),
        Err(error) => error.to_string(),
    }
}

/// A DDL/DML/administrative statement with an output format fails with the clear,
/// actionable message rather than a `COPY TO` planner error.
#[tokio::test(flavor = "multi_thread")]
async fn output_on_non_row_producing_statement_is_a_clear_error() {
    let rt: TestRuntime = common::runtime("output-guard").await;
    let runtime = &rt.runtime;
    let table = format!("output_guard_{}", std::process::id());
    let _ = runtime
        .run_query(
            Query::sql(format!("DROP TABLE IF EXISTS {table}")),
            beacon_core::AuthIdentity::system(),
        )
        .await;

    // CREATE TABLE (DDL) and SET (statement) both return no rows.
    let cases = [
        format!("CREATE TABLE {table} (id BIGINT)"),
        "SET datafusion.execution.batch_size = 4096".to_string(),
    ];
    for sql in cases {
        let message = expect_output_error(runtime, &sql).await;
        assert!(
            message.contains("output format can only be applied to queries that return rows"),
            "unexpected error for `{sql}`: {message}"
        );
    }

    // INSERT, after the table exists, is also rejected (DML produces only a count).
    runtime
        .run_query(
            Query::sql(format!("CREATE TABLE {table} (id BIGINT)")),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("create table should succeed without output");
    let message = expect_output_error(runtime, &format!("INSERT INTO {table} VALUES (1)")).await;
    assert!(
        message.contains("output format can only be applied to queries that return rows"),
        "unexpected error for INSERT: {message}"
    );

    let _ = runtime
        .run_query(
            Query::sql(format!("DROP TABLE IF EXISTS {table}")),
            beacon_core::AuthIdentity::system(),
        )
        .await;
}

/// A row-producing `SELECT` with an output format still succeeds and yields a file.
#[tokio::test(flavor = "multi_thread")]
async fn output_on_select_still_succeeds() {
    let rt: TestRuntime = common::runtime("output-select").await;
    let result = rt
        .runtime
        .run_query(
            csv_query("SELECT 1 AS a"),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("SELECT with output should succeed");
    assert!(
        matches!(result.query_output, QueryOutput::File(_)),
        "expected a file output for a SELECT with an output format"
    );
}
