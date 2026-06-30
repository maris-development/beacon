//! Tests for `explain_analyze_client_query`: running a query and returning its
//! physical plan as pgjson annotated with per-node runtime metrics.

use std::sync::Arc;

use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use futures::TryStreamExt;

async fn run_sql(runtime: &Runtime, sql: &str) {
    runtime
        .run_query(Query::sql(sql.to_string()), beacon_core::AuthIdentity::system())
        .await
        .expect("sql should run")
        .into_record_stream()
        .expect("streamed result")
        .try_collect::<Vec<_>>()
        .await
        .expect("sql should drain");
}

fn sql_query(sql: &str) -> Query {
    Query::sql(sql.to_string())
}

/// `explain_analyze_client_query` returns the pgjson "Plan with Metrics" shape:
/// a single-element array wrapping a `Plan` node that carries the operator name,
/// actual row count, and a nested `Plans` array of children.
#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_returns_pg_json_with_metrics() {
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    let suffix = uuid::Uuid::new_v4().simple();
    let table = format!("explain_analyze_pgjson_{suffix}");
    run_sql(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
    run_sql(&runtime, &format!("INSERT INTO {table} VALUES (1), (2), (3)")).await;

    let json_str = runtime
        .explain_analyze_client_query(sql_query(&format!("SELECT a FROM {table}")), beacon_core::AuthIdentity::empty())
        .await
        .expect("explain analyze should succeed");

    let value: serde_json::Value =
        serde_json::from_str(&json_str).expect("output should be valid JSON");

    // Top level is a one-element array `[{ "Plan": { ... } }]`.
    let plan = value
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("Plan"))
        .expect("output should be [{ \"Plan\": ... }]");

    assert!(
        plan.get("Node Type").and_then(|v| v.as_str()).is_some(),
        "root plan node should carry a `Node Type`, got: {plan}"
    );
    assert!(
        plan.get("Plans").and_then(|v| v.as_array()).is_some(),
        "root plan node should carry a `Plans` array, got: {plan}"
    );

    // Some node in the tree should report `Actual Rows` (the scan/projection that
    // produced the three inserted rows).
    fn has_actual_rows(node: &serde_json::Value) -> bool {
        node.get("Actual Rows").is_some()
            || node
                .get("Plans")
                .and_then(|v| v.as_array())
                .is_some_and(|children| children.iter().any(has_actual_rows))
    }
    assert!(
        has_actual_rows(plan),
        "annotated plan should include `Actual Rows` metrics, got:\n{json_str}"
    );

    let _ = run_sql(&runtime, &format!("DROP TABLE {table}")).await;
}

/// The endpoint honors admin vs anonymous: an anonymous (non-super-user) call
/// rejects DDL before execution, while a super-user call is permitted — mirroring
/// the `/api/query` permission gate.
#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_gates_ddl_by_privilege() {
    let runtime = Runtime::new(Arc::new(beacon_config::Config::load().unwrap()))
        .await
        .expect("runtime should boot");

    let suffix = uuid::Uuid::new_v4().simple();
    let table = format!("explain_analyze_ddl_{suffix}");

    // Anonymous (read-only): standard DDL is gated by DataFusion's `verify_plan`,
    // which rejects it before any execution with a "DDL not supported" error.
    let err = runtime
        .explain_analyze_client_query(
            sql_query(&format!("CREATE TABLE {table} (a BIGINT)")),
            beacon_core::AuthIdentity::empty(),
        )
        .await
        .err()
        .expect("anonymous explain analyze of DDL should be rejected");
    assert!(
        err.to_string().contains("DDL not supported"),
        "unexpected error: {err}"
    );

    // Super-user: the same DDL passes the gate and is analyzed (and executed).
    runtime
        .explain_analyze_client_query(
            sql_query(&format!("CREATE TABLE {table} (a BIGINT)")),
            beacon_core::AuthIdentity::system(),
        )
        .await
        .expect("super-user explain analyze of DDL should be permitted");

    let _ = run_sql(&runtime, &format!("DROP TABLE {table}")).await;
}
