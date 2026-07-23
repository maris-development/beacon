//! The `beacon.system` schema: runtime introspection reachable through SQL.
//!
//! These tables replaced `Runtime::list_functions`, `Runtime::list_table_functions`
//! and `Runtime::get_query_metrics` (and the HTTP routes over them), so the
//! coverage here is what guarantees that information is still obtainable.

mod common;

use common::{runtime, scalar_i64, total_rows};
use datafusion::arrow::array::{Array, StringArray, UInt64Array};

/// The three tables are registered and visible through the standard catalog.
#[tokio::test(flavor = "multi_thread")]
async fn system_tables_are_listed_in_information_schema() {
    let rt = runtime("system-schema-listing").await;

    let batches = rt
        .sql(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'system' ORDER BY table_name",
        )
        .await;

    let names: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("table_name is Utf8");
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(
        names,
        vec![
            "functions",
            "query_metrics",
            "roles",
            "table_functions",
            "users"
        ],
        "the system schema should expose exactly these tables"
    );
}

/// `beacon.system.functions` lists the session's scalar functions with their docs.
#[tokio::test(flavor = "multi_thread")]
async fn functions_table_lists_scalar_functions() {
    let rt = runtime("system-schema-functions").await;

    let count = scalar_i64(
        &rt.sql("SELECT count(*) FROM beacon.system.functions")
            .await,
    );
    assert!(
        count > 0,
        "the functions table should list the registered scalar functions"
    );

    // A function every DataFusion session has, with its documented return type.
    let batches = rt
        .sql(
            "SELECT return_type, parameters FROM beacon.system.functions \
             WHERE function_name = 'abs'",
        )
        .await;
    assert_eq!(total_rows(&batches), 1, "`abs` should be listed once");

    // `parameters` is a JSON array, not an opaque debug string.
    let parameters = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("parameters is Utf8")
        .value(0);
    serde_json::from_str::<serde_json::Value>(parameters)
        .expect("parameters should be valid JSON")
        .as_array()
        .expect("parameters should be a JSON array");
}

/// `beacon.system.table_functions` lists the table-valued functions — the listing
/// that has no `information_schema` equivalent, which is why this table exists.
#[tokio::test(flavor = "multi_thread")]
async fn table_functions_table_lists_read_functions() {
    let rt = runtime("system-schema-table-functions").await;

    let count = scalar_i64(
        &rt.sql(
            "SELECT count(*) FROM beacon.system.table_functions \
             WHERE function_name = 'read_parquet'",
        )
        .await,
    );
    assert_eq!(count, 1, "`read_parquet` should be listed once");

    // Every row reports TABLE as its return type.
    let non_table = scalar_i64(
        &rt.sql(
            "SELECT count(*) FROM beacon.system.table_functions WHERE return_type <> 'TABLE'",
        )
        .await,
    );
    assert_eq!(non_table, 0, "table functions should all return TABLE");
}

/// A completed query is observable in `beacon.system.query_metrics`: its row
/// carries the output row count and a populated physical-plan metric tree.
#[tokio::test(flavor = "multi_thread")]
async fn query_metrics_table_records_completed_queries() {
    let rt = runtime("system-schema-query-metrics").await;

    let table = "metrics_src";
    rt.sql(&format!("CREATE TABLE {table} (a BIGINT)")).await;
    rt.sql(&format!("INSERT INTO {table} VALUES (1), (2), (3)"))
        .await;

    // The metrics for a query are consolidated when its stream ends, so this
    // SELECT is fully drained by `rt.sql` before the next statement runs.
    rt.sql(&format!("SELECT a FROM {table}")).await;

    let batches = rt
        .sql(
            "SELECT result_num_rows, node_metrics, query FROM beacon.system.query_metrics \
             WHERE query LIKE '%SELECT a FROM metrics_src%'",
        )
        .await;
    assert_eq!(
        total_rows(&batches),
        1,
        "the completed SELECT should have exactly one metrics row"
    );

    let result_num_rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("result_num_rows is UInt64")
        .value(0);
    assert_eq!(
        result_num_rows, 3,
        "the recorded output row count should match the rows the query returned"
    );

    // `node_metrics` is the physical plan's metric tree. It is only non-empty
    // because `run_query` registers the physical plan with the tracker.
    let node_metrics = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("node_metrics is Utf8")
        .value(0);
    let tree: serde_json::Value =
        serde_json::from_str(node_metrics).expect("node_metrics should be valid JSON");
    assert!(
        tree.get("operator").and_then(|v| v.as_str()).is_some(),
        "node_metrics should carry the root operator, got: {tree}"
    );
}

/// The system schema is read-only: it rejects `CREATE TABLE` into it.
#[tokio::test(flavor = "multi_thread")]
async fn system_schema_rejects_writes() {
    let rt = runtime("system-schema-read-only").await;

    let err = rt
        .try_sql("CREATE TABLE beacon.system.intruder (a BIGINT)")
        .await
        .err()
        .expect("creating a table in the system schema should fail");
    assert!(
        err.to_string().contains("read-only") || err.to_string().contains("system"),
        "unexpected error: {err}"
    );
}

/// The auth directory is readable as SQL by the super-user, completing the set:
/// `CREATE USER` / `CREATE ROLE` / `GRANT` were already SQL, and now so is
/// reading back what they did.
#[tokio::test(flavor = "multi_thread")]
async fn auth_tables_expose_the_directory_to_the_super_user() {
    let rt = runtime("system-schema-auth").await;

    rt.sql("CREATE ROLE analyst").await;
    rt.sql("CREATE USER alice WITH PASSWORD 'pw'").await;
    rt.sql("GRANT ROLE analyst TO USER alice").await;

    assert_eq!(
        scalar_i64(
            &rt.sql("SELECT count(*) FROM beacon.system.users WHERE username = 'alice'")
                .await
        ),
        1,
        "a user created through SQL should be listed"
    );
    assert_eq!(
        scalar_i64(
            &rt.sql("SELECT count(*) FROM beacon.system.roles WHERE role_name = 'analyst'")
                .await
        ),
        1,
        "a role created through SQL should be listed"
    );

    let batches = rt
        .sql("SELECT username, roles FROM beacon.system.users WHERE username = 'alice'")
        .await;
    let roles = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("roles is Utf8")
        .value(0);
    let roles: Vec<String> =
        serde_json::from_str(roles).expect("roles should be a JSON array");
    assert!(
        roles.iter().any(|r| r == "analyst"),
        "alice's granted role should be listed, got: {roles:?}"
    );

    // Roles render their rules as JSON, and no password material is present
    // anywhere in the users table.
    let dumped = format!("{:?}", rt.sql("SELECT * FROM beacon.system.users").await);
    assert!(
        !dumped.contains("$argon2"),
        "the users table must never expose password hashes"
    );
}

/// The auth tables are super-user-only *unconditionally* — this runtime has
/// grant enforcement off, which is the default and the case where a gate that
/// depended on enforcement would leak the directory.
#[tokio::test(flavor = "multi_thread")]
async fn auth_tables_are_super_user_only_even_without_enforcement() {
    let rt = runtime("system-schema-auth-gate").await;

    for sql in [
        "SELECT * FROM beacon.system.users",
        "SELECT * FROM beacon.system.roles",
        // Reached indirectly: the gate matches the scan, not the statement shape.
        "SELECT count(*) FROM (SELECT * FROM beacon.system.users)",
    ] {
        let err = rt
            .try_sql_as(sql, beacon_core::AuthIdentity::empty())
            .await
            .err()
            .unwrap_or_else(|| panic!("non-super read should be rejected: {sql}"));
        assert!(
            err.to_string().contains("super-user"),
            "expected a super-user error for `{sql}`, got: {err}"
        );
    }

    // The non-auth system tables stay readable — the gate is scoped, not blanket.
    rt.sql_as(
        "SELECT count(*) FROM beacon.system.functions",
        beacon_core::AuthIdentity::empty(),
    )
    .await;
}
