//! The `beacon.system` schema: runtime introspection reachable through SQL.
//!
//! These tables replaced `Runtime::get_query_metrics` (and the HTTP routes over
//! it), so the coverage here is what guarantees that information is still
//! obtainable. Functions are not among them: DataFusion's own catalog
//! (`SHOW FUNCTIONS`) is the only one, read through `Runtime::show_functions`.
//!
//! `query_metrics` is the internal managed table `__beacon_query_metrics` under a
//! public name, so the rows a query writes are durable — see
//! `query_metrics_survive_a_restart`.

mod common;

use common::{restartable_runtime, runtime, scalar_i64, total_rows};
use datafusion::arrow::array::{Array, AsArray, StringArray, UInt64Array};

/// The tables are registered and visible through the standard catalog.
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
        vec!["query_metrics", "roles", "users"],
        "the system schema should expose exactly these tables"
    );
}

/// `Runtime::show_functions` reads DataFusion's own function catalog and hands
/// back its rows as Arrow — one row per overload signature, which is what the
/// transports shape into a per-name listing.
#[tokio::test(flavor = "multi_thread")]
async fn show_functions_returns_the_function_catalog() {
    let rt = runtime("system-schema-functions").await;

    let batches = rt
        .runtime
        .show_functions()
        .await
        .expect("the function catalog is readable");

    let mut names = Vec::new();
    let mut described_abs = false;
    for batch in &batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|array| array.as_string_opt::<i32>())
                .unwrap_or_else(|| panic!("SHOW FUNCTIONS should carry a `{name}` column"))
        };
        let (functions, descriptions) = (column("function_name"), column("description"));
        for row in 0..batch.num_rows() {
            let name = functions.value(row);
            names.push(name.to_string());
            if name == "abs" && !descriptions.is_null(row) {
                described_abs = true;
            }
        }
    }

    assert!(names.iter().any(|name| name == "abs"), "got: {names:?}");
    assert!(described_abs, "`abs` carries its description");
    // One row per overload signature: `abs` alone has several.
    assert!(names.iter().filter(|name| *name == "abs").count() > 1);
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

/// The metrics row carries both logical plans — parsed and optimized — because
/// `run_query` registers the plan with the tracker before execution. This is
/// what `EXPLAIN`-style introspection over past queries is built on.
#[tokio::test(flavor = "multi_thread")]
async fn query_metrics_capture_the_logical_plans() {
    let rt = runtime("system-schema-metrics-plans").await;

    rt.sql("CREATE TABLE plan_src (a BIGINT)").await;
    rt.sql("INSERT INTO plan_src VALUES (1)").await;
    rt.sql("SELECT a FROM plan_src WHERE a > 0").await;

    let batches = rt
        .sql(
            "SELECT parsed_logical_plan, optimized_logical_plan \
             FROM beacon.system.query_metrics \
             WHERE query LIKE '%SELECT a FROM plan_src%'",
        )
        .await;
    assert_eq!(total_rows(&batches), 1);

    for (col, name) in [(0, "parsed_logical_plan"), (1, "optimized_logical_plan")] {
        let plan = batches[0]
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("plan columns are Utf8")
            .value(0);
        assert!(
            plan.contains("plan_src"),
            "{name} should render the plan over plan_src, got: {plan:?}"
        );
    }
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

/// The whole schema — not just the auth tables — is super-user-only, and
/// *unconditionally* so: this runtime has grant enforcement off, which is the
/// default and the case where a gate that depended on enforcement would leak.
#[tokio::test(flavor = "multi_thread")]
async fn system_schema_is_super_user_only_even_without_enforcement() {
    let rt = runtime("system-schema-auth-gate").await;

    for sql in [
        "SELECT * FROM beacon.system.users",
        "SELECT * FROM beacon.system.roles",
        // The rest of the schema is no more readable: query_metrics carries the
        // text and plans of what other users ran.
        "SELECT count(*) FROM beacon.system.query_metrics",
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

    // `information_schema` is no more readable than `beacon.system`, and function
    // documentation stays available to every caller through the accessor the
    // transports use, which reads that catalog as the engine.
    assert!(rt
        .try_sql_as(
            "SELECT count(*) FROM information_schema.routines",
            beacon_core::AuthIdentity::empty()
        )
        .await
        .is_err_and(|err| err.to_string().contains("super-user")));
    assert!(!rt
        .runtime
        .show_functions()
        .await
        .expect("the function catalog is readable")
        .is_empty());
}

/// Metrics are persisted, not held in memory: a query recorded before a restart
/// is still readable after one, through both the SQL name and the accessor the
/// transports use.
#[tokio::test(flavor = "multi_thread")]
async fn query_metrics_survive_a_restart() {
    let rt = restartable_runtime("system-schema-metrics-restart", |builder| builder).await;

    rt.sql("CREATE TABLE persisted_src (a BIGINT)").await;
    rt.sql("INSERT INTO persisted_src VALUES (1), (2)").await;
    rt.sql("SELECT a FROM persisted_src").await;

    let batches = rt
        .sql(
            "SELECT query_id, username FROM beacon.system.query_metrics \
             WHERE query LIKE '%SELECT a FROM persisted_src%' \
               AND finished_at IS NOT NULL",
        )
        .await;
    assert_eq!(total_rows(&batches), 1, "the SELECT should be recorded");
    assert_eq!(
        common::column_strings(&batches, 1),
        vec![common::ADMIN_USERNAME],
        "the row names the principal that ran the query"
    );
    let query_id: uuid::Uuid = common::column_strings(&batches, 0)[0]
        .parse()
        .expect("query_id is a uuid");

    let rt = rt.restart().await;

    // The row is still there after the restart. Matched on the id, not the query
    // text: reading the metrics table is itself a query, so a `LIKE` over the text
    // also matches the read that went looking for it.
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!(
                "SELECT count(*) FROM beacon.system.query_metrics WHERE query_id = '{query_id}'"
            ))
            .await,
        ),
        1,
        "recorded metrics should survive a restart"
    );

    // …and reachable by id through the accessor the HTTP layer uses.
    let rows = rt
        .runtime
        .get_query_metrics(query_id)
        .await
        .expect("reading metrics by id succeeds");
    assert_eq!(total_rows(&rows), 1, "the id should resolve to its row");

    // An unknown id resolves to nothing rather than erroring.
    let unknown = rt
        .runtime
        .get_query_metrics(uuid::Uuid::nil())
        .await
        .expect("reading an unknown id succeeds");
    assert_eq!(total_rows(&unknown), 0);
}
