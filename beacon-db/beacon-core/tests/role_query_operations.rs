//! The `query_allow_<operation>` role settings, end to end: a role forbids an
//! operation such as a join, and the runtime refuses the queries of its users
//! that use it. Without the setting, every operation is allowed.

mod common;

use beacon_core::query::Query;
use beacon_core::{AuthIdentity, Credential};
use common::{runtime_with, TestRuntime};

async fn user(rt: &TestRuntime, name: &str, roles: &[&str]) -> AuthIdentity {
    rt.sql(&format!("CREATE USER {name} WITH PASSWORD 'pw'"))
        .await;
    for role in roles {
        rt.sql(&format!("GRANT ROLE {role} TO USER {name}")).await;
    }
    rt.runtime
        .authenticate(&Credential::basic(name, "pw"))
        .await
        .expect("the user should authenticate")
}

async fn assert_refused(rt: &TestRuntime, sql: &str, identity: &AuthIdentity, operation: &str) {
    let error = rt
        .try_sql_as(sql, identity.clone())
        .await
        .expect_err(&format!("`{sql}` should be refused"));
    let message = format!("{error:#}");
    assert!(
        message.contains("operation not permitted") && message.contains(operation),
        "`{sql}`: {message}"
    );
}

async fn assert_allowed(rt: &TestRuntime, sql: &str, identity: &AuthIdentity) {
    rt.try_sql_as(sql, identity.clone())
        .await
        .unwrap_or_else(|error| panic!("`{sql}` should run: {error:#}"));
}

async fn setup(tag: &str) -> TestRuntime {
    let rt = runtime_with(tag, |builder| builder).await;
    for sql in [
        "CREATE TABLE a (k BIGINT, v BIGINT)",
        "CREATE TABLE b (k BIGINT, w BIGINT)",
        "INSERT INTO a VALUES (1, 10), (2, 20)",
        "INSERT INTO b VALUES (1, 100), (2, 200)",
        "CREATE VIEW ab AS SELECT a.k, v, w FROM a JOIN b ON a.k = b.k",
        "CREATE MATERIALIZED VIEW ab_stored AS SELECT a.k, v, w FROM a JOIN b ON a.k = b.k",
        "CREATE ROLE strict",
        "ALTER ROLE strict SET query_allow_join = false",
        "ALTER ROLE strict SET query_allow_order_by = false",
    ] {
        rt.sql(sql).await;
    }
    rt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_forbids_the_operations_it_sets() {
    let rt = setup("role-ops").await;
    let alice = user(&rt, "alice", &["strict"]).await;

    for sql in [
        "SELECT * FROM a JOIN b ON a.k = b.k",
        "SELECT * FROM a, b",
        "SELECT k FROM a INTERSECT SELECT k FROM b",
        "SELECT * FROM ab",
    ] {
        assert_refused(&rt, sql, &alice, "JOIN").await;
    }
    assert_refused(&rt, "SELECT k FROM a ORDER BY v", &alice, "ORDER BY").await;

    for sql in [
        "SELECT count(*) FROM a",
        "SELECT k, count(*) FROM a GROUP BY k",
        "WITH c AS (SELECT k FROM a) SELECT * FROM c",
        "SELECT k FROM a ORDER BY v LIMIT 1",
        "SELECT * FROM ab_stored",
    ] {
        assert_allowed(&rt, sql, &alice).await;
    }

    // `EXPLAIN ANALYZE` runs the plan, so the same rules apply.
    let error = rt
        .runtime
        .explain_analyze_query(
            Query::sql("SELECT * FROM a JOIN b ON a.k = b.k".to_string()),
            alice.clone(),
        )
        .await
        .expect_err("EXPLAIN ANALYZE of a join should be refused");
    assert!(format!("{error:#}").contains("JOIN"), "{error:#}");

    assert_allowed(
        &rt,
        "SELECT * FROM a JOIN b ON a.k = b.k",
        &rt.admin().await,
    )
    .await;
    rt.sql("ALTER ROLE strict RESET query_allow_join").await;
    assert_allowed(&rt, "SELECT * FROM a JOIN b ON a.k = b.k", &alice).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_most_generous_role_wins_and_no_role_forbids_nothing() {
    let rt = setup("role-ops-roles").await;
    rt.sql("CREATE ROLE open").await;
    rt.sql("ALTER ROLE open SET query_allow_join = TRUE").await;
    let both = user(&rt, "both", &["strict", "open"]).await;
    let plain = user(&rt, "plain", &[]).await;
    let join = "SELECT * FROM a JOIN b ON a.k = b.k";

    assert_allowed(&rt, join, &both).await;
    assert_refused(&rt, "SELECT k FROM a ORDER BY v", &both, "ORDER BY").await;
    assert_allowed(&rt, join, &plain).await;

    let error = rt
        .try_sql("ALTER ROLE strict SET query_allow_join = 'no'")
        .await
        .expect_err("the value must be true or false");
    assert!(format!("{error:#}").contains("true or false"), "{error:#}");
}
