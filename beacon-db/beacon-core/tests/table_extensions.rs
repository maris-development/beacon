//! `Runtime::table_extensions`: the extensions of a table for a caller who may
//! read it.
//!
//! `SHOW EXTENSIONS` is super-user only, so a transport that serves a regular
//! caller (the MCP server) reads the extensions here. The gate must match the one
//! of the table's schema: a reader of the table gets them, anyone else does not.

mod common;

use beacon_core::{AuthIdentity, Credential};
use common::restartable_runtime;
use datafusion::sql::TableReference;

const MCP: &str = r#"{"enabled": true, "tool_name": "query_granted"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_gets_the_extensions_of_a_readable_table_only() {
    let rt = restartable_runtime("tex-authz", |b| b.with_auth_enforcement(true)).await;
    for sql in [
        "CREATE TABLE granted (a BIGINT)",
        "CREATE TABLE secret (a BIGINT)",
        &format!("SET EXTENSION 'mcp' FOR granted TO '{MCP}'"),
        &format!("SET EXTENSION 'mcp' FOR secret TO '{MCP}'"),
        "CREATE ROLE reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE reader TO USER alice",
        "GRANT SELECT ON TABLE granted TO ROLE reader",
    ] {
        rt.sql_as(sql, AuthIdentity::system()).await;
    }
    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice authenticates");

    let granted = rt
        .runtime
        .table_extensions(TableReference::bare("granted"), &alice)
        .await
        .expect("a readable table's extensions");
    let secret = rt
        .runtime
        .table_extensions(TableReference::bare("secret"), &alice)
        .await;
    let show = rt.try_sql_as("SHOW EXTENSIONS FOR granted", alice).await;

    let mcp = granted.mcp.expect("the mcp extension");
    assert!(mcp.enabled);
    assert_eq!(mcp.tool_name.as_deref(), Some("query_granted"));
    assert!(
        secret.is_err(),
        "an ungranted table must not leak its extensions"
    );
    assert!(show.is_err(), "SHOW EXTENSIONS stays super-user only");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_without_extensions_gives_an_empty_set() {
    let rt = restartable_runtime("tex-empty", |b| b).await;
    rt.sql_as("CREATE TABLE plain (a BIGINT)", AuthIdentity::system())
        .await;

    let extensions = rt
        .runtime
        .table_extensions(TableReference::bare("plain"), &AuthIdentity::empty())
        .await
        .expect("extensions of a readable table");

    assert!(extensions.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_table_errors() {
    let rt = restartable_runtime("tex-missing", |b| b).await;

    let missing = rt
        .runtime
        .table_extensions(TableReference::bare("nope"), &AuthIdentity::system())
        .await;

    assert!(missing.is_err());
}
