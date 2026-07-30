//! `Runtime::table_arrow_schema` — the provider-backed replacement for planning
//! `SELECT * FROM t LIMIT 0`.
//!
//! Two things matter and are pinned here. It must return the same schema a scan
//! would, so Flight SQL keeps reporting real Arrow types; and it must apply the
//! *same* read authorization, because a schema is metadata about data the caller
//! may not be allowed to see. The gate is hand-written rather than routed through
//! `authorize_logical_plan` (there is no plan to authorize), so it is asserted
//! against the SQL path rather than assumed to agree with it.

mod common;

use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, ADMIN_PASSWORD, ADMIN_USERNAME};
use datafusion::sql::TableReference;

/// The provider's schema matches what a scan of the same table reports.
#[tokio::test(flavor = "multi_thread")]
async fn returns_the_same_schema_a_scan_would() {
    let rt = restartable_runtime("tas-schema", |b| b).await;
    rt.sql_as(
        "CREATE TABLE obs (a BIGINT, b DOUBLE, c VARCHAR)",
        AuthIdentity::system(),
    )
    .await;
    // A row, so the comparison scan actually emits a batch to read the schema
    // from — `LIMIT 0` produces none, which is part of why it is a poor way to
    // ask a table what its columns are.
    rt.sql_as("INSERT INTO obs VALUES (1, 2.0, 'x')", AuthIdentity::system())
        .await;

    let from_provider = rt
        .runtime
        .table_arrow_schema(TableReference::bare("obs"), &AuthIdentity::system())
        .await
        .expect("schema from the provider");

    let from_scan = rt
        .try_sql_as("SELECT * FROM obs", AuthIdentity::system())
        .await
        .expect("scan");

    assert_eq!(
        from_provider.fields().len(),
        3,
        "all three columns are reported"
    );
    // Same field names and Arrow types, in the same order, as the scan's schema.
    let scan_schema = from_scan
        .first()
        .map(|b| b.schema())
        .expect("at least one batch carries the schema");
    assert_eq!(
        from_provider
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect::<Vec<_>>(),
        scan_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect::<Vec<_>>(),
    );
}

/// A table the caller has no grant on must not leak its schema — the same denial
/// the equivalent `SELECT` gets. This is the whole risk of not going through
/// `authorize_logical_plan`, so both paths are asserted together.
#[tokio::test(flavor = "multi_thread")]
async fn denies_a_table_the_caller_may_not_read() {
    let rt = restartable_runtime("tas-authz", |b| b.with_auth_enforcement(true)).await;

    rt.sql_as("CREATE TABLE granted (a BIGINT)", AuthIdentity::system())
        .await;
    rt.sql_as("CREATE TABLE secret (a BIGINT)", AuthIdentity::system())
        .await;
    rt.sql_as("CREATE ROLE reader", AuthIdentity::system()).await;
    rt.sql_as(
        "CREATE USER alice WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;
    rt.sql_as(
        "GRANT SELECT ON TABLE granted TO ROLE reader",
        AuthIdentity::system(),
    )
    .await;

    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice authenticates");
    assert!(!alice.is_super_user);

    // Granted: both paths succeed.
    rt.runtime
        .table_arrow_schema(TableReference::bare("granted"), &alice)
        .await
        .expect("a granted table's schema is readable");
    rt.try_sql_as("SELECT * FROM granted LIMIT 0", alice.clone())
        .await
        .expect("a granted table scans");

    // Ungranted: both paths deny.
    let schema_denied = rt
        .runtime
        .table_arrow_schema(TableReference::bare("secret"), &alice)
        .await;
    assert!(
        schema_denied.is_err(),
        "an ungranted table must not leak its schema, got: {schema_denied:?}"
    );
    let scan_denied = rt.try_sql_as("SELECT * FROM secret LIMIT 0", alice).await;
    assert!(
        scan_denied.is_err(),
        "sanity: the equivalent scan is denied too"
    );
}

/// The internal `__beacon_*` tables hold password hashes and are super-user-only
/// *unconditionally* — enforcement OFF must not open them, matching the gate in
/// `authorize_logical_plan`. Fails closed.
#[tokio::test(flavor = "multi_thread")]
async fn internal_tables_stay_super_user_only_with_enforcement_off() {
    let rt = restartable_runtime("tas-internal", |b| b.with_auth_enforcement(false)).await;

    rt.sql_as(
        "CREATE USER bob WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    let bob = rt
        .runtime
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .expect("bob authenticates");
    assert!(!bob.is_super_user);

    let denied = rt
        .runtime
        .table_arrow_schema(TableReference::bare("__beacon_users"), &bob)
        .await;
    assert!(
        denied.is_err(),
        "the internal auth table must be denied even with enforcement off, got: {denied:?}"
    );

    // The super-user still reaches it, so the gate is on identity, not existence.
    let admin = rt
        .runtime
        .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
        .await
        .expect("admin authenticates");
    rt.runtime
        .table_arrow_schema(TableReference::bare("__beacon_users"), &admin)
        .await
        .expect("the super-user may read the internal table's schema");
}

/// An unknown table is an error, not an empty schema — callers map that to 404.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_table_errors() {
    let rt = restartable_runtime("tas-missing", |b| b).await;
    let missing = rt
        .runtime
        .table_arrow_schema(TableReference::bare("nope"), &AuthIdentity::system())
        .await;
    assert!(missing.is_err(), "an unknown table must error");
}
