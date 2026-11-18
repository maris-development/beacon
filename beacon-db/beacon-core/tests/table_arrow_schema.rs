//! `Runtime::table_arrow_schema` — the provider-backed replacement for planning
//! `SELECT * FROM t LIMIT 0`.
//!
//! Two things matter and are pinned here. It must return the same schema a scan
//! would, so Flight SQL keeps reporting real Arrow types; and it must ask the
//! validator of the caller, because a schema is metadata about data the caller
//! may not be allowed to see. There is no plan to validate, so the check is
//! `PlanValidator::check_table`, and it is asserted against the SQL path.

mod common;

use beacon_core::plan_validator::AllowAll;
use common::{restartable_runtime, Unprivileged};
use datafusion::sql::TableReference;

/// The provider's schema matches what a scan of the same table reports.
#[tokio::test(flavor = "multi_thread")]
async fn returns_the_same_schema_a_scan_would() {
    let rt = restartable_runtime("tas-schema", |b| b).await;
    rt.sql_as(
        "CREATE TABLE obs (a BIGINT, b DOUBLE, c VARCHAR)",
        beacon_core::plan_validator::allow_all(),
    )
    .await;
    // A row, so the comparison scan actually emits a batch to read the schema
    // from — `LIMIT 0` produces none, which is part of why it is a poor way to
    // ask a table what its columns are.
    rt.sql_as("INSERT INTO obs VALUES (1, 2.0, 'x')", beacon_core::plan_validator::allow_all())
        .await;

    let from_provider = rt
        .runtime
        .table_arrow_schema(TableReference::bare("obs"), &beacon_core::plan_validator::AllowAll)
        .await
        .expect("schema from the provider");

    let from_scan = rt
        .try_sql_as("SELECT * FROM obs", beacon_core::plan_validator::allow_all())
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

/// A table that the validator refuses must not leak its schema — the same refusal the
/// equivalent `SELECT` gets.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_table_gives_no_schema() {
    let rt = restartable_runtime("tas-refused", |b| b).await;
    rt.sql("CREATE TABLE obs (a BIGINT)").await;
    let metadata = || TableReference::partial("information_schema", "tables");

    // Permitted: both paths succeed.
    rt.runtime
        .table_arrow_schema(TableReference::bare("obs"), &Unprivileged)
        .await
        .expect("the schema of a permitted table is readable");
    rt.try_sql_as("SELECT * FROM obs LIMIT 0", common::unprivileged())
        .await
        .expect("a permitted table scans");

    // Refused: both paths refuse.
    let schema_refused = rt
        .runtime
        .table_arrow_schema(metadata(), &Unprivileged)
        .await;
    assert!(
        schema_refused.is_err(),
        "a refused table must not leak its schema, got: {schema_refused:?}"
    );
    let scan_refused = rt
        .try_sql_as(
            "SELECT * FROM information_schema.tables LIMIT 0",
            common::unprivileged(),
        )
        .await;
    assert!(scan_refused.is_err(), "the equivalent scan is refused too");

    // The refusal comes from the validator, not from the table.
    rt.runtime
        .table_arrow_schema(metadata(), &AllowAll)
        .await
        .expect("full access reads the schema");
}

/// An unknown table is an error, not an empty schema — callers map that to 404.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_table_errors() {
    let rt = restartable_runtime("tas-missing", |b| b).await;
    let missing = rt
        .runtime
        .table_arrow_schema(TableReference::bare("nope"), &beacon_core::plan_validator::AllowAll)
        .await;
    assert!(missing.is_err(), "an unknown table must error");
}
