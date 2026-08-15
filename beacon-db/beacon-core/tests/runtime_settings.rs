//! `SET` / `RESET` / `ALTER SYSTEM` / `SHOW SETTINGS` end to end, through a real
//! runtime.
//!
//! The unit tests cover the pieces (the namespace round-trips, the AST rewrite
//! resolves a key). These prove the pieces are joined: a `SET` reaches the shared
//! session, a later statement sees it, an `ALTER SYSTEM SET` survives a restart,
//! and the privilege boundary holds.
//!
//! Every assertion reads a value back through SQL rather than through the
//! `Runtime`, which exposes no config getter on purpose — and reading a setting
//! from a SQL client is the feature under test.

mod common;

use arrow::array::{Array as _, StringArray};
use arrow::record_batch::RecordBatch;
use beacon_core::AuthIdentity;
use common::TestRuntime;

/// The single string in a one-row, one-column result.
fn scalar_str(batches: &[RecordBatch]) -> String {
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a string column");
    assert_eq!(column.len(), 1, "expected exactly one row");
    column.value(0).to_string()
}

/// Every value in the first column, across batches.
fn column_strings(batches: &[RecordBatch], index: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a string column");
            (0..column.len())
                .map(|row| column.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The value a setting currently holds, read the way a client would.
async fn setting(rt: &TestRuntime, name: &str) -> String {
    let rows = rt
        .sql(&format!(
            "SELECT value FROM information_schema.df_settings WHERE name = '{name}'"
        ))
        .await;
    scalar_str(&rows)
}

/// A non-super-user identity. Beacon's super-user is a single configured
/// credential, so any other authenticated principal is non-super by construction.
fn regular_user() -> AuthIdentity {
    AuthIdentity {
        username: "reader".to_string(),
        roles: vec![],
        is_super_user: false,
    }
}

/// The whole point of moving the settings onto a `ConfigExtension`: a `SET` has to
/// change what the *next* statement sees, on the one shared session.
#[tokio::test(flavor = "multi_thread")]
async fn set_changes_what_a_later_statement_reads() {
    let rt = common::runtime("set-live").await;

    assert_eq!(setting(&rt, "beacon.default_table").await, "default");

    rt.sql("SET beacon.default_table = 'observations'").await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "observations");

    rt.sql("SET beacon.sql.stream_coalesce.target_rows = 1024")
        .await;
    assert_eq!(
        setting(&rt, "beacon.sql.stream_coalesce.target_rows").await,
        "1024"
    );
}

/// The `beacon.` prefix reaches DataFusion's own options — the alias this rewrite
/// exists for. `SHOW` has to agree with `SET`, since both resolve a name through
/// the same path.
#[tokio::test(flavor = "multi_thread")]
async fn the_beacon_prefix_reaches_datafusion_options() {
    let rt = common::runtime("prefix-alias").await;

    rt.sql("SET beacon.execution.batch_size = 8192").await;
    assert_eq!(
        setting(&rt, "datafusion.execution.batch_size").await,
        "8192"
    );

    // Both spellings of `SHOW` name the same option.
    let via_beacon = rt.sql("SHOW beacon.execution.batch_size").await;
    let via_datafusion = rt.sql("SHOW datafusion.execution.batch_size").await;
    assert_eq!(scalar_str(&via_beacon), "datafusion.execution.batch_size");
    assert_eq!(
        scalar_str(&via_datafusion),
        "datafusion.execution.batch_size"
    );

    // The documented `BEACON_BATCH_SIZE` spelling lands in the same option.
    rt.sql("SET beacon.batch_size = 4096").await;
    assert_eq!(
        setting(&rt, "datafusion.execution.batch_size").await,
        "4096"
    );

    // …and `datafusion.*` keeps working unchanged.
    rt.sql("SET datafusion.execution.batch_size = 2048").await;
    assert_eq!(
        setting(&rt, "datafusion.execution.batch_size").await,
        "2048"
    );
}

/// A beacon setting has to appear in `information_schema.df_settings` under its
/// fully qualified name — which is also what makes `SHOW <key>` resolve at all.
#[tokio::test(flavor = "multi_thread")]
async fn beacon_settings_are_visible_to_show() {
    let rt = common::runtime("show-key").await;

    rt.sql("SET beacon.netcdf.use_rust_reader = true").await;
    assert_eq!(setting(&rt, "beacon.netcdf.use_rust_reader").await, "true");

    let rows = rt.sql("SHOW beacon.netcdf.use_rust_reader").await;
    assert_eq!(scalar_str(&rows), "beacon.netcdf.use_rust_reader");

    let count = rt
        .sql("SELECT count(*) FROM information_schema.df_settings WHERE name LIKE 'beacon.%'")
        .await;
    assert!(
        common::scalar_i64(&count) > 10,
        "the whole namespace should be listed"
    );
}

/// `RESET` restores the value the runtime *booted* with, not DataFusion's compiled
/// default — the reason beacon intercepts `RESET` rather than delegating it.
#[tokio::test(flavor = "multi_thread")]
async fn reset_restores_the_runtimes_own_default() {
    // A runtime whose batch size differs from DataFusion's compiled default.
    let rt = common::runtime_with("reset-boot", |builder| builder.with_batch_size(12_345)).await;

    rt.sql("SET beacon.batch_size = 999").await;
    assert_eq!(setting(&rt, "datafusion.execution.batch_size").await, "999");

    rt.sql("RESET beacon.batch_size").await;
    assert_eq!(
        setting(&rt, "datafusion.execution.batch_size").await,
        "12345",
        "RESET must restore the runtime's configured value, not DataFusion's default"
    );

    // The same holds for a beacon-native setting.
    rt.sql("SET beacon.netcdf.use_rust_reader = true").await;
    rt.sql("RESET beacon.netcdf.use_rust_reader").await;
    assert_eq!(setting(&rt, "beacon.netcdf.use_rust_reader").await, "false");
}

/// A startup-only key would look like it worked and change nothing, so it is
/// refused — with the variable to edit.
#[tokio::test(flavor = "multi_thread")]
async fn a_startup_only_setting_is_refused() {
    let rt = common::runtime("startup-only").await;

    let error = rt
        .try_sql("SET beacon.port = 1234")
        .await
        .expect_err("a startup-only key must be refused")
        .to_string();
    assert!(error.contains("BEACON_PORT"), "unhelpful error: {error}");

    let error = rt
        .try_sql("SET beacon.nonsense = 1")
        .await
        .expect_err("an unknown key must be refused")
        .to_string();
    assert!(error.contains("SHOW SETTINGS"), "unhelpful error: {error}");
}

/// `ALTER SYSTEM SET` is the persistent half: it applies now *and* replays at the
/// next boot, which is what an operator on Docker or Kubernetes needs.
#[tokio::test(flavor = "multi_thread")]
async fn alter_system_survives_a_restart() {
    let rt = common::restartable_runtime("alter-system", |b| b).await;

    rt.sql("ALTER SYSTEM SET beacon.default_table = 'observations'")
        .await;
    rt.sql("ALTER SYSTEM SET beacon.netcdf.use_rust_reader = 'true'")
        .await;

    // Applied to the live session straight away.
    assert_eq!(setting(&rt, "beacon.default_table").await, "observations");

    let rt = rt.restart().await;

    assert_eq!(
        setting(&rt, "beacon.default_table").await,
        "observations",
        "an ALTER SYSTEM value must outlive a restart"
    );
    assert_eq!(setting(&rt, "beacon.netcdf.use_rust_reader").await, "true");
}

/// After a restart, a persisted value *is* what the server started with, so a
/// plain `RESET` has to return to it — not skip past it to the environment's
/// value, which the next restart would immediately override again.
#[tokio::test(flavor = "multi_thread")]
async fn reset_returns_to_a_persisted_value_after_a_restart() {
    let rt = common::restartable_runtime("reset-vs-persisted", |b| b).await;

    rt.sql("ALTER SYSTEM SET beacon.default_table = 'observations'")
        .await;
    let rt = rt.restart().await;

    rt.sql("SET beacon.default_table = 'scratch'").await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "scratch");

    rt.sql("RESET beacon.default_table").await;
    assert_eq!(
        setting(&rt, "beacon.default_table").await,
        "observations",
        "RESET must return to the startup state, which includes the persisted value"
    );
}

/// `ALTER SYSTEM RESET` drops the persisted value, so the next boot goes back to
/// what the environment supplied.
#[tokio::test(flavor = "multi_thread")]
async fn alter_system_reset_forgets_the_persisted_value() {
    let rt = common::restartable_runtime("alter-system-reset", |b| b).await;

    rt.sql("ALTER SYSTEM SET beacon.default_table = 'observations'")
        .await;
    let rt = rt.restart().await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "observations");

    rt.sql("ALTER SYSTEM RESET beacon.default_table").await;
    // Restored live…
    assert_eq!(setting(&rt, "beacon.default_table").await, "default");
    // …and no longer replayed at boot.
    let rt = rt.restart().await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "default");
}

/// A plain `SET` is live-only. Without this the two statements would be the same
/// thing and the split would be pointless.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_set_does_not_survive_a_restart() {
    let rt = common::restartable_runtime("set-not-persisted", |b| b).await;

    rt.sql("SET beacon.default_table = 'observations'").await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "observations");

    let rt = rt.restart().await;
    assert_eq!(setting(&rt, "beacon.default_table").await, "default");
}

/// An in-memory runtime has nowhere to persist to, and has to say so rather than
/// accept a value it would silently lose.
#[tokio::test(flavor = "multi_thread")]
async fn alter_system_refuses_an_in_memory_runtime() {
    let rt = common::runtime("alter-system-in-memory").await;

    let error = rt
        .try_sql("ALTER SYSTEM SET beacon.default_table = 'observations'")
        .await
        .expect_err("an in-memory runtime cannot persist")
        .to_string();
    assert!(error.contains("in-memory"), "unhelpful error: {error}");

    // …and the refusal leaves nothing behind. A statement that reported failure
    // must not have changed the session on its way to the error.
    assert_eq!(setting(&rt, "beacon.default_table").await, "default");
}

/// `SHOW SETTINGS` documents the engine, so any authenticated caller can read it —
/// the issue's "a user cannot discover which settings exist". Changing one stays
/// super-user-only.
#[tokio::test(flavor = "multi_thread")]
async fn show_settings_is_readable_but_set_is_not() {
    let rt = common::runtime("settings-privileges").await;

    let rows = rt.sql_as("SHOW SETTINGS", regular_user()).await;
    let names = column_strings(&rows, 0);
    assert!(names.iter().any(|name| name == "beacon.default_table"));
    assert!(
        names.iter().all(|name| name.starts_with("beacon.")),
        "SHOW SETTINGS must expose only the beacon namespace"
    );

    for sql in [
        "SET beacon.default_table = 'observations'",
        "ALTER SYSTEM SET beacon.default_table = 'observations'",
        // The table form lives in `beacon.system`, which is super-user-only.
        "SELECT * FROM beacon.system.settings",
    ] {
        let error = rt
            .try_sql_as(sql, regular_user())
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be refused for a regular user"))
            .to_string();
        assert!(
            error.contains("permitted") || error.contains("permission"),
            "`{sql}` should fail as a privilege error, got: {error}"
        );
    }

    // None of the refused statements changed anything.
    assert_eq!(setting(&rt, "beacon.default_table").await, "default");
}

/// `SHOW SETTINGS` reports the live value and the one a `RESET` would restore, so
/// an operator can see both without running the reset.
#[tokio::test(flavor = "multi_thread")]
async fn show_settings_reports_the_value_and_the_boot_default() {
    let rt = common::runtime("settings-columns").await;

    rt.sql("SET beacon.default_table = 'observations'").await;

    let rows = rt
        .sql(
            "SELECT value, \"default\" FROM beacon.system.settings \
             WHERE name = 'beacon.default_table'",
        )
        .await;
    assert_eq!(column_strings(&rows, 0), vec!["observations".to_string()]);
    assert_eq!(column_strings(&rows, 1), vec!["default".to_string()]);
}

/// Beacon's new statements must not shadow the SQL they resemble. `ALTER TABLE`
/// and `SET <datafusion option>` predate this feature and have to keep working.
#[tokio::test(flavor = "multi_thread")]
async fn the_new_statements_do_not_shadow_existing_sql() {
    let rt = common::runtime("no-shadowing").await;

    rt.sql("SET datafusion.execution.batch_size = 8192").await;
    rt.sql("SET timezone = 'UTC'").await;
    rt.sql("SHOW TABLES").await;

    // `ALTER TABLE` on a missing table must fail as a *table* error, proving the
    // `ALTER SYSTEM` peek did not swallow it.
    let error = rt
        .try_sql("ALTER TABLE nope ADD COLUMN x INT")
        .await
        .expect_err("the table does not exist")
        .to_string();
    assert!(
        !error.contains("SHOW SETTINGS") && !error.contains("startup"),
        "ALTER TABLE was mistaken for ALTER SYSTEM: {error}"
    );
}
