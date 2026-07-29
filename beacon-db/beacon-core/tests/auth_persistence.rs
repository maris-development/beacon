//! Proves the point of the managed-tables auth backend: users/roles/grants are the durable truth
//! in the internal `__beacon_*` tables (so they survive a restart), those tables are super-user-only
//! (unconditionally, even with enforcement OFF — the security gate), and they are hidden from
//! user-facing catalog listings.

mod common;

use arrow::array::Array;
use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, total_rows, ADMIN_PASSWORD, ADMIN_USERNAME};

/// A user, role and table grant created before a restart still authenticate and enforce afterwards —
/// the state lived in the tables store, not in memory.
#[tokio::test(flavor = "multi_thread")]
async fn users_roles_and_grants_survive_a_restart() {
    // Enforcement on so the persisted grant is actually exercised after the restart.
    let rt = restartable_runtime("auth-persist", |b| b.with_auth_enforcement(true)).await;

    rt.sql_as("CREATE TABLE obs (a BIGINT)", AuthIdentity::system())
        .await;
    rt.sql_as("INSERT INTO obs VALUES (1), (2)", AuthIdentity::system())
        .await;
    rt.sql_as("CREATE ROLE reader", AuthIdentity::system())
        .await;
    rt.sql_as(
        "CREATE USER alice WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;
    rt.sql_as(
        "GRANT SELECT ON TABLE obs TO ROLE reader",
        AuthIdentity::system(),
    )
    .await;

    // Restart: drops the runtime (releasing the redb lock) and reopens the same store.
    let rt = rt.restart().await;

    // The user still authenticates (password hash persisted) with the same role.
    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice should still authenticate after a restart");
    assert_eq!(alice.roles, vec!["reader".to_string()]);
    assert!(!alice.is_super_user);

    // The grant is still enforced: the granted table reads, an ungranted one is denied.
    let granted = rt
        .try_sql_as("SELECT * FROM obs", alice.clone())
        .await
        .expect("the persisted grant should still allow the read");
    assert_eq!(total_rows(&granted), 2);

    rt.sql_as("CREATE TABLE secret (a BIGINT)", AuthIdentity::system())
        .await;
    let denied = rt.try_sql_as("SELECT * FROM secret", alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "an ungranted table must still be denied after a restart"
    );

    // The super-user still authenticates from config (never stored in the tables).
    assert!(
        rt.runtime
            .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
            .await
            .expect("admin should authenticate")
            .is_super_user
    );
}

/// The security gate: a non-super-user cannot read the internal `__beacon_users` table (which holds
/// Argon2 password hashes) even with enforcement OFF — the gate is unconditional and fails closed.
#[tokio::test(flavor = "multi_thread")]
async fn internal_tables_are_denied_to_non_super_users_with_enforcement_off() {
    // Enforcement OFF (the default): a grant-based gate would leak the hashes here.
    let rt = restartable_runtime("auth-gate", |b| b.with_auth_enforcement(false)).await;
    rt.sql_as(
        "CREATE USER alice WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;

    // A super-user (system) can read it — the write path and admins still resolve it by name.
    let admin_read = rt
        .try_sql_as("SELECT * FROM __beacon_users", AuthIdentity::system())
        .await
        .expect("the super-user may read the internal table");
    assert!(
        total_rows(&admin_read) >= 1,
        "the internal users table should hold the created user"
    );

    // A role-less non-super-user is denied, even though enforcement is off, so ordinary reads of
    // ordinary tables would be allowed.
    let denied = rt
        .try_sql_as("SELECT * FROM __beacon_users", AuthIdentity::empty())
        .await;
    assert!(
        denied
            .as_ref()
            .is_err_and(|e| e.to_string().contains("restricted to the super-user")),
        "a non-super-user must be denied the internal auth table with enforcement off, got: {denied:?}"
    );
}

/// The internal tables are hidden from user-facing catalog listings (`SHOW TABLES`), even for the
/// super-user, so their existence is not advertised.
#[tokio::test(flavor = "multi_thread")]
async fn internal_tables_are_hidden_from_show_tables() {
    let rt = restartable_runtime("auth-hidden", |b| b).await;
    rt.sql_as("CREATE TABLE visible (a BIGINT)", AuthIdentity::system())
        .await;

    let batches = rt.sql("SHOW TABLES").await;
    let mut names: Vec<String> = Vec::new();
    for batch in &batches {
        // information_schema's SHOW TABLES exposes a `table_name` column.
        let idx = batch
            .schema()
            .index_of("table_name")
            .expect("SHOW TABLES should have a table_name column");
        let col = batch.column(idx);
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..arr.len() {
                names.push(arr.value(i).to_string());
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
            for i in 0..arr.len() {
                names.push(arr.value(i).to_string());
            }
        }
    }

    assert!(
        names.iter().any(|n| n == "visible"),
        "a user table should be listed: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.starts_with("__beacon_")),
        "internal auth tables must not appear in SHOW TABLES: {names:?}"
    );
}
