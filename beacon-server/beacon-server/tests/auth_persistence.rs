//! Proves the durability of the auth state and the gate on the internal tables.
//!
//! - Users, roles, grants, denies and role settings are in `beacon-auth.json`, so they survive
//!   a restart.
//! - On the first start without that file, the server imports the `__beacon_*` auth tables of
//!   `beacon.db`.
//! - The internal `__beacon_*` tables are super-user-only, also with enforcement off.
//! - The internal tables are hidden from user-facing catalog listings.

mod common;

use beacon_auth::{AuthIdentity, Privilege, PrivilegeRule, PrivilegeTarget};
use common::{config, server_with, total_rows, TestServer, ADMIN_PASSWORD, ADMIN_USERNAME};

/// The internal table of the engine that holds the metrics of each query.
const QUERY_METRICS_TABLE: &str = "__beacon_query_metrics";

/// A server with grant enforcement on, so a stored grant has an effect.
async fn enforced_server() -> TestServer {
    server_with(config(true)).await
}

/// A user, role, table grant, deny and role setting created before a restart still apply
/// afterwards. The state is in the auth store file, not in memory.
#[tokio::test(flavor = "multi_thread")]
async fn users_roles_and_grants_survive_a_restart() {
    let ts = enforced_server().await;
    for statement in [
        "CREATE TABLE obs (a BIGINT)",
        "INSERT INTO obs VALUES (1), (2)",
        "CREATE TABLE closed (a BIGINT)",
        "CREATE ROLE reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE reader TO USER alice",
        "GRANT SELECT TO ROLE reader",
        "DENY SELECT ON TABLE closed TO ROLE reader",
        "ALTER ROLE reader SET tier = 'gold'",
    ] {
        ts.sql_as(statement, AuthIdentity::system()).await;
    }

    // Restart: drops the server (releasing the redb lock) and opens the same root again.
    let ts = ts.restart().await;

    // The auth state is in its own file next to `beacon.db`.
    assert!(
        ts.auth_store_path().is_file(),
        "the auth store file should exist at {}",
        ts.auth_store_path().display()
    );

    // The user still authenticates (password hash persisted) with the same role.
    let alice = ts.login("alice", "pw").await;
    assert_eq!(alice.roles, vec!["reader".to_string()]);
    assert!(!alice.is_super_user);

    // The grant still applies: the granted table reads.
    let granted = ts
        .try_sql_as("SELECT * FROM obs", alice.clone())
        .await
        .expect("the persisted grant should still allow the read");
    assert_eq!(total_rows(&granted), 2);

    // The deny still applies, and it wins over the broad grant.
    let denied = ts.try_sql_as("SELECT * FROM closed", alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied table must still be denied after a restart"
    );

    // The role setting survives.
    assert_eq!(
        ts.server.auth().role_setting("reader", "tier").as_deref(),
        Some("gold")
    );

    // The super-user still authenticates from config (never stored in the auth store).
    assert!(ts.login(ADMIN_USERNAME, ADMIN_PASSWORD).await.is_super_user);
}

/// The five auth tables of `beacon.db` in an earlier version. The server imports them.
async fn create_legacy_auth_tables(ts: &TestServer) {
    for statement in [
        "CREATE TABLE __beacon_users (username VARCHAR, password_hash VARCHAR)",
        "CREATE TABLE __beacon_user_roles (username VARCHAR, role VARCHAR)",
        "CREATE TABLE __beacon_roles (name VARCHAR)",
        "CREATE TABLE __beacon_role_rules \
         (role VARCHAR, kind VARCHAR, privilege VARCHAR, target_type VARCHAR, target_value VARCHAR)",
        "CREATE TABLE __beacon_role_settings (role VARCHAR, \"key\" VARCHAR, value VARCHAR)",
    ] {
        ts.sql_as(statement, AuthIdentity::system()).await;
    }
}

/// Inserts a user with the password `pw` into the legacy users table.
async fn insert_legacy_user(ts: &TestServer, username: &str) {
    let hash = beacon_auth::hash_password("pw").expect("hash the password");
    ts.sql_as(
        &format!("INSERT INTO __beacon_users VALUES ('{username}', '{hash}')"),
        AuthIdentity::system(),
    )
    .await;
}

/// Drops the server, removes the auth store file and opens the server again.
///
/// The new server finds no auth store, so it imports the legacy tables.
async fn restart_without_auth_store(ts: TestServer) -> TestServer {
    let path = ts.auth_store_path();
    std::fs::remove_file(&path).expect("remove the auth store file");
    ts.restart().await
}

/// On the first start without an auth store, the server imports the auth tables of `beacon.db`.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_start_imports_the_legacy_auth_tables() {
    // Arrange
    let ts = enforced_server().await;
    create_legacy_auth_tables(&ts).await;
    insert_legacy_user(&ts, "legacy_bob").await;
    for statement in [
        "CREATE TABLE obs (a BIGINT)",
        "INSERT INTO obs VALUES (1), (2), (3)",
        "CREATE TABLE other (a BIGINT)",
        "INSERT INTO __beacon_roles VALUES ('legacy_reader')",
        "INSERT INTO __beacon_user_roles VALUES ('legacy_bob', 'legacy_reader')",
        "INSERT INTO __beacon_role_rules VALUES ('legacy_reader', 'grant', 'SELECT', 'table', 'obs')",
        "INSERT INTO __beacon_role_settings VALUES ('legacy_reader', 'tier', 'gold')",
    ] {
        ts.sql_as(statement, AuthIdentity::system()).await;
    }

    // Act
    let ts = restart_without_auth_store(ts).await;

    // Assert
    let bob = ts.login("legacy_bob", "pw").await;
    assert_eq!(bob.roles, vec!["legacy_reader".to_string()]);
    assert!(!bob.is_super_user);

    let role = ts
        .server
        .auth()
        .list_roles()
        .into_iter()
        .find(|role| role.name == "legacy_reader")
        .expect("the legacy role should be imported");
    let grant = PrivilegeRule::new(
        Privilege::Select,
        Some(PrivilegeTarget::Table("obs".to_string())),
    );
    assert!(
        role.grants.contains(&grant),
        "the legacy grant should be imported, got: {:?}",
        role.grants
    );
    assert_eq!(
        ts.server
            .auth()
            .role_setting("legacy_reader", "tier")
            .as_deref(),
        Some("gold")
    );

    let read = ts
        .try_sql_as("SELECT * FROM obs", bob.clone())
        .await
        .expect("the imported grant should allow the read");
    assert_eq!(total_rows(&read), 3);
    let denied = ts.try_sql_as("SELECT * FROM other", bob).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "a table without a grant must be denied"
    );
    assert!(
        ts.auth_store_path().is_file(),
        "the import should write the auth store file"
    );
}

/// The import drops the auth tables. `beacon.db` then holds no password hash, and a lost auth
/// store cannot bring back a user that the admin dropped.
#[tokio::test(flavor = "multi_thread")]
async fn the_import_drops_the_legacy_tables_and_runs_once() {
    // Arrange
    let ts = enforced_server().await;
    create_legacy_auth_tables(&ts).await;
    insert_legacy_user(&ts, "legacy_bob").await;
    let ts = restart_without_auth_store(ts).await;
    ts.login("legacy_bob", "pw").await;

    // Act
    let tables_left: Vec<&str> = [
        "__beacon_users",
        "__beacon_user_roles",
        "__beacon_roles",
        "__beacon_role_rules",
        "__beacon_role_settings",
    ]
    .into_iter()
    .filter(|table| ts.server.runtime().table_exists(*table).unwrap())
    .collect();
    ts.sql_as("DROP USER legacy_bob", AuthIdentity::system())
        .await;
    let ts = restart_without_auth_store(ts).await;
    let login = ts
        .server
        .authenticate(&beacon_auth::Credential::basic("legacy_bob", "pw"))
        .await;

    // Assert
    assert!(tables_left.is_empty(), "tables left: {tables_left:?}");
    assert!(login.is_err(), "a dropped user must not come back");
}

/// An earlier version stored a quoted table name with its quotes, so the rule never matched.
/// The import strips them. A revoke then also holds after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_stored_quoted_table_rule_matches_and_revokes_after_a_restart() {
    let ts = enforced_server().await;
    create_legacy_auth_tables(&ts).await;
    insert_legacy_user(&ts, "legacy_alice").await;
    for statement in [
        "CREATE TABLE \"with-hyphen\" (a BIGINT)",
        "INSERT INTO \"with-hyphen\" VALUES (1)",
        "INSERT INTO __beacon_roles VALUES ('legacy_reader')",
        "INSERT INTO __beacon_user_roles VALUES ('legacy_alice', 'legacy_reader')",
        "INSERT INTO __beacon_role_rules (role, kind, privilege, target_type, target_value) \
         VALUES ('legacy_reader', 'grant', 'SELECT', 'table', '\"with-hyphen\"')",
    ] {
        ts.sql_as(statement, AuthIdentity::system()).await;
    }

    let ts = restart_without_auth_store(ts).await;
    let alice = ts.login("legacy_alice", "pw").await;
    let read = ts
        .try_sql_as("SELECT * FROM \"with-hyphen\"", alice.clone())
        .await
        .expect("the stored rule matches once its quotes are gone");
    assert_eq!(total_rows(&read), 1);

    ts.sql_as(
        "REVOKE SELECT ON TABLE \"with-hyphen\" FROM ROLE legacy_reader",
        AuthIdentity::system(),
    )
    .await;
    // The auth store exists now, so this start does not import the legacy tables again.
    let ts = ts.restart().await;
    let denied = ts.try_sql_as("SELECT * FROM \"with-hyphen\"", alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "the revoked rule must not come back after a restart"
    );
}

/// The security gate: a non-super-user cannot read an internal `__beacon_*` table, even with
/// enforcement OFF. The gate is unconditional and fails closed.
#[tokio::test(flavor = "multi_thread")]
async fn internal_tables_are_denied_to_non_super_users_with_enforcement_off() {
    // Enforcement OFF (the default): a grant-based gate would leak the table here.
    let ts = server_with(config(false)).await;
    let sql = format!("SELECT * FROM {QUERY_METRICS_TABLE}");

    // A super-user can read it.
    ts.try_sql_as(&sql, AuthIdentity::system())
        .await
        .expect("the super-user may read the internal table");

    // A role-less non-super-user is denied, even though enforcement is off, so ordinary reads of
    // ordinary tables would be allowed.
    let denied = ts.try_sql_as(&sql, AuthIdentity::empty()).await;
    assert!(
        denied
            .as_ref()
            .is_err_and(|e| e.to_string().contains("restricted to the super-user")),
        "a non-super-user must be denied the internal table with enforcement off, got: {denied:?}"
    );

    // The table check refuses each internal name, also a name with no table.
    let validator = ts.server.validator(AuthIdentity::empty());
    for table in [QUERY_METRICS_TABLE, "__beacon_users"] {
        let checked = validator.check_table(&ts.server.runtime().table_facts(table));
        assert!(
            checked
                .as_ref()
                .is_err_and(|e| e.to_string().contains("restricted to the super-user")),
            "the table check must refuse {table}, got: {checked:?}"
        );
    }
}

/// The internal tables are hidden from user-facing catalog listings (`SHOW TABLES`), even for the
/// super-user, so their existence is not advertised.
#[tokio::test(flavor = "multi_thread")]
async fn internal_tables_are_hidden_from_show_tables() {
    let ts = server_with(config(false)).await;
    ts.sql_as("CREATE TABLE visible (a BIGINT)", AuthIdentity::system())
        .await;

    let batches = ts.sql("SHOW TABLES").await;
    // information_schema's SHOW TABLES exposes a `table_name` column.
    let column = batches
        .first()
        .expect("SHOW TABLES should return a batch")
        .schema()
        .index_of("table_name")
        .expect("SHOW TABLES should have a table_name column");
    let names = common::column_strings(&batches, column);

    assert!(
        names.iter().any(|n| n == "visible"),
        "a user table should be listed: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.starts_with("__beacon_")),
        "internal tables must not appear in SHOW TABLES: {names:?}"
    );
}
