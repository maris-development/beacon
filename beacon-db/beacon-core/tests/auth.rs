//! Integration tests for the authentication + authorization layer, exercised end to end through
//! the real [`Runtime`] (`run_query` with `AuthIdentity`s).
//!
//! Each test builds its own runtime through [`RuntimeBuilder`] with an ephemeral in-memory auth
//! context and an isolated storage root, so auth state and tables are per-test. Table names are
//! still suffixed with a uuid so a scenario's own statements never collide.

mod common;

use std::path::{Path, PathBuf};

use beacon_core::{AuthIdentity, Credential};
use common::{
    runtime_with, total_rows, TestRuntime, ADMIN_PASSWORD, ADMIN_USERNAME, ANONYMOUS_USERNAME,
};

/// Builds a runtime with explicit auth settings. `enforce` gates table/path grants for
/// non-super-users; `anonymous_enabled` seeds and enables the anonymous principal.
async fn auth_runtime(tag: &str, enforce: bool, anonymous_enabled: bool) -> TestRuntime {
    runtime_with(tag, move |builder| {
        let builder = builder.with_auth_enforcement(enforce);
        if anonymous_enabled {
            builder.with_anonymous_user(ANONYMOUS_USERNAME)
        } else {
            builder
        }
    })
    .await
}

/// A unique-per-test name so a scenario's statements never collide.
fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

// --------------------------------------------------------------------------------------------
// Authentication
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_credentials_authenticate_as_super_user() {
    let rt = auth_runtime("auth-admin", false, true).await;
    let identity = rt.admin().await;
    assert_eq!(identity.username, ADMIN_USERNAME);
    assert!(
        identity.is_super_user,
        "the bootstrapped admin is a super-user"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_password_and_unknown_user_are_rejected() {
    let rt = auth_runtime("auth-reject", false, true).await;
    assert!(rt
        .runtime
        .authenticate(&Credential::basic(ADMIN_USERNAME, "wrong"))
        .await
        .is_err());
    assert!(rt
        .runtime
        .authenticate(&Credential::basic("ghost", "whatever"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_credential_without_oidc_is_rejected() {
    // OIDC is disabled by default, so the local provider rejects bearer tokens.
    let rt = auth_runtime("auth-bearer", false, true).await;
    assert!(rt
        .runtime
        .authenticate(&Credential::bearer("not-a-jwt"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_enabled_resolves_to_roleless_identity() {
    let rt = auth_runtime("auth-anon-on", false, true).await;
    assert!(rt.runtime.anonymous_enabled());
    let identity = rt
        .runtime
        .authenticate_anonymous()
        .await
        .expect("anonymous should resolve");
    assert!(identity.roles.is_empty());
    assert!(!identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_disabled_errors() {
    let rt = auth_runtime("auth-anon-off", false, false).await;
    assert!(!rt.runtime.anonymous_enabled());
    assert!(rt.runtime.authenticate_anonymous().await.is_err());
}

// --------------------------------------------------------------------------------------------
// User / role lifecycle via SQL
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn create_user_role_and_grant_then_authenticate() {
    let rt = auth_runtime("auth-lifecycle", false, true).await;
    rt.sql_as("CREATE ROLE reader", AuthIdentity::system()).await;
    rt.sql_as("CREATE USER alice WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    rt.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;

    let identity = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice should authenticate");
    assert_eq!(identity.username, "alice");
    assert_eq!(identity.roles, vec!["reader".to_string()]);
    assert!(!identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_user_revokes_authentication() {
    let rt = auth_runtime("auth-drop-user", false, true).await;
    rt.sql_as("CREATE USER bob WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    assert!(rt
        .runtime
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_ok());

    rt.sql_as("DROP USER bob", AuthIdentity::system()).await;
    assert!(rt
        .runtime
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn created_users_and_roles_are_never_super_user() {
    let rt = auth_runtime("auth-no-escalate", false, true).await;
    rt.sql_as("CREATE ROLE owners", AuthIdentity::system()).await;
    rt.sql_as("CREATE USER root WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    rt.sql_as("GRANT ROLE owners TO USER root", AuthIdentity::system())
        .await;

    // No SQL-created user is ever a super-user, whatever roles they hold.
    let identity = rt
        .runtime
        .authenticate(&Credential::basic("root", "pw"))
        .await
        .unwrap();
    assert!(!identity.is_super_user);

    // Write/management privileges cannot be granted to a role — roles are strictly read-only, so
    // there is no way to mint another super-user through SQL.
    for sql in [
        "GRANT ALL TO ROLE owners",
        "GRANT INSERT ON TABLE t TO ROLE owners",
        "GRANT DROP ON TABLE t TO ROLE owners",
    ] {
        let err = rt.try_sql_as(sql, AuthIdentity::system()).await;
        assert!(
            err.is_err_and(|e| e.to_string().contains("read-only")),
            "granting write privileges to a role should be rejected: {sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_super_user_is_config_defined_and_reserved() {
    let rt = auth_runtime("auth-reserved", false, true).await;

    // The configured admin credential is the one and only super-user.
    assert!(rt.admin().await.is_super_user);

    // Its username is reserved: it cannot be created or dropped as a stored user via SQL.
    // (Quoted because the admin username contains a hyphen.)
    let created = rt
        .try_sql_as(
            &format!("CREATE USER \"{ADMIN_USERNAME}\" WITH PASSWORD 'x'"),
            AuthIdentity::system(),
        )
        .await;
    assert!(
        created.is_err_and(|e| e.to_string().contains("reserved super-user")),
        "creating the super-user as a stored user should be rejected"
    );

    let dropped = rt
        .try_sql_as(
            &format!("DROP USER \"{ADMIN_USERNAME}\""),
            AuthIdentity::system(),
        )
        .await;
    assert!(
        dropped.is_err_and(|e| e.to_string().contains("reserved super-user")),
        "dropping the super-user should be rejected"
    );
}

// --------------------------------------------------------------------------------------------
// Read enforcement (auth enforcement on)
// --------------------------------------------------------------------------------------------

/// Seeds two tables and a `reader` role/user, granting SELECT only on `t1`. Returns
/// `(t1, t2, alice_identity)`.
async fn seed_two_tables_and_reader(rt: &TestRuntime) -> (String, String, AuthIdentity) {
    let t1 = unique("t1");
    let t2 = unique("t2");
    rt.sql_as(&format!("CREATE TABLE {t1} (a BIGINT)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("INSERT INTO {t1} VALUES (1), (2)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("CREATE TABLE {t2} (a BIGINT)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("INSERT INTO {t2} VALUES (3)"), AuthIdentity::system())
        .await;

    let role = unique("reader");
    let user = unique("alice");
    rt.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT SELECT ON TABLE {t1} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let identity = rt
        .runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();
    (t1, t2, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_table_grant_allows_only_the_granted_table() {
    let rt = auth_runtime("auth-table-grant", true, true).await;
    let (t1, t2, alice) = seed_two_tables_and_reader(&rt).await;

    let allowed = rt.try_sql_as(&format!("SELECT * FROM {t1}"), alice.clone()).await;
    assert_eq!(total_rows(&allowed.expect("granted table is readable")), 2);

    let denied = rt.try_sql_as(&format!("SELECT * FROM {t2}"), alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "ungranted table must be denied"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_deny_wins_over_grant() {
    let rt = auth_runtime("auth-deny-wins", true, true).await;
    let t1 = unique("t1");
    let t2 = unique("t2");
    rt.sql_as(&format!("CREATE TABLE {t1} (a BIGINT)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("INSERT INTO {t1} VALUES (1)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("CREATE TABLE {t2} (a BIGINT)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("INSERT INTO {t2} VALUES (2)"), AuthIdentity::system())
        .await;

    let role = unique("reader");
    let user = unique("alice");
    rt.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    // Grant SELECT on everything, then deny one table — deny must win.
    rt.sql_as(&format!("GRANT SELECT TO ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("DENY SELECT ON TABLE {t2} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let alice = rt
        .runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();

    assert!(rt
        .try_sql_as(&format!("SELECT * FROM {t1}"), alice.clone())
        .await
        .is_ok());
    assert!(
        rt.try_sql_as(&format!("SELECT * FROM {t2}"), alice)
            .await
            .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied table must be rejected even with a broad grant"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_revoke_removes_access() {
    let rt = auth_runtime("auth-revoke", true, true).await;
    let (t1, _t2, alice) = seed_two_tables_and_reader(&rt).await;

    // A self-contained scenario: seed_* used unique names, so grant via a fresh role.
    let role = unique("r");
    let user = unique("u");
    let table = unique("tg");
    rt.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(&format!("INSERT INTO {table} VALUES (1)"), AuthIdentity::system())
        .await;
    rt.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT SELECT ON TABLE {table} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    let u = rt
        .runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();

    assert!(rt
        .try_sql_as(&format!("SELECT * FROM {table}"), u.clone())
        .await
        .is_ok());

    rt.sql_as(
        &format!("REVOKE SELECT ON TABLE {table} FROM ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    assert!(
        rt.try_sql_as(&format!("SELECT * FROM {table}"), u)
            .await
            .is_err(),
        "access must be gone after REVOKE"
    );

    // (t1/alice from the shared helper remain readable — sanity that seeding is independent.)
    assert!(rt
        .try_sql_as(&format!("SELECT * FROM {t1}"), alice)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_anonymous_has_no_read_access() {
    let rt = auth_runtime("auth-anon-denied", true, true).await;
    let table = unique("t");
    rt.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(&format!("INSERT INTO {table} VALUES (1)"), AuthIdentity::system())
        .await;

    let anon = rt.runtime.authenticate_anonymous().await.unwrap();
    assert!(
        rt.try_sql_as(&format!("SELECT * FROM {table}"), anon)
            .await
            .is_err(),
        "a role-less anonymous user must be denied under enforcement"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn super_user_bypasses_enforcement() {
    let rt = auth_runtime("auth-super-bypass", true, true).await;
    let (_t1, t2, _alice) = seed_two_tables_and_reader(&rt).await;
    let admin = rt.admin().await;
    // The admin can read the table the reader was never granted.
    assert!(rt
        .try_sql_as(&format!("SELECT * FROM {t2}"), admin)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforcement_off_allows_ungranted_reads() {
    let rt = auth_runtime("auth-enforce-off", false, true).await;
    let table = unique("t");
    rt.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(&format!("INSERT INTO {table} VALUES (1)"), AuthIdentity::system())
        .await;

    // A role-less identity can still read when enforcement is off (backwards-compatible default).
    let result = rt
        .try_sql_as(&format!("SELECT * FROM {table}"), AuthIdentity::empty())
        .await;
    assert_eq!(total_rows(&result.expect("read allowed when enforce=off")), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn information_schema_is_exempt_from_enforcement() {
    let rt = auth_runtime("auth-info-schema", true, true).await;
    let table = unique("t");
    rt.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;

    // A role-less user can still introspect information_schema even with enforcement on.
    let result = rt
        .try_sql_as(
            "SELECT table_name FROM information_schema.tables",
            AuthIdentity::empty(),
        )
        .await;
    assert!(
        result.is_ok(),
        "information_schema must be readable: {result:?}"
    );
}

// --------------------------------------------------------------------------------------------
// Privilege escalation / write gating
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn non_super_user_cannot_manage_auth_or_run_ddl() {
    let rt = auth_runtime("auth-gate-ddl", true, true).await;
    rt.sql_as("CREATE ROLE reader", AuthIdentity::system()).await;
    rt.sql_as("CREATE USER alice WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    rt.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;
    let alice = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .unwrap();

    // Auth-management statements are super-user-only (they lower to extension nodes).
    for sql in [
        "CREATE ROLE hacker",
        "GRANT ALL TO ROLE reader",
        "CREATE USER bob WITH PASSWORD 'pw'",
        "DROP USER alice",
    ] {
        let err = rt
            .try_sql_as(sql, alice.clone())
            .await
            .err()
            .unwrap_or_else(|| panic!("non-super should be rejected: {sql}"));
        assert!(
            err.to_string().contains("super-user"),
            "expected super-user error for `{sql}`, got: {err}"
        );
    }

    // Standard DDL is also rejected for a non-super-user.
    assert!(rt
        .try_sql_as(
            &format!("CREATE TABLE {} (a BIGINT)", unique("x")),
            alice
        )
        .await
        .is_err());
}

// --------------------------------------------------------------------------------------------
// Path-glob enforcement (ad-hoc file scans via `read_parquet`)
// --------------------------------------------------------------------------------------------
//
// An ad-hoc `read_parquet('<glob>')` scan resolves to a `Path` target (the datasets-root-relative
// glob), checked against `GRANT/DENY ... ON PATH '<pattern>'` with segment-aware matching.

/// The bundled parquet fixture used to back dataset files.
fn parquet_fixture() -> PathBuf {
    // `beacon-db/beacon-core` -> up two, to the workspace root that holds
    // `test-datasets/`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

/// Copies the parquet fixture to `<datasets_dir>/<rel>`, creating parent dirs.
fn place_dataset(datasets_dir: &Path, rel: &str) {
    let dst = datasets_dir.join(rel);
    std::fs::create_dir_all(dst.parent().unwrap()).expect("create dataset subdir");
    std::fs::copy(parquet_fixture(), &dst).expect("copy parquet fixture");
}

/// Seeds a role assigned to a fresh user, returning `(role, alice_identity)`.
async fn seed_reader(rt: &TestRuntime) -> (String, AuthIdentity) {
    let role = unique("preader");
    let user = unique("palice");
    rt.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    rt.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    let identity = rt
        .runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();
    (role, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_grant_matches_only_granted_prefix() {
    let rt = auth_runtime("auth-path-grant", true, true).await;
    let root = unique("pathauth");
    place_dataset(rt.datasets_dir(), &format!("{root}/allowed/data.parquet"));
    place_dataset(rt.datasets_dir(), &format!("{root}/blocked/data.parquet"));

    let (role, alice) = seed_reader(&rt).await;
    rt.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/allowed/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let allowed = rt
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/allowed/*.parquet')"),
            alice.clone(),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "granted path should be readable: {allowed:?}"
    );

    let denied = rt
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/blocked/*.parquet')"),
            alice,
        )
        .await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "a different path prefix must be denied"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_glob_is_segment_aware() {
    let rt = auth_runtime("auth-path-seg", true, true).await;
    let root = unique("pathseg");
    place_dataset(rt.datasets_dir(), &format!("{root}/data/top.parquet"));
    place_dataset(rt.datasets_dir(), &format!("{root}/data/sub/nested.parquet"));
    place_dataset(rt.datasets_dir(), &format!("{root}/rec/sub/nested.parquet"));

    let (role, alice) = seed_reader(&rt).await;
    // Single-segment grant: `*` does not cross `/`.
    rt.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/data/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    // Recursive grant: `**` crosses `/`.
    rt.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/rec/**' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    // Top-level file under the single-segment grant is allowed.
    assert!(rt
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/data/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_ok());

    // A nested path is NOT covered by the single-`*` grant.
    assert!(
        rt.try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/data/sub/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "a single `*` grant must not cross a path separator"
    );

    // The recursive `**` grant does cover a nested path.
    assert!(rt
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/rec/sub/*.parquet')"),
            alice,
        )
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_deny_wins_over_broad_grant() {
    let rt = auth_runtime("auth-path-deny", true, true).await;
    let root = unique("pathdeny");
    place_dataset(rt.datasets_dir(), &format!("{root}/public/data.parquet"));
    place_dataset(rt.datasets_dir(), &format!("{root}/secret/data.parquet"));

    let (role, alice) = seed_reader(&rt).await;
    // Broad SELECT grant, then deny one subtree — the deny must win.
    rt.sql_as(&format!("GRANT SELECT TO ROLE {role}"), AuthIdentity::system())
        .await;
    rt.sql_as(
        &format!("DENY SELECT ON PATH '{root}/secret/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    assert!(rt
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/public/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_ok());

    assert!(
        rt.try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/secret/*.parquet')"),
            alice,
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied subtree must be rejected despite the broad grant"
    );
}
