//! Integration tests for the authentication + authorization layer, exercised end to end through
//! the real [`Runtime`] (`run_query` with `AuthIdentity`s).
//!
//! Each test builds its own runtime with an ephemeral in-memory auth context
//! (`new_with_in_memory_auth`), bootstrapped from the config's admin credentials, so auth state is
//! isolated per test. The rest of the runtime (object stores, tables dir) is shared on disk, so
//! table names are suffixed with a uuid to avoid cross-test collisions.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use beacon_config::Config;
use beacon_core::{runtime::Runtime, AuthIdentity, Credential};
use futures::TryStreamExt;

/// Builds a config with explicit auth settings (otherwise defaults from the environment).
fn config(enforce: bool, anonymous_enabled: bool) -> Arc<Config> {
    let mut config = Config::load().expect("load config");
    config.auth.enforce = enforce;
    config.auth.anonymous_enabled = anonymous_enabled;
    Arc::new(config)
}

async fn runtime_with(config: Arc<Config>) -> (Runtime, Arc<Config>) {
    let runtime = Runtime::new_with_in_memory_auth(config.clone())
        .await
        .expect("runtime should start");
    (runtime, config)
}

/// A unique-per-test table name so concurrent tests don't collide on the shared tables dir.
fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// Authenticates the bootstrapped admin (super-user) for the given config.
async fn admin_identity(runtime: &Runtime, config: &Config) -> AuthIdentity {
    runtime
        .authenticate(&Credential::basic(
            config.admin.username.clone(),
            config.admin.password.clone(),
        ))
        .await
        .expect("admin should authenticate")
}

/// Runs a statement and collects its rows.
async fn exec(
    runtime: &Runtime,
    sql: &str,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<RecordBatch>> {
    let batches = runtime
        .run_query(beacon_core::query::Query::sql(sql.to_string()), identity)
        .await?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(batches)
}

/// Runs a statement as a super-user and asserts it succeeds (test setup helper).
async fn exec_admin(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    exec(runtime, sql, AuthIdentity::system())
        .await
        .unwrap_or_else(|err| panic!("super-user statement failed: {sql}: {err}"))
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

// --------------------------------------------------------------------------------------------
// Authentication
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_credentials_authenticate_as_super_user() {
    let (runtime, config) = runtime_with(config(false, true)).await;
    let identity = admin_identity(&runtime, &config).await;
    assert_eq!(identity.username, config.admin.username);
    assert!(identity.is_super_user, "the bootstrapped admin is a super-user");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_password_and_unknown_user_are_rejected() {
    let (runtime, config) = runtime_with(config(false, true)).await;
    assert!(runtime
        .authenticate(&Credential::basic(config.admin.username.clone(), "wrong"))
        .await
        .is_err());
    assert!(runtime
        .authenticate(&Credential::basic("ghost", "whatever"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_credential_without_oidc_is_rejected() {
    // OIDC is disabled by default, so the local provider rejects bearer tokens.
    let (runtime, _config) = runtime_with(config(false, true)).await;
    assert!(runtime
        .authenticate(&Credential::bearer("not-a-jwt"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_enabled_resolves_to_roleless_identity() {
    let (runtime, _config) = runtime_with(config(false, true)).await;
    assert!(runtime.anonymous_enabled());
    let identity = runtime
        .authenticate_anonymous()
        .await
        .expect("anonymous should resolve");
    assert!(identity.roles.is_empty());
    assert!(!identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_disabled_errors() {
    let (runtime, _config) = runtime_with(config(false, false)).await;
    assert!(!runtime.anonymous_enabled());
    assert!(runtime.authenticate_anonymous().await.is_err());
}

// --------------------------------------------------------------------------------------------
// User / role lifecycle via SQL
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn create_user_role_and_grant_then_authenticate() {
    let (runtime, _config) = runtime_with(config(false, true)).await;
    exec_admin(&runtime, "CREATE ROLE reader").await;
    exec_admin(&runtime, "CREATE USER alice WITH PASSWORD 'pw'").await;
    exec_admin(&runtime, "GRANT ROLE reader TO USER alice").await;

    let identity = runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice should authenticate");
    assert_eq!(identity.username, "alice");
    assert_eq!(identity.roles, vec!["reader".to_string()]);
    assert!(!identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_user_revokes_authentication() {
    let (runtime, _config) = runtime_with(config(false, true)).await;
    exec_admin(&runtime, "CREATE USER bob WITH PASSWORD 'pw'").await;
    assert!(runtime
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_ok());

    exec_admin(&runtime, "DROP USER bob").await;
    assert!(runtime
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn granting_a_global_all_role_makes_a_user_super() {
    let (runtime, _config) = runtime_with(config(false, true)).await;
    exec_admin(&runtime, "CREATE ROLE owners").await;
    exec_admin(&runtime, "GRANT ALL TO ROLE owners").await;
    exec_admin(&runtime, "CREATE USER root WITH PASSWORD 'pw'").await;
    exec_admin(&runtime, "GRANT ROLE owners TO USER root").await;

    let identity = runtime
        .authenticate(&Credential::basic("root", "pw"))
        .await
        .unwrap();
    assert!(identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn reserved_super_user_role_cannot_be_created_dropped_or_revoked() {
    let (runtime, _config) = runtime_with(config(false, true)).await;

    let created = exec(&runtime, "CREATE ROLE admin", AuthIdentity::system()).await;
    assert!(
        created.is_err_and(|e| e.to_string().contains("reserved")),
        "creating the reserved super-user role should be rejected"
    );

    let dropped = exec(&runtime, "DROP ROLE admin", AuthIdentity::system()).await;
    assert!(dropped.is_err(), "dropping the super-user role should be rejected");

    let revoked = exec(&runtime, "REVOKE ALL FROM ROLE admin", AuthIdentity::system()).await;
    assert!(
        revoked.is_err(),
        "revoking the super-user role's global ALL grant should be rejected"
    );
}

// --------------------------------------------------------------------------------------------
// Read enforcement (BEACON_AUTH_ENFORCE = true)
// --------------------------------------------------------------------------------------------

/// Seeds two tables and a `reader` role/user, granting SELECT only on `t1`. Returns
/// `(t1, t2, alice_identity)`.
async fn seed_two_tables_and_reader(runtime: &Runtime) -> (String, String, AuthIdentity) {
    let t1 = unique("t1");
    let t2 = unique("t2");
    exec_admin(runtime, &format!("CREATE TABLE {t1} (a BIGINT)")).await;
    exec_admin(runtime, &format!("INSERT INTO {t1} VALUES (1), (2)")).await;
    exec_admin(runtime, &format!("CREATE TABLE {t2} (a BIGINT)")).await;
    exec_admin(runtime, &format!("INSERT INTO {t2} VALUES (3)")).await;

    let role = unique("reader");
    let user = unique("alice");
    exec_admin(runtime, &format!("CREATE ROLE {role}")).await;
    exec_admin(runtime, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    exec_admin(runtime, &format!("GRANT ROLE {role} TO USER {user}")).await;
    exec_admin(runtime, &format!("GRANT SELECT ON TABLE {t1} TO ROLE {role}")).await;

    let identity = runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();
    (t1, t2, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_table_grant_allows_only_the_granted_table() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    let (t1, t2, alice) = seed_two_tables_and_reader(&runtime).await;

    let allowed = exec(&runtime, &format!("SELECT * FROM {t1}"), alice.clone()).await;
    assert_eq!(total_rows(&allowed.expect("granted table is readable")), 2);

    let denied = exec(&runtime, &format!("SELECT * FROM {t2}"), alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "ungranted table must be denied"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_deny_wins_over_grant() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    let t1 = unique("t1");
    let t2 = unique("t2");
    exec_admin(&runtime, &format!("CREATE TABLE {t1} (a BIGINT)")).await;
    exec_admin(&runtime, &format!("INSERT INTO {t1} VALUES (1)")).await;
    exec_admin(&runtime, &format!("CREATE TABLE {t2} (a BIGINT)")).await;
    exec_admin(&runtime, &format!("INSERT INTO {t2} VALUES (2)")).await;

    let role = unique("reader");
    let user = unique("alice");
    exec_admin(&runtime, &format!("CREATE ROLE {role}")).await;
    exec_admin(&runtime, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    exec_admin(&runtime, &format!("GRANT ROLE {role} TO USER {user}")).await;
    // Grant SELECT on everything, then deny one table — deny must win.
    exec_admin(&runtime, &format!("GRANT SELECT TO ROLE {role}")).await;
    exec_admin(&runtime, &format!("DENY SELECT ON TABLE {t2} TO ROLE {role}")).await;

    let alice = runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();

    assert!(exec(&runtime, &format!("SELECT * FROM {t1}"), alice.clone())
        .await
        .is_ok());
    assert!(
        exec(&runtime, &format!("SELECT * FROM {t2}"), alice)
            .await
            .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied table must be rejected even with a broad grant"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_revoke_removes_access() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    let (t1, _t2, alice) = seed_two_tables_and_reader(&runtime).await;

    // Re-derive the role name from the grant: seed_* used unique names, so grant via a fresh role.
    // Simpler: build a self-contained scenario here.
    let role = unique("r");
    let user = unique("u");
    let table = unique("tg");
    exec_admin(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
    exec_admin(&runtime, &format!("INSERT INTO {table} VALUES (1)")).await;
    exec_admin(&runtime, &format!("CREATE ROLE {role}")).await;
    exec_admin(&runtime, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    exec_admin(&runtime, &format!("GRANT ROLE {role} TO USER {user}")).await;
    exec_admin(&runtime, &format!("GRANT SELECT ON TABLE {table} TO ROLE {role}")).await;
    let u = runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();

    assert!(exec(&runtime, &format!("SELECT * FROM {table}"), u.clone())
        .await
        .is_ok());

    exec_admin(&runtime, &format!("REVOKE SELECT ON TABLE {table} FROM ROLE {role}")).await;
    assert!(
        exec(&runtime, &format!("SELECT * FROM {table}"), u)
            .await
            .is_err(),
        "access must be gone after REVOKE"
    );

    // (t1/alice from the shared helper remain readable — sanity that seeding is independent.)
    assert!(exec(&runtime, &format!("SELECT * FROM {t1}"), alice)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_anonymous_has_no_read_access() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    let table = unique("t");
    exec_admin(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
    exec_admin(&runtime, &format!("INSERT INTO {table} VALUES (1)")).await;

    let anon = runtime.authenticate_anonymous().await.unwrap();
    assert!(
        exec(&runtime, &format!("SELECT * FROM {table}"), anon)
            .await
            .is_err(),
        "a role-less anonymous user must be denied under enforcement"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn super_user_bypasses_enforcement() {
    let (runtime, config) = runtime_with(config(true, true)).await;
    let (_t1, t2, _alice) = seed_two_tables_and_reader(&runtime).await;
    let admin = admin_identity(&runtime, &config).await;
    // The admin can read the table the reader was never granted.
    assert!(exec(&runtime, &format!("SELECT * FROM {t2}"), admin)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforcement_off_allows_ungranted_reads() {
    let (runtime, _config) = runtime_with(config(false, true)).await;
    let table = unique("t");
    exec_admin(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;
    exec_admin(&runtime, &format!("INSERT INTO {table} VALUES (1)")).await;

    // A role-less identity can still read when enforcement is off (backwards-compatible default).
    let result = exec(&runtime, &format!("SELECT * FROM {table}"), AuthIdentity::empty()).await;
    assert_eq!(total_rows(&result.expect("read allowed when enforce=off")), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn information_schema_is_exempt_from_enforcement() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    let table = unique("t");
    exec_admin(&runtime, &format!("CREATE TABLE {table} (a BIGINT)")).await;

    // A role-less user can still introspect information_schema even with enforcement on.
    let result = exec(
        &runtime,
        "SELECT table_name FROM information_schema.tables",
        AuthIdentity::empty(),
    )
    .await;
    assert!(result.is_ok(), "information_schema must be readable: {result:?}");
}

// --------------------------------------------------------------------------------------------
// Privilege escalation / write gating
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn non_super_user_cannot_manage_auth_or_run_ddl() {
    let (runtime, _config) = runtime_with(config(true, true)).await;
    exec_admin(&runtime, "CREATE ROLE reader").await;
    exec_admin(&runtime, "CREATE USER alice WITH PASSWORD 'pw'").await;
    exec_admin(&runtime, "GRANT ROLE reader TO USER alice").await;
    let alice = runtime
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
        let err = exec(&runtime, sql, alice.clone())
            .await
            .err()
            .unwrap_or_else(|| panic!("non-super should be rejected: {sql}"));
        assert!(
            err.to_string().contains("super-user"),
            "expected super-user error for `{sql}`, got: {err}"
        );
    }

    // Standard DDL is also rejected for a non-super-user.
    assert!(exec(&runtime, &format!("CREATE TABLE {} (a BIGINT)", unique("x")), alice)
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
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

/// Copies the parquet fixture to `<datasets_dir>/<rel>`, creating parent dirs.
fn place_dataset(datasets_dir: &Path, rel: &str) {
    let dst = datasets_dir.join(rel);
    std::fs::create_dir_all(dst.parent().unwrap()).expect("create dataset subdir");
    std::fs::copy(parquet_fixture(), &dst).expect("copy parquet fixture");
}

/// Seeds a `reader` role assigned to a fresh user, returning `(role, user, alice_identity)`.
async fn seed_reader(runtime: &Runtime) -> (String, AuthIdentity) {
    let role = unique("preader");
    let user = unique("palice");
    exec_admin(runtime, &format!("CREATE ROLE {role}")).await;
    exec_admin(runtime, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    exec_admin(runtime, &format!("GRANT ROLE {role} TO USER {user}")).await;
    let identity = runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap();
    (role, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_grant_matches_only_granted_prefix() {
    let config = config(true, true);
    let datasets = config.storage.datasets_dir.clone();
    let root = unique("pathauth");
    place_dataset(&datasets, &format!("{root}/allowed/data.parquet"));
    place_dataset(&datasets, &format!("{root}/blocked/data.parquet"));

    let (runtime, _config) = runtime_with(config).await;
    let (role, alice) = seed_reader(&runtime).await;
    exec_admin(
        &runtime,
        &format!("GRANT SELECT ON PATH '{root}/allowed/*' TO ROLE {role}"),
    )
    .await;

    let allowed = exec(
        &runtime,
        &format!("SELECT * FROM read_parquet('{root}/allowed/*.parquet')"),
        alice.clone(),
    )
    .await;
    assert!(allowed.is_ok(), "granted path should be readable: {allowed:?}");

    let denied = exec(
        &runtime,
        &format!("SELECT * FROM read_parquet('{root}/blocked/*.parquet')"),
        alice,
    )
    .await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "a different path prefix must be denied"
    );

    let _ = std::fs::remove_dir_all(datasets.join(&root));
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_glob_is_segment_aware() {
    let config = config(true, true);
    let datasets = config.storage.datasets_dir.clone();
    let root = unique("pathseg");
    place_dataset(&datasets, &format!("{root}/data/top.parquet"));
    place_dataset(&datasets, &format!("{root}/data/sub/nested.parquet"));
    place_dataset(&datasets, &format!("{root}/rec/sub/nested.parquet"));

    let (runtime, _config) = runtime_with(config).await;
    let (role, alice) = seed_reader(&runtime).await;
    // Single-segment grant: `*` does not cross `/`.
    exec_admin(
        &runtime,
        &format!("GRANT SELECT ON PATH '{root}/data/*' TO ROLE {role}"),
    )
    .await;
    // Recursive grant: `**` crosses `/`.
    exec_admin(
        &runtime,
        &format!("GRANT SELECT ON PATH '{root}/rec/**' TO ROLE {role}"),
    )
    .await;

    // Top-level file under the single-segment grant is allowed.
    assert!(exec(
        &runtime,
        &format!("SELECT * FROM read_parquet('{root}/data/*.parquet')"),
        alice.clone(),
    )
    .await
    .is_ok());

    // A nested path is NOT covered by the single-`*` grant.
    assert!(
        exec(
            &runtime,
            &format!("SELECT * FROM read_parquet('{root}/data/sub/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "a single `*` grant must not cross a path separator"
    );

    // The recursive `**` grant does cover a nested path.
    assert!(exec(
        &runtime,
        &format!("SELECT * FROM read_parquet('{root}/rec/sub/*.parquet')"),
        alice,
    )
    .await
    .is_ok());

    let _ = std::fs::remove_dir_all(datasets.join(&root));
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_deny_wins_over_broad_grant() {
    let config = config(true, true);
    let datasets = config.storage.datasets_dir.clone();
    let root = unique("pathdeny");
    place_dataset(&datasets, &format!("{root}/public/data.parquet"));
    place_dataset(&datasets, &format!("{root}/secret/data.parquet"));

    let (runtime, _config) = runtime_with(config).await;
    let (role, alice) = seed_reader(&runtime).await;
    // Broad SELECT grant, then deny one subtree — the deny must win.
    exec_admin(&runtime, &format!("GRANT SELECT TO ROLE {role}")).await;
    exec_admin(
        &runtime,
        &format!("DENY SELECT ON PATH '{root}/secret/*' TO ROLE {role}"),
    )
    .await;

    assert!(exec(
        &runtime,
        &format!("SELECT * FROM read_parquet('{root}/public/*.parquet')"),
        alice.clone(),
    )
    .await
    .is_ok());

    assert!(
        exec(
            &runtime,
            &format!("SELECT * FROM read_parquet('{root}/secret/*.parquet')"),
            alice,
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied subtree must be rejected despite the broad grant"
    );

    let _ = std::fs::remove_dir_all(datasets.join(&root));
}
