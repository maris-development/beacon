//! Integration tests for the authentication + authorization layer, exercised end to end through
//! the real [`Server`](beacon_server::Server) (`run_query` with `AuthIdentity`s).
//!
//! Each test opens its own server over an isolated temp root, so auth state and tables are
//! per-test. Table names are still suffixed with a uuid so a scenario's own statements never
//! collide.

mod common;

use std::path::{Path, PathBuf};

use beacon_auth::{AuthIdentity, Credential};
use beacon_server::server::ANONYMOUS_USERNAME;
use common::{config, server_with, total_rows, unique, TestServer, ADMIN_USERNAME};

/// Opens a server with explicit auth settings. `enforce` gates table/path grants for
/// non-super-users; `anonymous_enabled` seeds and enables the anonymous principal.
async fn auth_server(enforce: bool, anonymous_enabled: bool) -> TestServer {
    let mut config = config(enforce);
    config.auth.anonymous_enabled = anonymous_enabled;
    server_with(config).await
}

/// `(schema, table)` for every row of a catalog listing.
fn qualified_names(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(String, String)> {
    use arrow::array::AsArray;

    let mut rows = Vec::new();
    for batch in batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|array| array.as_string_opt::<i32>())
                .expect("the listing carries the information_schema columns")
        };
        let (schemas, tables) = (column("table_schema"), column("table_name"));
        for row in 0..batch.num_rows() {
            rows.push((
                schemas.value(row).to_string(),
                tables.value(row).to_string(),
            ));
        }
    }
    rows
}

/// Just the table names of a catalog listing.
fn table_names(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    qualified_names(batches)
        .into_iter()
        .map(|(_, table)| table)
        .collect()
}

// --------------------------------------------------------------------------------------------
// Authentication
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_credentials_authenticate_as_super_user() {
    let ts = auth_server(false, true).await;
    let identity = ts.admin().await;
    assert_eq!(identity.username, ADMIN_USERNAME);
    assert!(
        identity.is_super_user,
        "the bootstrapped admin is a super-user"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_password_and_unknown_user_are_rejected() {
    let ts = auth_server(false, true).await;
    assert!(ts
        .server
        .authenticate(&Credential::basic(ADMIN_USERNAME, "wrong"))
        .await
        .is_err());
    assert!(ts
        .server
        .authenticate(&Credential::basic("ghost", "whatever"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_credential_without_oidc_is_rejected() {
    // OIDC is disabled by default, so the local provider rejects bearer tokens.
    let ts = auth_server(false, true).await;
    assert!(ts
        .server
        .authenticate(&Credential::bearer("not-a-jwt"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_enabled_resolves_to_roleless_identity() {
    let ts = auth_server(false, true).await;
    assert!(ts.server.auth().anonymous_enabled());
    let identity = ts
        .server
        .authenticate_anonymous()
        .await
        .expect("anonymous should resolve");
    assert!(identity.roles.is_empty());
    assert!(!identity.is_super_user);
}

/// The anonymous principal has no password, so a Basic login as it must fail. Otherwise
/// `anonymous:` with an empty password gets its roles even with anonymous access off.
#[tokio::test(flavor = "multi_thread")]
async fn a_basic_login_as_the_anonymous_user_is_rejected() {
    for anonymous_enabled in [true, false] {
        let ts = auth_server(false, anonymous_enabled).await;
        assert!(
            ts.server
                .authenticate(&Credential::basic(ANONYMOUS_USERNAME, ""))
                .await
                .is_err(),
            "anonymous enabled: {anonymous_enabled}"
        );
    }
}

/// An empty password is no password, so a Basic login with one fails for every user.
#[tokio::test(flavor = "multi_thread")]
async fn a_basic_login_with_an_empty_password_is_rejected() {
    let ts = auth_server(false, true).await;
    let user = unique("nopw");
    ts.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD ''"),
        AuthIdentity::system(),
    )
    .await;

    assert!(ts
        .server
        .authenticate(&Credential::basic(user, ""))
        .await
        .is_err());
    // The anonymous path itself still resolves.
    assert!(ts.server.authenticate_anonymous().await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_disabled_errors() {
    let ts = auth_server(false, false).await;
    assert!(!ts.server.auth().anonymous_enabled());
    assert!(ts.server.authenticate_anonymous().await.is_err());
}

// --------------------------------------------------------------------------------------------
// User / role lifecycle via SQL
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn create_user_role_and_grant_then_authenticate() {
    let ts = auth_server(false, true).await;
    ts.sql_as("CREATE ROLE reader", AuthIdentity::system())
        .await;
    ts.sql_as(
        "CREATE USER alice WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;

    let identity = ts
        .server
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice should authenticate");
    assert_eq!(identity.username, "alice");
    assert_eq!(identity.roles, vec!["reader".to_string()]);
    assert!(!identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_user_revokes_authentication() {
    let ts = auth_server(false, true).await;
    ts.sql_as("CREATE USER bob WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    assert!(ts
        .server
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_ok());

    ts.sql_as("DROP USER bob", AuthIdentity::system()).await;
    assert!(ts
        .server
        .authenticate(&Credential::basic("bob", "pw"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn created_users_and_roles_are_never_super_user() {
    let ts = auth_server(false, true).await;
    ts.sql_as("CREATE ROLE owners", AuthIdentity::system())
        .await;
    ts.sql_as(
        "CREATE USER root WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as("GRANT ROLE owners TO USER root", AuthIdentity::system())
        .await;

    // No SQL-created user is ever a super-user, whatever roles they hold.
    let identity = ts.login("root", "pw").await;
    assert!(!identity.is_super_user);

    // Write/management privileges cannot be granted to a role — roles are strictly read-only, so
    // there is no way to mint another super-user through SQL.
    for sql in [
        "GRANT ALL TO ROLE owners",
        "GRANT INSERT ON TABLE t TO ROLE owners",
        "GRANT DROP ON TABLE t TO ROLE owners",
    ] {
        let err = ts.try_sql_as(sql, AuthIdentity::system()).await;
        assert!(
            err.is_err_and(|e| e.to_string().contains("read-only")),
            "granting write privileges to a role should be rejected: {sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_super_user_is_config_defined_and_reserved() {
    let ts = auth_server(false, true).await;

    // The configured admin credential is the one and only super-user.
    assert!(ts.admin().await.is_super_user);

    // Its username is reserved: it cannot be created or dropped as a stored user via SQL.
    // (Quoted because the admin username contains a hyphen.)
    let created = ts
        .try_sql_as(
            &format!("CREATE USER \"{ADMIN_USERNAME}\" WITH PASSWORD 'x'"),
            AuthIdentity::system(),
        )
        .await;
    assert!(
        created.is_err_and(|e| e.to_string().contains("reserved super-user")),
        "creating the super-user as a stored user should be rejected"
    );

    let dropped = ts
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
async fn seed_two_tables_and_reader(ts: &TestServer) -> (String, String, AuthIdentity) {
    let t1 = unique("t1");
    let t2 = unique("t2");
    ts.sql_as(
        &format!("CREATE TABLE {t1} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {t1} VALUES (1), (2)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("CREATE TABLE {t2} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {t2} VALUES (3)"),
        AuthIdentity::system(),
    )
    .await;

    let role = unique("reader");
    let user = unique("alice");
    ts.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    ts.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT SELECT ON TABLE {t1} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let identity = ts.login(&user, "pw").await;
    (t1, t2, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_table_grant_allows_only_the_granted_table() {
    let ts = auth_server(true, true).await;
    let (t1, t2, alice) = seed_two_tables_and_reader(&ts).await;

    let allowed = ts
        .try_sql_as(&format!("SELECT * FROM {t1}"), alice.clone())
        .await;
    assert_eq!(total_rows(&allowed.expect("granted table is readable")), 2);

    let denied = ts.try_sql_as(&format!("SELECT * FROM {t2}"), alice).await;
    assert!(
        denied.is_err_and(|e| e.to_string().contains("permission denied")),
        "ungranted table must be denied"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_rules_on_quoted_table_names_match() {
    let ts = auth_server(true, true).await;
    let tables = ["\"MixedCase\"", "\"with-hyphen\"", "\"2024\"", "\"select\""];
    for table in tables.iter().chain(["\"Closed-One\""].iter()) {
        ts.sql_as(
            &format!("CREATE TABLE {table} (a BIGINT)"),
            AuthIdentity::system(),
        )
        .await;
        ts.sql_as(
            &format!("INSERT INTO {table} VALUES (1)"),
            AuthIdentity::system(),
        )
        .await;
    }
    for statement in [
        "CREATE ROLE quoted_reader".to_string(),
        "CREATE USER quoted_alice WITH PASSWORD 'pw'".to_string(),
        "GRANT ROLE quoted_reader TO USER quoted_alice".to_string(),
    ]
    .into_iter()
    .chain(
        tables
            .iter()
            .map(|t| format!("GRANT SELECT ON TABLE {t} TO ROLE quoted_reader")),
    ) {
        ts.sql_as(&statement, AuthIdentity::system()).await;
    }
    let alice = ts.login("quoted_alice", "pw").await;

    for table in tables {
        let read = ts
            .try_sql_as(&format!("SELECT * FROM {table}"), alice.clone())
            .await;
        assert_eq!(
            total_rows(&read.unwrap_or_else(|e| panic!("{table}: {e}"))),
            1,
            "{table}"
        );
    }
    let closed = ts
        .try_sql_as("SELECT * FROM \"Closed-One\"", alice.clone())
        .await;
    assert!(closed.is_err_and(|e| e.to_string().contains("permission denied")));

    // A deny on a quoted name blocks, and a revoke with the same spelling removes the grant.
    ts.sql_as(
        "DENY SELECT ON TABLE \"with-hyphen\" TO ROLE quoted_reader",
        AuthIdentity::system(),
    )
    .await;
    let denied = ts
        .try_sql_as("SELECT * FROM \"with-hyphen\"", alice.clone())
        .await;
    assert!(denied.is_err_and(|e| e.to_string().contains("permission denied")));
    ts.sql_as(
        "REVOKE SELECT ON TABLE \"MixedCase\" FROM ROLE quoted_reader",
        AuthIdentity::system(),
    )
    .await;
    let revoked = ts.try_sql_as("SELECT * FROM \"MixedCase\"", alice).await;
    assert!(revoked.is_err_and(|e| e.to_string().contains("permission denied")));
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_deny_wins_over_grant() {
    let ts = auth_server(true, true).await;
    let t1 = unique("t1");
    let t2 = unique("t2");
    ts.sql_as(
        &format!("CREATE TABLE {t1} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {t1} VALUES (1)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("CREATE TABLE {t2} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {t2} VALUES (2)"),
        AuthIdentity::system(),
    )
    .await;

    let role = unique("reader");
    let user = unique("alice");
    ts.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    ts.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    // Grant SELECT on everything, then deny one table — deny must win.
    ts.sql_as(
        &format!("GRANT SELECT TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("DENY SELECT ON TABLE {t2} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let alice = ts.login(&user, "pw").await;

    assert!(ts
        .try_sql_as(&format!("SELECT * FROM {t1}"), alice.clone())
        .await
        .is_ok());
    assert!(
        ts.try_sql_as(&format!("SELECT * FROM {t2}"), alice)
            .await
            .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied table must be rejected even with a broad grant"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_revoke_removes_access() {
    let ts = auth_server(true, true).await;
    let (t1, _t2, alice) = seed_two_tables_and_reader(&ts).await;

    // A self-contained scenario: seed_* used unique names, so grant via a fresh role.
    let role = unique("r");
    let user = unique("u");
    let table = unique("tg");
    ts.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {table} VALUES (1)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    ts.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT SELECT ON TABLE {table} TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    let u = ts.login(&user, "pw").await;

    assert!(ts
        .try_sql_as(&format!("SELECT * FROM {table}"), u.clone())
        .await
        .is_ok());

    ts.sql_as(
        &format!("REVOKE SELECT ON TABLE {table} FROM ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    assert!(
        ts.try_sql_as(&format!("SELECT * FROM {table}"), u)
            .await
            .is_err(),
        "access must be gone after REVOKE"
    );

    // (t1/alice from the shared helper remain readable — sanity that seeding is independent.)
    assert!(ts
        .try_sql_as(&format!("SELECT * FROM {t1}"), alice)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_anonymous_has_no_read_access() {
    let ts = auth_server(true, true).await;
    let table = unique("t");
    ts.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {table} VALUES (1)"),
        AuthIdentity::system(),
    )
    .await;

    let anon = ts.server.authenticate_anonymous().await.unwrap();
    assert!(
        ts.try_sql_as(&format!("SELECT * FROM {table}"), anon)
            .await
            .is_err(),
        "a role-less anonymous user must be denied under enforcement"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn super_user_bypasses_enforcement() {
    let ts = auth_server(true, true).await;
    let (_t1, t2, _alice) = seed_two_tables_and_reader(&ts).await;
    let admin = ts.admin().await;
    // The admin can read the table the reader was never granted.
    assert!(ts
        .try_sql_as(&format!("SELECT * FROM {t2}"), admin)
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforcement_off_allows_ungranted_reads() {
    let ts = auth_server(false, true).await;
    let table = unique("t");
    ts.sql_as(
        &format!("CREATE TABLE {table} (a BIGINT)"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("INSERT INTO {table} VALUES (1)"),
        AuthIdentity::system(),
    )
    .await;

    // A role-less identity can still read when enforcement is off (backwards-compatible default).
    let result = ts
        .try_sql_as(&format!("SELECT * FROM {table}"), AuthIdentity::empty())
        .await;
    assert_eq!(
        total_rows(&result.expect("read allowed when enforce=off")),
        1
    );
}

/// The metadata schemas are the super-user's, and a regular caller's catalog
/// listing is built for them instead — showing only the tables their roles grant.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_schemas_are_super_user_only_and_listings_are_role_filtered() {
    let ts = auth_server(true, true).await;
    let granted = unique("granted");
    let hidden = unique("hidden");
    for table in [&granted, &hidden] {
        ts.sql_as(
            &format!("CREATE TABLE {table} (a BIGINT)"),
            AuthIdentity::system(),
        )
        .await;
    }
    ts.sql_as("CREATE ROLE reader", AuthIdentity::system())
        .await;
    ts.sql_as(
        &format!("GRANT SELECT ON TABLE {granted} TO ROLE reader"),
        AuthIdentity::system(),
    )
    .await;
    let reader = AuthIdentity {
        username: "reader-user".to_string(),
        roles: vec!["reader".to_string()],
        is_super_user: false,
    };

    // Neither metadata schema is readable, however it is reached.
    for sql in [
        "SELECT table_name FROM information_schema.tables",
        "SHOW TABLES",
        "SELECT count(*) FROM beacon.system.query_metrics",
    ] {
        let err = ts
            .try_sql_as(sql, reader.clone())
            .await
            .err()
            .unwrap_or_else(|| panic!("non-super read should be rejected: {sql}"));
        assert!(
            err.to_string().contains("super-user"),
            "expected a super-user error for `{sql}`, got: {err}"
        );
    }

    // The listing they get instead holds the granted table and not the other one.
    let runtime = ts.server.runtime();
    let visible = table_names(
        &runtime
            .visible_tables(ts.server.validator(reader).as_ref())
            .await
            .expect("listing succeeds for a non-super caller"),
    );
    assert!(
        visible.contains(&granted),
        "a granted table must be listed, got: {visible:?}"
    );
    assert!(
        !visible.contains(&hidden),
        "an ungranted table must not be listed, got: {visible:?}"
    );
    assert!(
        !visible.iter().any(|table| table == "functions"),
        "the metadata schemas must not appear in a listing, got: {visible:?}"
    );

    // The super-user still sees everything, metadata schemas included.
    let batches = runtime
        .visible_tables(ts.server.validator(AuthIdentity::system()).as_ref())
        .await
        .expect("listing succeeds for the super-user");
    let all = table_names(&batches);
    assert!(all.contains(&hidden));
    assert!(qualified_names(&batches)
        .iter()
        .any(|(schema, table)| schema == "information_schema" && table == "tables"));
}

// --------------------------------------------------------------------------------------------
// Privilege escalation / write gating
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn non_super_user_cannot_manage_auth_or_run_ddl() {
    let ts = auth_server(true, true).await;
    ts.sql_as("CREATE ROLE reader", AuthIdentity::system())
        .await;
    ts.sql_as(
        "CREATE USER alice WITH PASSWORD 'pw'",
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as("GRANT ROLE reader TO USER alice", AuthIdentity::system())
        .await;
    let alice = ts.login("alice", "pw").await;

    // The server applies the auth statements, and only for a super-user.
    for sql in [
        "CREATE ROLE hacker",
        "GRANT ALL TO ROLE reader",
        "CREATE USER bob WITH PASSWORD 'pw'",
        "DROP USER alice",
    ] {
        let err = ts
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
    assert!(ts
        .try_sql_as(&format!("CREATE TABLE {} (a BIGINT)", unique("x")), alice)
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
    // `beacon-server/beacon-server` -> up two, to the workspace root that holds
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
async fn seed_reader(ts: &TestServer) -> (String, AuthIdentity) {
    let role = unique("preader");
    let user = unique("palice");
    ts.sql_as(&format!("CREATE ROLE {role}"), AuthIdentity::system())
        .await;
    ts.sql_as(
        &format!("CREATE USER {user} WITH PASSWORD 'pw'"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("GRANT ROLE {role} TO USER {user}"),
        AuthIdentity::system(),
    )
    .await;
    let identity = ts.login(&user, "pw").await;
    (role, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_grant_matches_only_granted_prefix() {
    let ts = auth_server(true, true).await;
    let root = unique("pathauth");
    place_dataset(ts.datasets_dir(), &format!("{root}/allowed/data.parquet"));
    place_dataset(ts.datasets_dir(), &format!("{root}/blocked/data.parquet"));

    let (role, alice) = seed_reader(&ts).await;
    ts.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/allowed/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    let allowed = ts
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/allowed/*.parquet')"),
            alice.clone(),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "granted path should be readable: {allowed:?}"
    );

    let denied = ts
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
    let ts = auth_server(true, true).await;
    let root = unique("pathseg");
    place_dataset(ts.datasets_dir(), &format!("{root}/data/top.parquet"));
    place_dataset(
        ts.datasets_dir(),
        &format!("{root}/data/sub/nested.parquet"),
    );
    place_dataset(ts.datasets_dir(), &format!("{root}/rec/sub/nested.parquet"));

    let (role, alice) = seed_reader(&ts).await;
    // Single-segment grant: `*` does not cross `/`.
    ts.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/data/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    // Recursive grant: `**` crosses `/`.
    ts.sql_as(
        &format!("GRANT SELECT ON PATH '{root}/rec/**' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    // Top-level file under the single-segment grant is allowed. A `data/*.parquet` glob is not:
    // the listing lets `*` cross `/`, so it also reads `data/sub/nested.parquet`.
    assert!(ts
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/data/top.parquet')"),
            alice.clone(),
        )
        .await
        .is_ok());

    // A nested path is NOT covered by the single-`*` grant.
    assert!(
        ts.try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/data/sub/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "a single `*` grant must not cross a path separator"
    );

    // The recursive `**` grant does cover a nested path.
    assert!(ts
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/rec/sub/*.parquet')"),
            alice,
        )
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn enforced_path_deny_wins_over_broad_grant() {
    let ts = auth_server(true, true).await;
    let root = unique("pathdeny");
    place_dataset(ts.datasets_dir(), &format!("{root}/public/data.parquet"));
    place_dataset(ts.datasets_dir(), &format!("{root}/secret/data.parquet"));

    let (role, alice) = seed_reader(&ts).await;
    // Broad SELECT grant, then deny one subtree — the deny must win.
    ts.sql_as(
        &format!("GRANT SELECT TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;
    ts.sql_as(
        &format!("DENY SELECT ON PATH '{root}/secret/*' TO ROLE {role}"),
        AuthIdentity::system(),
    )
    .await;

    assert!(ts
        .try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/public/*.parquet')"),
            alice.clone(),
        )
        .await
        .is_ok());

    assert!(
        ts.try_sql_as(
            &format!("SELECT * FROM read_parquet('{root}/secret/*.parquet')"),
            alice,
        )
        .await
        .is_err_and(|e| e.to_string().contains("permission denied")),
        "the denied subtree must be rejected despite the broad grant"
    );
}
