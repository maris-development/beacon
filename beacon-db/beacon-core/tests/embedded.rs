//! Integration coverage for the embedded entry point ([`beacon_core::embedded`]).
//!
//! These tests pin the contract the Python bindings depend on: opening a database in-process,
//! and the two auth modes behaving as documented — auth off means a local super-user with
//! enforcement off (everything works, no credentials), auth on means anonymous-until-proven
//! otherwise. They go through `Database` only, never `RuntimeBuilder`, because that is exactly
//! what an embedder sees.

use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use beacon_core::embedded::{AuthMode, Database, DbPath, OpenOptions};
use beacon_core::{AuthIdentity, Credential};
use futures::TryStreamExt;
use tempfile::TempDir;

const ADMIN_USERNAME: &str = "test-admin";
const ADMIN_PASSWORD: &str = "test-admin-password";

/// Opens an in-memory database with the given auth mode. Crawlers are disabled: nothing here
/// needs them, and a test process should spawn no background schedulers.
async fn open_memory(auth: AuthMode) -> Database {
    let mut options = OpenOptions::new().with_auth(auth);
    options.crawlers.enable = false;
    Database::open(DbPath::Memory, options)
        .await
        .expect("open in-memory database")
}

/// Runs SQL as `identity`, collecting the streamed batches.
async fn try_sql(
    db: &Database,
    sql: &str,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<RecordBatch>> {
    db.sql(sql, identity)
        .await?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await
        .map_err(Into::into)
}

async fn sql(db: &Database, sql_text: &str, identity: AuthIdentity) -> Vec<RecordBatch> {
    try_sql(db, sql_text, identity)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql_text}\n{e}"))
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// --------------------------------------------------------------------------------------------
// Auth disabled — the default: no credentials, full access
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn default_mode_is_auth_disabled_local_super_user() {
    let db = open_memory(AuthMode::default()).await;

    assert!(!db.auth_enabled(), "auth is off by default");
    let identity = db
        .default_identity()
        .expect("a credential-less identity exists when auth is disabled");
    assert_eq!(identity.username, beacon_core::beacon_auth::LOCAL_USERNAME);
    assert!(
        identity.is_super_user,
        "the local embedded identity is a super-user, or DDL would be rejected"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_disabled_permits_ddl_dml_and_reads() {
    let db = open_memory(AuthMode::Disabled).await;
    let me = db.require_default_identity().unwrap();

    // The read path.
    let rows = sql(&db, "SELECT 1 AS a", me.clone()).await;
    assert_eq!(total_rows(&rows), 1);

    // DDL: rejected outright for a non-super identity, so this proves the local identity
    // clears `validate_query_plan`.
    sql(&db, "CREATE VIEW v AS SELECT 42 AS answer", me.clone()).await;
    let rows = sql(&db, "SELECT answer FROM v", me.clone()).await;
    assert_eq!(total_rows(&rows), 1);

    // The internal auth tables are super-user-only *always*, independent of enforcement —
    // reading them is the sharpest available check that this identity really is privileged.
    let users = sql(&db, "SELECT * FROM beacon.system.users", me).await;
    assert!(
        !users.is_empty(),
        "the local identity can read the auth directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_disabled_rejects_credentials_instead_of_ignoring_them() {
    let db = open_memory(AuthMode::Disabled).await;

    let err = db
        .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
        .await
        .expect_err("credentials must not be silently accepted when auth is disabled");
    let message = err.to_string();
    assert!(
        message.contains("auth disabled"),
        "the error should explain the mode, got: {message}"
    );
}

// --------------------------------------------------------------------------------------------
// Auth enabled — anonymous until credentials are supplied
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn auth_enabled_defaults_to_anonymous_and_blocks_privileged_work() {
    let db = open_memory(AuthMode::enabled_with_admin(ADMIN_USERNAME, ADMIN_PASSWORD)).await;

    assert!(db.auth_enabled());
    let anon = db
        .require_default_identity()
        .expect("anonymous access is enabled by default");
    assert_eq!(anon.username, beacon_core::beacon_auth::ANONYMOUS_USERNAME);
    assert!(!anon.is_super_user);

    // Reads that touch no protected object still work.
    assert_eq!(total_rows(&sql(&db, "SELECT 1 AS a", anon.clone()).await), 1);

    // DDL requires the super-user.
    let err = try_sql(&db, "CREATE VIEW v AS SELECT 1", anon.clone())
        .await
        .expect_err("anonymous may not run DDL");
    assert!(
        err.to_string().contains("not permitted"),
        "unexpected error: {err}"
    );

    // The auth directory is fenced off regardless of enforcement.
    let err = try_sql(&db, "SELECT * FROM beacon.system.users", anon)
        .await
        .expect_err("anonymous may not read the auth directory");
    assert!(
        err.to_string().contains("permission denied"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_enabled_admin_credentials_unlock_full_access() {
    let db = open_memory(AuthMode::enabled_with_admin(ADMIN_USERNAME, ADMIN_PASSWORD)).await;

    let admin = db
        .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
        .await
        .expect("admin credentials authenticate");
    assert!(admin.is_super_user);

    sql(&db, "CREATE VIEW v AS SELECT 7 AS lucky", admin.clone()).await;
    assert_eq!(
        total_rows(&sql(&db, "SELECT lucky FROM v", admin).await),
        1,
        "the admin can create and read a view"
    );

    // Wrong password stays rejected.
    assert!(db
        .authenticate(&Credential::basic(ADMIN_USERNAME, "wrong"))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_enabled_without_anonymous_makes_credentials_mandatory() {
    let db = open_memory(
        AuthMode::enabled_with_admin(ADMIN_USERNAME, ADMIN_PASSWORD).without_anonymous(),
    )
    .await;

    assert!(db.default_identity().is_none());
    let err = db
        .require_default_identity()
        .expect_err("a credential-less session must be refused, not downgraded to an empty identity");
    assert!(
        err.to_string().contains("requires credentials"),
        "unexpected error: {err}"
    );

    // Credentials still work.
    assert!(db
        .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
        .await
        .is_ok());
}

// --------------------------------------------------------------------------------------------
// The container file
// --------------------------------------------------------------------------------------------

/// A file-backed database is one `beacon.db`: what one session creates, the next session sees.
/// This is the property the whole single-file story rests on, reached through the embedded API.
#[tokio::test(flavor = "multi_thread")]
async fn file_database_persists_across_reopen() {
    let root = TempDir::new().expect("temp root");
    let db_file: PathBuf = root.path().join("beacon.db");

    let mut options = OpenOptions::new();
    options.crawlers.enable = false;

    {
        let db = Database::open(db_file.clone(), options.clone())
            .await
            .expect("create database file");
        let me = db.require_default_identity().unwrap();
        sql(
            &db,
            "CREATE TABLE persisted AS SELECT 1 AS id, 'kept' AS label",
            me.clone(),
        )
        .await;
        assert_eq!(
            total_rows(&sql(&db, "SELECT * FROM persisted", me).await),
            1
        );
    } // dropped -> the exclusive lock on beacon.db is released

    assert!(db_file.exists(), "the container file was created");

    let reopened = Database::open(db_file, options)
        .await
        .expect("reopen the same database file");
    let me = reopened.require_default_identity().unwrap();
    let rows = sql(&reopened, "SELECT * FROM persisted", me).await;
    assert_eq!(
        total_rows(&rows),
        1,
        "the table survived the reopen — the catalog and data live in the file"
    );
}

/// Opening a file a live `Database` already holds must fail with a message naming the file,
/// rather than a bare redb lock error. One process, one open container.
#[tokio::test(flavor = "multi_thread")]
async fn second_open_of_a_locked_file_fails_with_a_useful_message() {
    let root = TempDir::new().expect("temp root");
    let db_file = root.path().join("beacon.db");

    let mut options = OpenOptions::new();
    options.crawlers.enable = false;

    let _held = Database::open(db_file.clone(), options.clone())
        .await
        .expect("first open succeeds");

    let err = Database::open(db_file.clone(), options)
        .await
        .expect_err("the file is already locked");
    let message = format!("{err:#}");
    assert!(
        message.contains(&db_file.display().to_string()),
        "the error should name the locked file, got: {message}"
    );
}
