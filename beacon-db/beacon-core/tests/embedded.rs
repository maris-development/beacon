//! Integration coverage for the embedded entry point ([`beacon_core::embedded`]).
//!
//! These tests pin the contract the Python bindings depend on: opening a database in-process,
//! with full access for the process that opens it. They go through `Database` only, never
//! `RuntimeBuilder`, because that is exactly what an embedder sees.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use beacon_core::embedded::{Database, DbPath, OpenOptions};
use futures::TryStreamExt;
use tempfile::TempDir;

/// Opens an in-memory database. Crawlers are disabled: nothing here needs them, and a test
/// process should spawn no background schedulers.
async fn open_memory() -> Database {
    let mut options = OpenOptions::new();
    options.crawlers.enable = false;
    Database::open(DbPath::Memory, options)
        .await
        .expect("open in-memory database")
}

/// Runs SQL, collecting the streamed batches.
async fn try_sql(db: &Database, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    db.sql(sql)
        .await?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await
        .map_err(Into::into)
}

async fn sql(db: &Database, sql_text: &str) -> Vec<RecordBatch> {
    try_sql(db, sql_text)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql_text}\n{e}"))
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// --------------------------------------------------------------------------------------------
// Access — possession of the database is full control
// --------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_database_permits_ddl_dml_and_reads() {
    let db = open_memory().await;

    // The read path.
    assert_eq!(total_rows(&sql(&db, "SELECT 1 AS a").await), 1);

    // DDL and DML.
    sql(&db, "CREATE TABLE t (a BIGINT)").await;
    sql(&db, "INSERT INTO t VALUES (1), (2)").await;
    sql(&db, "CREATE VIEW v AS SELECT a FROM t").await;
    assert_eq!(total_rows(&sql(&db, "SELECT a FROM v").await), 2);

    // The metadata schemas are readable too.
    let tables = sql(&db, "SELECT * FROM information_schema.tables").await;
    assert!(total_rows(&tables) > 0);
}

/// The engine has no users. A statement that manages them is not a statement of the engine.
#[tokio::test(flavor = "multi_thread")]
async fn the_database_has_no_user_statements() {
    let db = open_memory().await;

    for statement in [
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT SELECT ON TABLE t TO ROLE reader",
    ] {
        assert!(try_sql(&db, statement).await.is_err(), "{statement}");
    }
    assert!(try_sql(&db, "SELECT * FROM beacon.system.users")
        .await
        .is_err());
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
        sql(
            &db,
            "CREATE TABLE persisted AS SELECT 1 AS id, 'kept' AS label",
        )
        .await;
        assert_eq!(
            total_rows(&sql(&db, "SELECT * FROM persisted").await),
            1
        );
    } // dropped -> the exclusive lock on beacon.db is released

    assert!(db_file.exists(), "the container file was created");

    let reopened = Database::open(db_file, options)
        .await
        .expect("reopen the same database file");
    let rows = sql(&reopened, "SELECT * FROM persisted").await;
    assert_eq!(
        total_rows(&rows),
        1,
        "the table survived the reopen — the catalog and data live in the file"
    );
}

/// A registered in-memory table is queryable by its bare name, and — crucially — is
/// **session-scoped**: it is a `MemTable`, not a beacon-managed table, so it must never be
/// persisted into `beacon.db`. This pins the property `Connection.register(...)` depends on.
#[tokio::test(flavor = "multi_thread")]
async fn registered_table_is_queryable_but_not_persisted() {
    let root = TempDir::new().expect("temp root");
    let db_file: PathBuf = root.path().join("beacon.db");

    let mut options = OpenOptions::new();
    options.crawlers.enable = false;

    let arrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    {
        let db = Database::open(db_file.clone(), options.clone())
            .await
            .expect("open database");
        db.register_batches("py_df", arrow_schema.clone(), vec![batch])
            .expect("register in-memory table");

        let rows = sql(&db, "SELECT sum(a) AS s FROM py_df").await;
        assert_eq!(total_rows(&rows), 1, "the registered table is queryable by name");

        assert!(db.deregister_table("py_df").unwrap(), "unregister reports it existed");
        assert!(!db.deregister_table("py_df").unwrap(), "second unregister is a no-op");
    } // drop -> release the lock

    // Reopen: the registered table must be gone (it was never written into the file).
    let reopened = Database::open(db_file, options).await.expect("reopen");
    let err = try_sql(&reopened, "SELECT * FROM py_df")
        .await
        .expect_err("a session-only table must not survive a reopen");
    assert!(
        err.to_string().contains("py_df"),
        "unexpected error: {err}"
    );
}

/// `persist_batches` writes a managed table into `beacon.db`, so — unlike a registered
/// `MemTable` — it survives a reopen. The counterpart to
/// `registered_table_is_queryable_but_not_persisted`.
#[tokio::test(flavor = "multi_thread")]
async fn persisted_batches_survive_a_reopen() {
    let root = TempDir::new().expect("temp root");
    let db_file: PathBuf = root.path().join("beacon.db");

    let mut options = OpenOptions::new();
    options.crawlers.enable = false;

    let arrow_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )
    .unwrap();

    {
        let db = Database::open(db_file.clone(), options.clone())
            .await
            .expect("open database");
        db.persist_batches("kept", arrow_schema.clone(), vec![batch])
            .await
            .expect("persist batches into a managed table");
        let rows = sql(&db, "SELECT sum(x) AS s FROM kept").await;
        assert_eq!(total_rows(&rows), 1);
    } // drop -> release the lock

    let reopened = Database::open(db_file, options).await.expect("reopen");
    let rows = sql(&reopened, "SELECT sum(x) AS s FROM kept").await;
    assert_eq!(
        total_rows(&rows),
        1,
        "the persisted table was written into the file and survives the reopen"
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

/// An embedder reads HDF5 with the pure-Rust reader, as the server does.
///
/// The probe is `nested-groups.h5`, which keeps its datasets two group levels deep. Only
/// the pure-Rust reader reports the two nested columns.
#[tokio::test(flavor = "multi_thread")]
async fn the_embedded_default_reads_hdf5_with_the_rust_reader() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("beacon-db")
        .join("beacon-file-formats/beacon-arrow-hdf5/test_files/nested-groups.h5");

    let db = open_memory().await;
    let batches = sql(
        &db,
        &format!(
            r#"SELECT station_id, "observations/temperature", "observations/qc/flag"
               FROM read_hdf5('{}')"#,
            fixture.display()
        ),
    )
    .await;

    assert_eq!(total_rows(&batches), 12, "3 stations x 4 samples");
    assert_eq!(
        batches[0].num_columns(),
        3,
        "netcdf-c reports only the root group, so a nested column would be absent"
    );
}
