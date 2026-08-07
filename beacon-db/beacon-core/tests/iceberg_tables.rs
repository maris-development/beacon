//! End-to-end tests for Apache Iceberg support, driven through the SQL endpoint
//! (`Runtime::run_query`): the `read_iceberg()` table function, `CREATE EXTERNAL
//! TABLE ... STORED AS ICEBERG`, a join with another format, predicate
//! pushdown, following a table another writer keeps changing, and surviving a
//! restart.
//!
//! Every table here is written by iceberg-rust into its own warehouse and then
//! copied into the datasets directory (see [`common::iceberg_fixture`]), so the
//! absolute paths inside the metadata never match where Beacon reads them —
//! which is the normal case for a table produced by another system.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::iceberg_fixture::IcebergFixture;
use common::{scalar_i64, total_rows, TestRuntime};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};

/// Rewrites the Iceberg table committed under `test-datasets/iceberg-example`,
/// which the Python integration suite copies into a running server's datasets
/// directory. It is ignored by default: it writes into the repository, and the
/// committed table only needs to change when the fixture does.
///
/// ```text
/// cargo test -p beacon-core --test iceberg_tables -- --ignored --exact \
///     regenerate_the_committed_fixture
/// ```
///
/// The warehouse path is fixed (`/tmp/beacon-iceberg-fixture`) so the absolute
/// paths inside the committed metadata are stable and obviously not where the
/// table is read from — which is the point: a real table is written by another
/// system and mounted somewhere else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "writes into test-datasets/; run explicitly to refresh the fixture"]
async fn regenerate_the_committed_fixture() {
    let warehouse = std::path::Path::new("/tmp/beacon-iceberg-fixture");
    let published = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-datasets")
        .canonicalize()
        .expect("test-datasets should exist");
    let _ = std::fs::remove_dir_all(warehouse);
    let _ = std::fs::remove_dir_all(published.join("iceberg-example"));

    let fixture = IcebergFixture::create(warehouse, &published, "iceberg-example").await;
    // A second commit, so the fixture also covers reading a table that grew, and
    // a schema evolution the integration suite can reveal one metadata file at a
    // time.
    fixture.insert("VALUES (5, 'ship', 3.5)").await;
    fixture.add_column("qc_flag").await;
    fixture
        .insert("(id, name, value, qc_flag) VALUES (6, 'ship', 4.0, 1)")
        .await;
    fixture.publish();
}

/// A table name and location unique to the test, so tests stay independent.
fn names(tag: &str) -> (String, String) {
    let rel = format!("iceberg_{tag}_{}", std::process::id());
    let location = IcebergFixture::location(&rel);
    (rel, location)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_iceberg_reads_a_table_by_location() {
    let rt: TestRuntime = common::runtime("iceberg-read-fn").await;
    let (rel, location) = names("fn");
    let _fixture = IcebergFixture::create(rt.tmp_dir(), rt.datasets_dir(), &rel).await;

    assert_eq!(
        scalar_i64(
            &rt.sql(&format!("SELECT count(*) FROM read_iceberg('{location}')"))
                .await
        ),
        4,
        "read_iceberg should see every row of the table"
    );

    // The column values arrive, not just the row count.
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!(
                "SELECT count(*) FROM read_iceberg('{location}') WHERE name = 'argo'"
            ))
            .await
        ),
        2,
    );

    // A location with no Iceberg table under it fails, and says why.
    let error = rt
        .try_sql("SELECT * FROM read_iceberg('not-a-table')")
        .await
        .expect_err("a location without a table should fail");
    assert!(
        error.to_string().contains("Iceberg metadata"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_iceberg_table_reads_joins_and_pushes_down() {
    let rt: TestRuntime = common::runtime("iceberg-external").await;
    let (rel, location) = names("ext");
    let _fixture = IcebergFixture::create(rt.tmp_dir(), rt.datasets_dir(), &rel).await;
    let table = format!("ice_{}", std::process::id());

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICEBERG LOCATION '{location}'"
    ))
    .await;

    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        4,
        "the external Iceberg table should expose every row"
    );

    // The schema came from the table metadata, not from the DDL.
    let columns = rt
        .sql(&format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '{table}' ORDER BY column_name"
        ))
        .await;
    assert_eq!(
        common::column_strings(&columns, 0),
        vec!["id".to_string(), "name".to_string(), "value".to_string()]
    );

    // A join with a CSV file: one query over two formats.
    common::write_file(
        &rt.datasets_dir().join("platforms/kind.csv"),
        "name,kind\nargo,float\nglider,auv\nbuoy,moored\n",
    );
    rt.sql("CREATE EXTERNAL TABLE platforms STORED AS CSV LOCATION 'platforms/'")
        .await;
    let joined = rt
        .sql(&format!(
            "SELECT count(*) FROM {table} i JOIN platforms p ON i.name = p.name \
             WHERE p.kind = 'float'"
        ))
        .await;
    assert_eq!(
        scalar_i64(&joined),
        2,
        "the join should match the two 'argo' rows"
    );

    // The predicate reaches the Iceberg scan, which is what prunes data files
    // from the manifests' statistics.
    let plan = rt
        .sql(&format!("EXPLAIN SELECT * FROM {table} WHERE id > 2"))
        .await;
    let plan_text = common::column_strings(&plan, 1).join("\n");
    assert!(
        plan_text.contains("IcebergTableScan"),
        "the plan should scan Iceberg directly:\n{plan_text}"
    );
    assert!(
        plan_text.contains("predicate:[") && plan_text.contains("id"),
        "the filter should be pushed into the Iceberg scan:\n{plan_text}"
    );

    // And the pruned scan still returns the right rows.
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!("SELECT count(*) FROM {table} WHERE id > 2"))
                .await
        ),
        2
    );

    rt.sql(&format!("DROP TABLE {table}")).await;
    assert_eq!(
        total_rows(
            &rt.sql(&format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'"
            ))
            .await
        ),
        0,
        "DROP TABLE should deregister the Iceberg table"
    );
}

/// A registered Iceberg table follows the table another system keeps writing:
/// new rows *and* a new column show up on the next query, with no restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registered_table_follows_later_commits() {
    let rt: TestRuntime = common::runtime("iceberg-refresh").await;
    let (rel, location) = names("refresh");
    let fixture = IcebergFixture::create(rt.tmp_dir(), rt.datasets_dir(), &rel).await;
    let table = format!("ice_live_{}", std::process::id());

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICEBERG LOCATION '{location}'"
    ))
    .await;
    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        4
    );

    // Another writer appends a snapshot.
    fixture.insert("VALUES (5, 'ship', 3.5)").await;
    fixture.publish();
    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        5,
        "the next query should see the new snapshot"
    );

    // ...and then evolves the schema.
    fixture.add_column("qc_flag").await;
    fixture.publish();
    let columns = rt
        .sql(&format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '{table}' ORDER BY column_name"
        ))
        .await;
    assert_eq!(
        common::column_strings(&columns, 0),
        vec![
            "id".to_string(),
            "name".to_string(),
            "qc_flag".to_string(),
            "value".to_string()
        ],
        "the added column should be visible on the next query, with no restart"
    );
    // The writer then fills the new column on a row it appends. Beacon reads the
    // value, and reports the column as null for the rows written before it.
    fixture
        .insert("(id, name, value, qc_flag) VALUES (6, 'ship', 4.0, 1)")
        .await;
    fixture.publish();
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!("SELECT count(*) FROM {table} WHERE qc_flag = 1"))
                .await
        ),
        1,
        "the value written into the new column should come back"
    );
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!(
                "SELECT count(*) FROM {table} WHERE qc_flag IS NULL"
            ))
            .await
        ),
        5,
        "rows written before the column exists read as null"
    );
}

/// Copy every file under `from` into `store` beneath `prefix`.
async fn upload_dir(store: &Arc<InMemory>, from: &Path, prefix: &str) {
    for entry in std::fs::read_dir(from).expect("read staged table") {
        let entry = entry.expect("read staged entry");
        let name = entry.file_name().to_string_lossy().to_string();
        let target = format!("{prefix}/{name}");
        if entry.file_type().expect("file type").is_dir() {
            Box::pin(upload_dir(store, &entry.path(), &target)).await;
        } else {
            let bytes = std::fs::read(entry.path()).expect("read staged file");
            store
                .put(
                    &object_store::path::Path::from(target),
                    PutPayload::from(bytes),
                )
                .await
                .expect("upload staged file");
        }
    }
}

/// A table that lives only in an object store reads with no local copy.
///
/// This is the S3 path without an S3: the datasets store is an in-memory
/// `ObjectStore`, and the directory the table was written in is deleted before
/// the first query. Nothing but the object store can answer the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_in_an_object_store_reads_with_no_local_copy() {
    let store = Arc::new(InMemory::new());
    let rt = common::runtime_with("iceberg-object-store", {
        let store = store.clone() as Arc<dyn ObjectStore>;
        move |b| b.with_default_object_store(store)
    })
    .await;

    // Write the table on disk, upload it, then take the disk copy away.
    let staging = tempfile::tempdir().expect("staging dir");
    let fixture = IcebergFixture::create(staging.path(), staging.path(), "table").await;
    upload_dir(&store, &staging.path().join("table"), "remote/obs").await;
    drop(fixture);
    staging.close().expect("remove the staged copy");

    assert_eq!(
        scalar_i64(
            &rt.sql("SELECT count(*) FROM read_iceberg('remote/obs')")
                .await
        ),
        4,
        "the table should read from the object store alone"
    );

    let table = format!("ice_remote_{}", std::process::id());
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICEBERG LOCATION 'datasets://remote/obs'"
    ))
    .await;
    assert_eq!(
        scalar_i64(
            &rt.sql(&format!("SELECT count(*) FROM {table} WHERE value > 10"))
                .await
        ),
        2
    );
}

/// The table definition is catalog state: reopening the same tables store must
/// bring the Iceberg table back, still readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_iceberg_table_survives_a_restart() {
    let rt = common::restartable_runtime("iceberg-restart", |b| b).await;
    let (rel, location) = names("restart");
    let _fixture = IcebergFixture::create(rt.tmp_dir(), rt.datasets_dir(), &rel).await;
    let table = format!("ice_persist_{}", std::process::id());

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICEBERG LOCATION '{location}'"
    ))
    .await;
    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        4,
        "sanity: the table reads before the restart"
    );

    let rt = rt.restart().await;

    assert_eq!(
        total_rows(
            &rt.sql(&format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'"
            ))
            .await
        ),
        1,
        "the Iceberg table should still be listed after a restart"
    );
    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        4,
        "the restored Iceberg table should still read its rows"
    );
}

/// Writes are out of scope, so `INSERT INTO` must fail with a reason a user can
/// act on, and must leave the table exactly as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_insert_is_refused_with_a_reason_and_changes_nothing() {
    let rt: TestRuntime = common::runtime("iceberg-readonly").await;
    let (rel, location) = names("readonly");
    let _fixture = IcebergFixture::create(rt.tmp_dir(), rt.datasets_dir(), &rel).await;
    let table = format!("ice_ro_{}", std::process::id());

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICEBERG LOCATION '{location}'"
    ))
    .await;

    let error = rt
        .try_sql(&format!("INSERT INTO {table} VALUES (9, 'x', 1.0)"))
        .await
        .expect_err("an Iceberg table should refuse an INSERT")
        .to_string();
    // Not DataFusion's bare "Insert into not implemented for this table": the
    // message names the table and says what to do instead.
    assert!(
        error.contains(&table),
        "the error should name the table: {error}"
    );
    assert!(
        error.contains("read-only") && error.contains("PyIceberg"),
        "the error should say why, and what to use instead: {error}"
    );

    assert_eq!(
        scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await),
        4,
        "a refused INSERT must not change the table"
    );
}
