//! `CREATE EXTERNAL TABLE ... STORED AS LANCE`: a Lance dataset at a location
//! that the user chooses. Beacon reads and writes it, and `DROP TABLE` keeps
//! its files.

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use common::{scalar_i64, scalar_string, TestRuntime};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

const LOCATION: &str = "ext.lance";

async fn count(rt: &TestRuntime, table: &str) -> i64 {
    scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await)
}

/// The text that a user sees for the spec message `message`. Every error of a
/// Beacon statement has the DataFusion prefix of an external error.
fn visible(message: &str) -> String {
    format!("External error: {message}")
}

/// The error text of a statement that must fail.
async fn error_of(rt: &TestRuntime, sql: &str) -> String {
    rt.try_sql(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("statement should fail: {sql}"))
        .to_string()
}

fn dataset_dir(rt: &TestRuntime) -> std::path::PathBuf {
    rt.datasets_dir().join(LOCATION)
}

async fn create_with_columns(rt: &TestRuntime, table: &str) {
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} (id BIGINT, name VARCHAR) \
         STORED AS LANCE LOCATION '{LOCATION}'"
    ))
    .await;
}

/// Append two rows to the dataset at `dir` with a Lance writer that is not Beacon.
async fn append_with_other_writer(dir: &Path) {
    write_with_other_writer(dir, beacon_lance::WriteKind::Append).await;
}

/// Write two rows to the dataset at `dir` with a Lance writer that is not Beacon.
async fn write_with_other_writer(dir: &Path, kind: beacon_lance::WriteKind) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let rows = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]),
    ));
    let other = beacon_lance::LanceWarehouse::new(Arc::new(object_store::memory::InMemory::new()));
    let uri = url::Url::from_file_path(dir).unwrap().to_string();
    beacon_lance::io::write_stream(&uri, other.session(), rows, kind)
        .await
        .expect("the other writer writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn create_on_dataset_written_by_lance() {
    let rt = common::runtime("lance-ext-existing").await;
    write_with_other_writer(&dataset_dir(&rt), beacon_lance::WriteKind::Create).await;

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t STORED AS LANCE LOCATION '{LOCATION}'"
    ))
    .await;
    assert_eq!(count(&rt, "t").await, 2);
    assert_eq!(
        scalar_string(&rt.sql("SELECT name FROM t WHERE id = 11").await),
        "y"
    );
}

/// A location with a scheme uses the store of that scheme, also when the
/// runtime has a datasets store.
#[tokio::test(flavor = "multi_thread")]
async fn schemed_location_uses_its_own_store() {
    let rt = common::restartable_runtime("lance-ext-schemed", |b| b).await;
    let dir = rt.tmp_dir().join("abs.lance");
    let location = url::Url::from_file_path(&dir).unwrap().to_string();
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t (id BIGINT) STORED AS LANCE LOCATION '{location}'"
    ))
    .await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;
    assert!(dir.join("_versions").is_dir(), "the dataset is at {dir:?}");
    assert!(!rt.datasets_dir().join("abs.lance").exists());

    let rt = rt.restart().await;
    assert_eq!(count(&rt, "t").await, 2);
    let shown = rt.sql("SHOW CREATE TABLE t").await;
    let last = shown[0].num_columns() - 1;
    let stored = common::column_strings(&shown, last);
    assert!(stored[0].contains(&location), "{stored:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn location_without_a_store_fails() {
    let rt = common::runtime("lance-ext-no-store").await;
    let error = error_of(
        &rt,
        "CREATE EXTERNAL TABLE t (id BIGINT) STORED AS LANCE LOCATION 'nostore://bucket/x.lance'",
    )
    .await;
    assert!(
        error.starts_with(&visible(
            "No object store is registered for 'nostore://bucket/x.lance'"
        )),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn glob_location_fails_before_create() {
    let rt = common::runtime("lance-ext-glob").await;
    let error = error_of(
        &rt,
        "CREATE EXTERNAL TABLE t (id BIGINT) STORED AS LANCE LOCATION 'x/*.lance'",
    )
    .await;
    assert_eq!(
        error,
        visible("A Lance location cannot contain a glob: 'x/*.lance'")
    );
    assert!(!rt.datasets_dir().join("x").exists(), "nothing is created");
    assert!(!rt.datasets_dir().join("_versions").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_by_fails() {
    let rt = common::runtime("lance-ext-partitioned").await;
    let error = error_of(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE t (id BIGINT) STORED AS LANCE \
             PARTITIONED BY (id) LOCATION '{LOCATION}'"
        ),
    )
    .await;
    assert_eq!(
        error,
        visible("STORED AS LANCE does not support PARTITIONED BY")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn defaults_and_constraints_fail() {
    let rt = common::runtime("lance-ext-defaults").await;
    for (columns, message) in [
        (
            "id BIGINT DEFAULT 1",
            "STORED AS LANCE does not support DEFAULT values",
        ),
        (
            "id BIGINT PRIMARY KEY",
            "STORED AS LANCE does not support PRIMARY KEY or UNIQUE",
        ),
        (
            "id BIGINT, UNIQUE (id)",
            "STORED AS LANCE does not support PRIMARY KEY or UNIQUE",
        ),
    ] {
        let error = error_of(
            &rt,
            &format!("CREATE EXTERNAL TABLE t ({columns}) STORED AS LANCE LOCATION '{LOCATION}'"),
        )
        .await;
        assert_eq!(error, visible(message), "{columns}");
    }
    assert!(!dataset_dir(&rt).exists(), "nothing is created");
}

#[tokio::test(flavor = "multi_thread")]
async fn create_with_columns_then_read_write() {
    let rt = common::runtime("lance-ext-rw").await;
    create_with_columns(&rt, "t").await;
    assert!(
        dataset_dir(&rt).join("_versions").is_dir(),
        "the dataset is in the datasets store"
    );

    rt.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'b')")
        .await;
    assert_eq!(count(&rt, "t").await, 3);

    rt.sql("DELETE FROM t WHERE name = 'a'").await;
    assert_eq!(count(&rt, "t").await, 2);

    rt.sql("UPDATE t SET name = 'c' WHERE id = 3").await;
    assert_eq!(
        scalar_string(&rt.sql("SELECT name FROM t WHERE id = 3").await),
        "c"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_index_compact_work() {
    let rt = common::runtime("lance-ext-maintenance").await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    rt.sql("ALTER TABLE t ADD COLUMN x DOUBLE").await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM t WHERE x IS NULL").await),
        2
    );
    rt.sql("CREATE INDEX ON t (id)").await;
    assert_eq!(common::total_rows(&rt.sql("SHOW INDEXES ON t").await), 1);
    rt.sql("DROP INDEX t_id_idx ON t").await;
    assert_eq!(common::total_rows(&rt.sql("SHOW INDEXES ON t").await), 0);
    rt.sql("COMPACT TABLE t").await;
    assert_eq!(count(&rt, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn column_list_on_existing_dataset_fails() {
    let rt = common::runtime("lance-ext-column-list").await;
    create_with_columns(&rt, "t").await;
    let error = error_of(
        &rt,
        &format!("CREATE EXTERNAL TABLE t2 (id BIGINT) STORED AS LANCE LOCATION '{LOCATION}'"),
    )
    .await;
    assert_eq!(
        error,
        visible(
            "Lance external tables take their columns from the dataset; remove the column list"
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_dataset_without_columns_fails() {
    let rt = common::runtime("lance-ext-missing").await;
    let error = error_of(
        &rt,
        "CREATE EXTERNAL TABLE t STORED AS LANCE LOCATION 'missing.lance'",
    )
    .await;
    assert_eq!(
        error,
        visible("No Lance dataset at 'missing.lance'; add a column list to create one")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_option_fails() {
    let rt = common::runtime("lance-ext-option").await;
    create_with_columns(&rt, "t").await;
    let error = error_of(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE p STORED AS LANCE LOCATION '{LOCATION}' OPTIONS ('foo' '1')"
        ),
    )
    .await;
    assert_eq!(error, visible("Unknown option 'foo' for a Lance table"));
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_keeps_files_and_recreate_reads_old_rows() {
    let rt = common::runtime("lance-ext-drop").await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    rt.sql("DROP TABLE t").await;
    assert!(
        dataset_dir(&rt).join("_versions").is_dir(),
        "DROP keeps the dataset files"
    );

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t2 STORED AS LANCE LOCATION '{LOCATION}'"
    ))
    .await;
    assert_eq!(count(&rt, "t2").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_keeps_external_table() {
    let rt = common::restartable_runtime("lance-ext-restart", |b| b).await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    let rt = rt.restart().await;
    assert_eq!(count(&rt, "t").await, 2);
    rt.sql("INSERT INTO t VALUES (3, 'c')").await;
    assert_eq!(count(&rt, "t").await, 3);

    rt.sql("DROP TABLE t").await;
    assert!(
        dataset_dir(&rt).join("_versions").is_dir(),
        "DROP after a restart keeps the dataset files"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_version_is_read_only() {
    let rt = common::runtime("lance-ext-pinned").await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a')").await;
    rt.sql("INSERT INTO t VALUES (2, 'b')").await;

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE p STORED AS LANCE LOCATION '{LOCATION}' OPTIONS ('version' '2')"
    ))
    .await;
    assert_eq!(count(&rt, "p").await, 1, "version 2 holds the first insert");
    assert_eq!(count(&rt, "t").await, 2);

    let read_only = visible("Lance table 'p' is pinned to version 2 and is read-only");
    for statement in [
        "INSERT INTO p VALUES (3, 'c')",
        "DELETE FROM p",
        "UPDATE p SET name = 'z'",
        "ALTER TABLE p ADD COLUMN x DOUBLE",
        "CREATE INDEX ON p (id)",
        "DROP INDEX p_id_idx ON p",
        "COMPACT TABLE p",
    ] {
        assert_eq!(error_of(&rt, statement).await, read_only, "{statement}");
    }
    assert_eq!(
        count(&rt, "t").await,
        2,
        "the refused writes change nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_keeps_pinned_version() {
    let rt = common::restartable_runtime("lance-ext-pinned-restart", |b| b).await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a')").await;
    rt.sql("INSERT INTO t VALUES (2, 'b')").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE p STORED AS LANCE LOCATION '{LOCATION}' OPTIONS ('version' '2')"
    ))
    .await;

    let rt = rt.restart().await;
    assert_eq!(count(&rt, "p").await, 1);
    assert_eq!(
        error_of(&rt, "INSERT INTO p VALUES (3, 'c')").await,
        visible("Lance table 'p' is pinned to version 2 and is read-only")
    );
}

/// An unpinned table reads the commits of another writer on the next scan.
#[tokio::test(flavor = "multi_thread")]
async fn scan_sees_commits_of_another_writer() {
    let rt = common::runtime("lance-ext-other-writer").await;
    create_with_columns(&rt, "t").await;
    rt.sql("INSERT INTO t VALUES (1, 'a')").await;
    assert_eq!(count(&rt, "t").await, 1);

    append_with_other_writer(&dataset_dir(&rt)).await;
    assert_eq!(count(&rt, "t").await, 3);

    rt.sql("INSERT INTO t VALUES (2, 'b')").await;
    assert_eq!(
        count(&rt, "t").await,
        4,
        "Beacon appends after the other commit"
    );
}

/// `table_sizes` measures the tables that Beacon stores, not external datasets.
#[tokio::test(flavor = "multi_thread")]
async fn table_sizes_leaves_out_external_tables() {
    let rt = common::runtime("lance-ext-sizes").await;
    create_with_columns(&rt, "t").await;
    rt.sql("CREATE TABLE m (id BIGINT)").await;

    let names = common::column_strings(
        &rt.sql("SELECT table_name FROM beacon.system.table_sizes")
            .await,
        0,
    );
    assert!(names.iter().any(|name| name == "m"), "{names:?}");
    assert!(!names.iter().any(|name| name == "t"), "{names:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_drop_still_deletes() {
    let rt = common::runtime("lance-ext-managed").await;
    rt.sql("CREATE TABLE m (id BIGINT)").await;
    rt.sql("INSERT INTO m VALUES (1), (2)").await;
    rt.sql("DROP TABLE m").await;
    rt.sql("CREATE TABLE m (id BIGINT)").await;
    assert_eq!(count(&rt, "m").await, 0);
}
