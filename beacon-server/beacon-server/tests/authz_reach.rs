//! Read authorization checks what a scan reaches, not how the query spells it.
//!
//! A glob lets `*` cross `/` in a listing, so the text of a path says little about the files
//! it reads. Each test here spells a read so that its text passes the old text check while the
//! files it reaches must not be read.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use beacon_auth::AuthIdentity;
use common::{config, server_with, unique, TestServer};

/// A server with grant enforcement on and anonymous access on.
async fn enforced_server() -> TestServer {
    server_with(config(true)).await
}

fn parquet_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

fn place_dataset(datasets_dir: &Path, rel: &str) {
    let dst = datasets_dir.join(rel);
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
    std::fs::copy(parquet_fixture(), &dst).unwrap();
}

async fn admin(ts: &TestServer, sql: &str) {
    ts.sql_as(sql, AuthIdentity::system()).await;
}

/// A fresh user with one fresh role that holds `rules` (`{r}` is the role name).
async fn reader(ts: &TestServer, rules: &[&str]) -> AuthIdentity {
    let role = unique("role");
    let user = unique("user");
    admin(ts, &format!("CREATE ROLE {role}")).await;
    for rule in rules {
        admin(ts, &rule.replace("{r}", &role)).await;
    }
    admin(ts, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    admin(ts, &format!("GRANT ROLE {role} TO USER {user}")).await;
    ts.login(&user, "pw").await
}

async fn assert_denied(ts: &TestServer, sql: &str, who: &AuthIdentity) {
    let result = ts.try_sql_as(sql, who.clone()).await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("permission denied")),
        "`{sql}` must be denied, got: {result:?}"
    );
}

async fn assert_allowed(ts: &TestServer, sql: &str, who: &AuthIdentity) {
    let result = ts.try_sql_as(sql, who.clone()).await;
    assert!(result.is_ok(), "`{sql}` must be allowed, got: {result:?}");
}

/// A deny on a sub-folder holds against every spelling that reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_deny_holds_against_a_wildcard_that_reaches_it() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/public/a.parquet"));
    place_dataset(ts.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let carol = reader(
        &ts,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON PATH '{root}/secret/**' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(
        &ts,
        &format!("SELECT * FROM read_parquet('{root}/public/*.parquet')"),
        &carol,
    )
    .await;
    for spelling in ["*.parquet", "[s]ecret/*.parquet", "secre?/*.parquet"] {
        assert_denied(
            &ts,
            &format!("SELECT * FROM read_parquet('{root}/{spelling}')"),
            &carol,
        )
        .await;
    }

    // A disk that ignores case reads the file, so the deny must stop it. A case-sensitive disk
    // has no such file, and the read fails before the check.
    let upper = format!("SELECT * FROM read_parquet('{root}/SECRET/s.parquet')");
    let result = ts.try_sql_as(&upper, carol.clone()).await;
    assert!(
        result.as_ref().is_err_and(|e| {
            let message = e.to_string();
            message.contains("permission denied") || message.contains("no file matched")
        }),
        "`{upper}` must not read the file, got: {result:?}"
    );
}

/// Issue #519: a deny on one file holds when the query reads the whole folder.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_deny_holds_when_the_query_reads_the_folder() {
    let ts = enforced_server().await;
    let folder = unique("ERA5_arrow");
    for name in ["open.arrow", "denied.arrow"] {
        write_arrow(&ts.datasets_dir().join(&folder).join(name));
    }
    let user = reader(
        &ts,
        &[
            &format!("GRANT SELECT ON PATH '{folder}/*' TO ROLE {{r}}"),
            &format!("DENY SELECT ON PATH '{folder}/denied.arrow' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(
        &ts,
        &format!("SELECT * FROM read_arrow(['{folder}/open.arrow'])"),
        &user,
    )
    .await;
    assert_denied(
        &ts,
        &format!("SELECT * FROM read_arrow(['{folder}/denied.arrow'])"),
        &user,
    )
    .await;
    assert_denied(
        &ts,
        &format!("SELECT * FROM read_arrow(['{folder}/*.arrow'])"),
        &user,
    )
    .await;
}

/// A one-column Arrow IPC file at `path`.
fn write_arrow(path: &Path) {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::FileWriter;
    use datafusion::arrow::record_batch::RecordBatch;

    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let mut writer = FileWriter::try_new(std::fs::File::create(path).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

/// A path grant covers the files its pattern names, and a glob that reaches further is denied.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_grant_covers_only_the_files_it_names() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/data/top.parquet"));
    place_dataset(
        ts.datasets_dir(),
        &format!("{root}/data/sub/nested.parquet"),
    );
    let alice = reader(
        &ts,
        &[&format!(
            "GRANT SELECT ON PATH '{root}/data/*' TO ROLE {{r}}"
        )],
    )
    .await;

    assert_allowed(
        &ts,
        &format!("SELECT * FROM read_parquet('{root}/data/top.parquet')"),
        &alice,
    )
    .await;
    // The listing lets `*` cross `/`, so this glob also reads `data/sub/nested.parquet`.
    assert_denied(
        &ts,
        &format!("SELECT * FROM read_parquet('{root}/data/*.parquet')"),
        &alice,
    )
    .await;
}

/// A table read needs only the table grant: a path deny on its files applies to read functions.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_deny_does_not_apply_to_a_table_or_a_view_over_the_path() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let table = unique("t_secret");
    let view = unique("v_secret");
    admin(
        &ts,
        &format!(
            "CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/secret/*.parquet'"
        ),
    )
    .await;
    admin(&ts, &format!("CREATE VIEW {view} AS SELECT * FROM {table}")).await;
    let carol = reader(
        &ts,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON PATH '{root}/secret/**' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(&ts, &format!("SELECT * FROM {table}"), &carol).await;
    assert_allowed(&ts, &format!("SELECT * FROM {view}"), &carol).await;
    assert_denied(
        &ts,
        &format!("SELECT * FROM read_parquet('{root}/secret/s.parquet')"),
        &carol,
    )
    .await;

    let dave = reader(
        &ts,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON TABLE {table} TO ROLE {{r}}"),
        ],
    )
    .await;
    assert_denied(&ts, &format!("SELECT * FROM {table}"), &dave).await;
    assert_denied(&ts, &format!("SELECT * FROM {view}"), &dave).await;
}

/// A table rule matches however it spells the name: bare, with the schema, or with the catalog.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_rule_matches_every_spelling_of_the_name() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/p.parquet"));
    let table = unique("t");
    admin(
        &ts,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/p.parquet'"),
    )
    .await;
    let read = format!("SELECT * FROM {table}");
    let runtime = ts.server.runtime();

    for spelling in [format!("public.{table}"), format!("beacon.public.{table}")] {
        let granted = reader(
            &ts,
            &[&format!("GRANT SELECT ON TABLE {spelling} TO ROLE {{r}}")],
        )
        .await;
        assert_allowed(&ts, &read, &granted).await;
        let validator = ts.server.validator(granted);
        assert!(
            runtime
                .table_arrow_schema(table.as_str(), validator.as_ref())
                .await
                .is_ok(),
            "the schema of {spelling} should be readable"
        );
        assert!(
            validator
                .check_table(&runtime.table_facts(table.as_str()))
                .is_ok(),
            "{spelling} should be listed"
        );

        let denied = reader(
            &ts,
            &[
                "GRANT SELECT TO ROLE {r}",
                &format!("DENY SELECT ON TABLE {spelling} TO ROLE {{r}}"),
            ],
        )
        .await;
        assert_denied(&ts, &read, &denied).await;
    }
}

/// A table deny also holds when the table's files are read by path.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_deny_holds_for_the_tables_files() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let table = unique("t_secret");
    admin(
        &ts,
        &format!(
            "CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/secret/*.parquet'"
        ),
    )
    .await;
    let carol = reader(
        &ts,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON TABLE {table} TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_denied(
        &ts,
        &format!("SELECT * FROM read_parquet('{root}/secret/s.parquet')"),
        &carol,
    )
    .await;
}

/// Every table function that reads data or file metadata is checked like `read_parquet`.
#[tokio::test(flavor = "multi_thread")]
async fn every_reading_table_function_needs_a_grant() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/p.parquet"));
    let delta = format!("{root}/delta");
    write_delta(ts.datasets_dir(), &delta);
    let bob = reader(&ts, &[]).await;
    let granted = reader(
        &ts,
        &[&format!("GRANT SELECT ON PATH '{root}/**' TO ROLE {{r}}")],
    )
    .await;

    for sql in [
        format!("SELECT * FROM read_delta('{delta}')"),
        format!("SELECT * FROM read_parquet_schema('{root}/p.parquet')"),
        format!("SELECT * FROM list_datasets('{root}/**')"),
    ] {
        assert_denied(&ts, &sql, &bob).await;
        assert_allowed(&ts, &sql, &granted).await;
    }
}

/// A table's schema is metadata about data the caller may not read.
#[tokio::test(flavor = "multi_thread")]
async fn describe_needs_a_grant() {
    let ts = enforced_server().await;
    let root = unique("reach");
    place_dataset(ts.datasets_dir(), &format!("{root}/p.parquet"));
    let table = unique("t");
    admin(
        &ts,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/p.parquet'"),
    )
    .await;
    let bob = reader(&ts, &[]).await;

    assert_denied(&ts, &format!("DESCRIBE {table}"), &bob).await;
}

/// A table function that reads no data needs no grant.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_function_without_data_needs_no_grant() {
    let ts = enforced_server().await;
    let bob = reader(&ts, &[]).await;

    assert_allowed(&ts, "SELECT * FROM generate_series(1, 3)", &bob).await;
}

/// A two-row Delta table at `<datasets_dir>/<rel>`: one Parquet file and a one-commit log.
fn write_delta(datasets_dir: &Path, rel: &str) {
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;

    let dir = datasets_dir.join(rel);
    std::fs::create_dir_all(dir.join("_delta_log")).unwrap();

    let data_file = "part-00000.parquet";
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let file = std::fs::File::create(dir.join(data_file)).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let size = std::fs::metadata(dir.join(data_file)).unwrap().len();

    // The Delta schema of the `id` column, as the log holds it.
    let delta_schema = serde_json::json!({
        "type": "struct",
        "fields": [{ "name": "id", "type": "integer", "nullable": false, "metadata": {} }],
    });
    let actions = [
        serde_json::json!({ "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 } }),
        serde_json::json!({ "metaData": {
            "id": uuid::Uuid::new_v4().to_string(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": delta_schema.to_string(),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0,
        } }),
        serde_json::json!({ "add": {
            "path": data_file,
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true,
        } }),
    ];
    let log = actions
        .iter()
        .map(|action| action.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        dir.join("_delta_log").join("00000000000000000000.json"),
        log + "\n",
    )
    .unwrap();
}
