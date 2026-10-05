//! Read authorization checks what a scan reaches, not how the query spells it.
//!
//! A glob lets `*` cross `/` in a listing, so the text of a path says little about the files
//! it reads. Each test here spells a read so that its text passes the old text check while the
//! files it reaches must not be read.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use beacon_core::{AuthIdentity, Credential};
use common::{runtime_with, TestRuntime, ANONYMOUS_USERNAME};

async fn enforced_runtime(tag: &str) -> TestRuntime {
    runtime_with(tag, |builder| {
        builder
            .with_auth_enforcement(true)
            .with_anonymous_user(ANONYMOUS_USERNAME)
    })
    .await
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
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

async fn admin(rt: &TestRuntime, sql: &str) {
    rt.sql_as(sql, AuthIdentity::system()).await;
}

/// A fresh user with one fresh role that holds `rules` (`{r}` is the role name).
async fn reader(rt: &TestRuntime, rules: &[&str]) -> AuthIdentity {
    let role = unique("role");
    let user = unique("user");
    admin(rt, &format!("CREATE ROLE {role}")).await;
    for rule in rules {
        admin(rt, &rule.replace("{r}", &role)).await;
    }
    admin(rt, &format!("CREATE USER {user} WITH PASSWORD 'pw'")).await;
    admin(rt, &format!("GRANT ROLE {role} TO USER {user}")).await;
    rt.runtime
        .authenticate(&Credential::basic(user, "pw"))
        .await
        .unwrap()
}

async fn assert_denied(rt: &TestRuntime, sql: &str, who: &AuthIdentity) {
    let result = rt.try_sql_as(sql, who.clone()).await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("permission denied")),
        "`{sql}` must be denied, got: {result:?}"
    );
}

async fn assert_allowed(rt: &TestRuntime, sql: &str, who: &AuthIdentity) {
    let result = rt.try_sql_as(sql, who.clone()).await;
    assert!(result.is_ok(), "`{sql}` must be allowed, got: {result:?}");
}

/// A deny on a sub-folder holds against every spelling that reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_deny_holds_against_a_wildcard_that_reaches_it() {
    let rt = enforced_runtime("reach-deny").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/public/a.parquet"));
    place_dataset(rt.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let carol = reader(
        &rt,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON PATH '{root}/secret/**' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(
        &rt,
        &format!("SELECT * FROM read_parquet('{root}/public/*.parquet')"),
        &carol,
    )
    .await;
    for spelling in ["*.parquet", "[s]ecret/*.parquet", "secre?/*.parquet"] {
        assert_denied(
            &rt,
            &format!("SELECT * FROM read_parquet('{root}/{spelling}')"),
            &carol,
        )
        .await;
    }

    // A disk that ignores case reads the file, so the deny must stop it. A case-sensitive disk
    // has no such file, and the read fails before the check.
    let upper = format!("SELECT * FROM read_parquet('{root}/SECRET/s.parquet')");
    let result = rt.try_sql_as(&upper, carol.clone()).await;
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
    let rt = enforced_runtime("reach-issue-519").await;
    let folder = unique("ERA5_arrow");
    for name in ["open.arrow", "denied.arrow"] {
        write_arrow(&rt.datasets_dir().join(&folder).join(name));
    }
    let user = reader(
        &rt,
        &[
            &format!("GRANT SELECT ON PATH '{folder}/*' TO ROLE {{r}}"),
            &format!("DENY SELECT ON PATH '{folder}/denied.arrow' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(
        &rt,
        &format!("SELECT * FROM read_arrow(['{folder}/open.arrow'])"),
        &user,
    )
    .await;
    assert_denied(
        &rt,
        &format!("SELECT * FROM read_arrow(['{folder}/denied.arrow'])"),
        &user,
    )
    .await;
    assert_denied(
        &rt,
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
    let rt = enforced_runtime("reach-grant").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/data/top.parquet"));
    place_dataset(
        rt.datasets_dir(),
        &format!("{root}/data/sub/nested.parquet"),
    );
    let alice = reader(
        &rt,
        &[&format!(
            "GRANT SELECT ON PATH '{root}/data/*' TO ROLE {{r}}"
        )],
    )
    .await;

    assert_allowed(
        &rt,
        &format!("SELECT * FROM read_parquet('{root}/data/top.parquet')"),
        &alice,
    )
    .await;
    // The listing lets `*` cross `/`, so this glob also reads `data/sub/nested.parquet`.
    assert_denied(
        &rt,
        &format!("SELECT * FROM read_parquet('{root}/data/*.parquet')"),
        &alice,
    )
    .await;
}

/// A table read needs only the table grant: a path deny on its files applies to read functions.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_deny_does_not_apply_to_a_table_or_a_view_over_the_path() {
    let rt = enforced_runtime("reach-table").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let table = unique("t_secret");
    let view = unique("v_secret");
    admin(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/secret/*.parquet'"
        ),
    )
    .await;
    admin(&rt, &format!("CREATE VIEW {view} AS SELECT * FROM {table}")).await;
    let carol = reader(
        &rt,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON PATH '{root}/secret/**' TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_allowed(&rt, &format!("SELECT * FROM {table}"), &carol).await;
    assert_allowed(&rt, &format!("SELECT * FROM {view}"), &carol).await;
    assert_denied(
        &rt,
        &format!("SELECT * FROM read_parquet('{root}/secret/s.parquet')"),
        &carol,
    )
    .await;

    let dave = reader(
        &rt,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON TABLE {table} TO ROLE {{r}}"),
        ],
    )
    .await;
    assert_denied(&rt, &format!("SELECT * FROM {table}"), &dave).await;
    assert_denied(&rt, &format!("SELECT * FROM {view}"), &dave).await;
}

/// A table rule matches however it spells the name: bare, with the schema, or with the catalog.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_rule_matches_every_spelling_of_the_name() {
    let rt = enforced_runtime("reach-qualified").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/p.parquet"));
    let table = unique("t");
    admin(
        &rt,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/p.parquet'"),
    )
    .await;
    let read = format!("SELECT * FROM {table}");

    for spelling in [format!("public.{table}"), format!("beacon.public.{table}")] {
        let granted = reader(&rt, &[&format!("GRANT SELECT ON TABLE {spelling} TO ROLE {{r}}")]).await;
        assert_allowed(&rt, &read, &granted).await;
        assert!(
            rt.runtime.table_arrow_schema(table.as_str(), &granted).await.is_ok(),
            "the schema of {spelling} should be readable"
        );
        assert!(rt.runtime.can_list_table(&granted, None, &table), "{spelling} should be listed");

        let denied = reader(
            &rt,
            &["GRANT SELECT TO ROLE {r}", &format!("DENY SELECT ON TABLE {spelling} TO ROLE {{r}}")],
        )
        .await;
        assert_denied(&rt, &read, &denied).await;
    }
}

/// A table deny also holds when the table's files are read by path.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_deny_holds_for_the_tables_files() {
    let rt = enforced_runtime("reach-table-deny").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/secret/s.parquet"));
    let table = unique("t_secret");
    admin(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/secret/*.parquet'"
        ),
    )
    .await;
    let carol = reader(
        &rt,
        &[
            "GRANT SELECT TO ROLE {r}",
            &format!("DENY SELECT ON TABLE {table} TO ROLE {{r}}"),
        ],
    )
    .await;

    assert_denied(
        &rt,
        &format!("SELECT * FROM read_parquet('{root}/secret/s.parquet')"),
        &carol,
    )
    .await;
}

/// Every table function that reads data or file metadata is checked like `read_parquet`.
#[tokio::test(flavor = "multi_thread")]
async fn every_reading_table_function_needs_a_grant() {
    let rt = enforced_runtime("reach-functions").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/p.parquet"));
    let delta = format!("{root}/delta");
    write_delta(rt.datasets_dir(), &delta).await;
    let bob = reader(&rt, &[]).await;
    let granted = reader(
        &rt,
        &[&format!("GRANT SELECT ON PATH '{root}/**' TO ROLE {{r}}")],
    )
    .await;

    for sql in [
        format!("SELECT * FROM read_delta('{delta}')"),
        format!("SELECT * FROM read_parquet_schema('{root}/p.parquet')"),
        format!("SELECT * FROM list_datasets('{root}/**')"),
    ] {
        assert_denied(&rt, &sql, &bob).await;
        assert_allowed(&rt, &sql, &granted).await;
    }
}

/// A table's schema is metadata about data the caller may not read.
#[tokio::test(flavor = "multi_thread")]
async fn describe_needs_a_grant() {
    let rt = enforced_runtime("reach-describe").await;
    let root = unique("reach");
    place_dataset(rt.datasets_dir(), &format!("{root}/p.parquet"));
    let table = unique("t");
    admin(
        &rt,
        &format!("CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{root}/p.parquet'"),
    )
    .await;
    let bob = reader(&rt, &[]).await;

    assert_denied(&rt, &format!("DESCRIBE {table}"), &bob).await;
}

/// A table function that reads no data needs no grant.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_function_without_data_needs_no_grant() {
    let rt = enforced_runtime("reach-series").await;
    let bob = reader(&rt, &[]).await;

    assert_allowed(&rt, "SELECT * FROM generate_series(1, 3)", &bob).await;
}

/// A two-row Delta table at `<datasets_dir>/<rel>`.
async fn write_delta(datasets_dir: &Path, rel: &str) {
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let dir = datasets_dir.join(rel);
    std::fs::create_dir_all(&dir).unwrap();
    let url = url::Url::from_directory_path(std::fs::canonicalize(&dir).unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    deltalake::DeltaTableBuilder::from_url(url)
        .unwrap()
        .build()
        .unwrap()
        .write(vec![batch])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .await
        .unwrap();
}
