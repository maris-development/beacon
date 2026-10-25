//! Function plugins: install from an archive, load from the plugins directory at
//! startup, and refuse collisions and bad plugin content.

mod common;

use bytes::Bytes;
use common::plugins::{plugin_archive, put_plugin_folder, sql_manifest, sql_plugin, write_record};
use common::{restartable_runtime, scalar_i64, scalar_string, TestRuntime};

const DEMO_SQL: &str = "CREATE FUNCTION demo_double(x BIGINT) RETURNS BIGINT RETURN x * 2";

async fn new_runtime(tag: &str) -> TestRuntime {
    restartable_runtime(tag, |b| b).await
}

fn demo() -> Vec<u8> {
    sql_plugin("demo", "0.1.0", ">=2", DEMO_SQL)
}

async fn install(rt: &TestRuntime, name: &str, archive: Vec<u8>) -> anyhow::Result<()> {
    rt.runtime
        .install_plugin_archive(name, Bytes::from(archive), "test")
        .await
}

async fn install_error(rt: &TestRuntime, name: &str, archive: Vec<u8>) -> String {
    match install(rt, name, archive).await {
        Ok(()) => panic!("expected the install of '{name}' to fail"),
        Err(error) => format!("{error:#}"),
    }
}

async fn sql_error(rt: &TestRuntime, sql: &str) -> String {
    match rt.try_sql(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(error) => format!("{error:#}"),
    }
}

async fn demo_double_21(rt: &TestRuntime) -> i64 {
    scalar_i64(&rt.sql("SELECT demo_double(21)").await)
}

/// The names of the entries in the plugins directory, in name order.
fn plugins_dir_entries(rt: &TestRuntime) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(rt.plugins_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_plugin_function_works_after_install() {
    let rt = new_runtime("plugin-install").await;
    install(&rt, "demo", demo()).await.unwrap();
    assert_eq!(demo_double_21(&rt).await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn install_writes_one_plugin_folder() {
    let rt = new_runtime("plugin-folder").await;
    let archive = demo();
    install(&rt, "demo", archive.clone()).await.unwrap();
    assert_eq!(plugins_dir_entries(&rt), ["demo"]);
    let dir = rt.plugins_dir().join("demo");
    assert!(dir.join("beacon-plugin.toml").is_file());
    assert!(dir.join("sql").join("functions.sql").is_file());
    let record = common::plugins::read_record(&rt.plugins_dir(), "demo");
    assert_eq!(record.name, "demo");
    assert_eq!(record.version, "0.1.0");
    assert_eq!(record.source, "test");
    assert_eq!(
        record.sha256,
        beacon_core::plugins::archive::sha256_hex(&archive)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_install_leaves_no_folder() {
    let rt = new_runtime("plugin-failed-folder").await;
    let archive = sql_plugin("demo", "0.1.0", ">=99", DEMO_SQL);
    install_error(&rt, "demo", archive).await;
    assert!(plugins_dir_entries(&rt).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_folder_is_not_replaced() {
    let rt = new_runtime("plugin-folder-exists").await;
    let stale = rt.plugins_dir().join("demo");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("keep.txt"), "x").unwrap();
    let error = install_error(&rt, "demo", demo()).await;
    assert!(error.contains("already exists"), "{error}");
    assert!(stale.join("keep.txt").is_file());
    assert_eq!(plugins_dir_entries(&rt), ["demo"]);
    let error = sql_error(&rt, "SELECT demo_double(21)").await;
    assert!(error.contains("demo_double"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_plugin_survives_a_restart() {
    let rt = new_runtime("plugin-restart").await;
    install(&rt, "demo", demo()).await.unwrap();
    let rt = rt.restart().await;
    assert_eq!(demo_double_21(&rt).await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleted_folder_removes_the_plugin() {
    let rt = new_runtime("plugin-folder-deleted").await;
    install(&rt, "demo", demo()).await.unwrap();
    std::fs::remove_dir_all(rt.plugins_dir().join("demo")).unwrap();
    let rt = rt.restart().await;
    let error = sql_error(&rt, "SELECT demo_double(21)").await;
    assert!(error.contains("demo_double"), "{error}");
    assert!(plugin_rows(&rt).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_temp_dirs_are_deleted_at_startup() {
    let rt = new_runtime("plugin-stale-dirs").await;
    install(&rt, "demo", demo()).await.unwrap();
    let temp = rt.plugins_dir().join(".tmp-stale");
    std::fs::create_dir_all(temp.join("sql")).unwrap();
    write_record(&temp, "stale", &"ab".repeat(32));
    // A folder with no valid plugin name is not a plugin, and Beacon keeps it.
    let other = rt.plugins_dir().join("Not-A-Plugin");
    std::fs::create_dir_all(&other).unwrap();
    let rt = rt.restart().await;
    assert!(!temp.exists(), "the temp directory is still there");
    assert!(other.exists(), "a foreign folder was deleted");
    assert!(
        rt.plugins_dir().join("demo").exists(),
        "the folder of an installed plugin was deleted"
    );
    assert_eq!(demo_double_21(&rt).await, 42);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn collision_with_a_built_in_fails_the_plugin() {
    let rt = new_runtime("plugin-built-in").await;
    let sql = "CREATE FUNCTION bi_first(x BIGINT) RETURNS BIGINT RETURN x + 1;\n\
               CREATE FUNCTION abs(x BIGINT) RETURNS BIGINT RETURN x + 100;";
    let error = install_error(&rt, "bi", sql_plugin("bi", "0.1.0", ">=2", sql)).await;
    assert!(error.contains("abs"), "{error}");
    let error = sql_error(&rt, "SELECT bi_first(1)").await;
    assert!(error.contains("bi_first"), "{error}");
    assert_eq!(scalar_i64(&rt.sql("SELECT abs(-1)").await), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn collision_with_a_user_function_fails_the_plugin() {
    let rt = new_runtime("plugin-user-fn").await;
    rt.sql("CREATE FUNCTION demo_double(x BIGINT) RETURNS BIGINT RETURN x + 1")
        .await;
    let error = install_error(&rt, "demo", demo()).await;
    assert!(
        error.contains("demo_double") && error.contains("user function"),
        "{error}"
    );
    assert_eq!(demo_double_21(&rt).await, 22);
}

#[tokio::test(flavor = "multi_thread")]
async fn user_function_wins_at_startup() {
    let rt = new_runtime("plugin-user-wins").await;
    rt.sql("CREATE FUNCTION f(x BIGINT) RETURNS BIGINT RETURN x + 1")
        .await;
    let archive = sql_plugin(
        "p",
        "0.1.0",
        ">=2",
        "CREATE FUNCTION f(x BIGINT) RETURNS BIGINT RETURN x + 100",
    );
    put_plugin_folder(&rt.plugins_dir(), "p", &archive);
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT f(1)").await), 2);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].name, "p");
    assert_eq!(rows[0].status, "failed");
    assert!(
        rows[0].error.as_deref().is_some_and(|e| e.contains("'f'")),
        "{rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn collision_between_two_plugins_fails_the_second() {
    let rt = new_runtime("plugin-two").await;
    let shared = "CREATE FUNCTION shared_fn(x BIGINT) RETURNS BIGINT RETURN x + 1";
    install(&rt, "first", sql_plugin("first", "0.1.0", ">=2", shared))
        .await
        .unwrap();
    let second = format!(
        "CREATE FUNCTION second_only(x BIGINT) RETURNS BIGINT RETURN x;\n\
         CREATE FUNCTION shared_fn(x BIGINT) RETURNS BIGINT RETURN x + 2;"
    );
    let error = install_error(&rt, "second", sql_plugin("second", "0.1.0", ">=2", &second)).await;
    assert!(
        error.contains("shared_fn") && error.contains("plugin 'first'"),
        "{error}"
    );
    assert_eq!(scalar_i64(&rt.sql("SELECT shared_fn(1)").await), 2);
    let error = sql_error(&rt, "SELECT second_only(1)").await;
    assert!(error.contains("second_only"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_sql_with_another_statement_fails() {
    let rt = new_runtime("plugin-other-stmt").await;
    let archive = sql_plugin("bad", "0.1.0", ">=2", "CREATE VIEW v AS SELECT 1");
    let error = install_error(&rt, "bad", archive).await;
    assert!(error.contains("only CREATE FUNCTION"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_beacon_range_fails() {
    let rt = new_runtime("plugin-range").await;
    let archive = sql_plugin("demo", "0.1.0", ">=99", DEMO_SQL);
    let error = install_error(&rt, "demo", archive).await;
    assert!(error.contains("requires Beacon >=99"), "{error}");
    let error = sql_error(&rt, "SELECT demo_double(21)").await;
    assert!(error.contains("demo_double"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_and_create_on_a_plugin_function_fail() {
    let rt = new_runtime("plugin-drop-create").await;
    install(&rt, "demo", demo()).await.unwrap();

    let error = sql_error(&rt, "DROP FUNCTION demo_double").await;
    assert!(
        error.contains("'demo_double' belongs to plugin 'demo'; use UNINSTALL demo"),
        "{error}"
    );
    let error = sql_error(
        &rt,
        "CREATE OR REPLACE FUNCTION demo_double(x BIGINT) RETURNS BIGINT RETURN x",
    )
    .await;
    assert!(
        error.contains("'demo_double' belongs to plugin 'demo'; choose another name"),
        "{error}"
    );
    let rows = rt.sql("SHOW CREATE FUNCTION demo_double").await;
    assert_eq!(common::column_strings(&rows, 1), vec![DEMO_SQL.to_string()]);
    assert_eq!(scalar_string(&rows), "demo_double");
    assert_eq!(demo_double_21(&rt).await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_plugin_folder_does_not_stop_startup() {
    let rt = new_runtime("plugin-corrupt").await;
    let dir = rt.plugins_dir().join("broken");
    common::write_file(&dir.join("beacon-plugin.toml"), "not a [manifest");
    write_record(&dir, "broken", &"ab".repeat(32));
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT CAST(1 AS BIGINT)").await), 1);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        (rows[0].name.as_str(), rows[0].status.as_str()),
        ("broken", "failed")
    );
    assert_eq!(rows[0].source.as_deref(), Some("test"));
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_sql_cannot_call_a_user_function() {
    let rt = new_runtime("plugin-calls-user-fn").await;
    rt.sql("CREATE FUNCTION u(x BIGINT) RETURNS BIGINT RETURN x + 1")
        .await;
    let archive = sql_plugin(
        "calls",
        "0.1.0",
        ">=2",
        "CREATE FUNCTION calls_u(x BIGINT) RETURNS BIGINT RETURN u(x) * 2",
    );
    let error = install_error(&rt, "calls", archive).await;
    assert!(
        error.contains("invalid body for function 'calls_u'") && error.contains("'u'"),
        "{error}"
    );
    let error = sql_error(&rt, "SELECT calls_u(1)").await;
    assert!(error.contains("calls_u"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_installs_of_two_plugins_with_one_function() {
    let rt = new_runtime("plugin-parallel").await;
    let one = sql_plugin(
        "one",
        "0.1.0",
        ">=2",
        "CREATE FUNCTION same_fn(x BIGINT) RETURNS BIGINT RETURN x + 1",
    );
    let two = sql_plugin(
        "two",
        "0.1.0",
        ">=2",
        "CREATE FUNCTION same_fn(x BIGINT) RETURNS BIGINT RETURN x + 2",
    );
    let spawn = |name: &'static str, archive: Vec<u8>| {
        let runtime = rt.runtime.clone();
        tokio::spawn(async move {
            runtime
                .install_plugin_archive(name, Bytes::from(archive), "test")
                .await
        })
    };
    let (first, second) = tokio::join!(spawn("one", one), spawn("two", two));
    let (first, second) = (first.unwrap(), second.unwrap());
    assert!(
        first.is_ok() != second.is_ok(),
        "exactly one install must win: {first:?} {second:?}"
    );
    let (winner, value) = if first.is_ok() {
        ("one", 2)
    } else {
        ("two", 3)
    };
    let loser = first.err().or(second.err()).unwrap();
    assert!(format!("{loser:#}").contains("same_fn"), "{loser:#}");
    assert_eq!(scalar_i64(&rt.sql("SELECT same_fn(1)").await), value);
    let error = sql_error(&rt, "DROP FUNCTION same_fn").await;
    assert!(
        error.contains(&format!("belongs to plugin '{winner}'")),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn manifest_with_unsafe_sql_path_fails() {
    let rt = new_runtime("plugin-unsafe-path").await;
    let manifest = sql_manifest("demo", "0.1.0", ">=2", &["../x.sql"]);
    let archive = plugin_archive(&[("beacon-plugin.toml", &manifest), ("x.sql", DEMO_SQL)]);
    let error = install_error(&rt, "demo", archive).await;
    assert!(error.contains("unsafe path in manifest"), "{error}");
}

/// One row of `beacon.system.plugins`, every column read as text.
#[derive(Debug)]
struct PluginRow {
    name: String,
    version: Option<String>,
    kind: Option<String>,
    status: String,
    error: Option<String>,
    functions: String,
    source: Option<String>,
    sha256: Option<String>,
}

async fn plugin_rows(rt: &TestRuntime) -> Vec<PluginRow> {
    use arrow::array::{Array, StringArray};
    let batches = rt
        .sql(
            "SELECT name, version, kind, status, error, array_to_string(functions, ','), source, sha256 FROM beacon.system.plugins ORDER BY name",
        )
        .await;
    let mut rows = Vec::new();
    for batch in &batches {
        let text = |col: usize, row: usize| {
            let column = batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a Utf8 column");
            (!column.is_null(row)).then(|| column.value(row).to_string())
        };
        for row in 0..batch.num_rows() {
            rows.push(PluginRow {
                name: text(0, row).unwrap(),
                version: text(1, row),
                kind: text(2, row),
                status: text(3, row).unwrap(),
                error: text(4, row),
                functions: text(5, row).unwrap_or_default(),
                source: text(6, row),
                sha256: text(7, row),
            });
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn system_plugins_lists_loaded_and_failed_plugins() {
    let rt = new_runtime("plugin-system-table").await;
    install(&rt, "demo", demo()).await.unwrap();
    let sql = "CREATE FUNCTION abs(x BIGINT) RETURNS BIGINT RETURN x + 100";
    put_plugin_folder(
        &rt.plugins_dir(),
        "bad",
        &sql_plugin("bad", "0.1.0", ">=2", sql),
    );
    let rt = rt.restart().await;

    let rows = plugin_rows(&rt).await;
    let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["bad", "demo"], "{rows:?}");

    let demo = &rows[1];
    assert_eq!(demo.version.as_deref(), Some("0.1.0"));
    assert_eq!(demo.kind.as_deref(), Some("sql"));
    assert_eq!(demo.status, "loaded");
    assert_eq!(demo.error, None);
    assert_eq!(demo.functions, "demo_double");
    assert_eq!(demo.source.as_deref(), Some("test"));
    assert_eq!(demo.sha256.as_deref().map(str::len), Some(64));

    let bad = &rows[0];
    assert_eq!(bad.status, "failed");
    assert!(
        bad.error.as_deref().is_some_and(|e| e.contains("abs")),
        "{bad:?}"
    );
    assert_eq!(bad.functions, "");
}

#[tokio::test(flavor = "multi_thread")]
async fn system_plugins_is_refused_for_a_non_super_user() {
    let rt = new_runtime("plugin-system-gate").await;
    let error = rt
        .try_sql_as(
            "SELECT * FROM beacon.system.plugins",
            beacon_core::AuthIdentity::empty(),
        )
        .await
        .err()
        .expect("a non-super read must fail");
    assert!(error.to_string().contains("super-user"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_install_record_shows_as_failed() {
    let rt = new_runtime("plugin-bad-record").await;
    let archive = sql_plugin("broken", "0.1.0", ">=2", DEMO_SQL);
    put_plugin_folder(&rt.plugins_dir(), "broken", &archive);
    std::fs::write(
        rt.plugins_dir().join("broken").join("install.json"),
        "{ not json",
    )
    .unwrap();
    let rt = rt.restart().await;
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row.name, "broken");
    assert_eq!(row.status, "failed");
    assert!(row.error.is_some(), "{row:?}");
    assert_eq!(
        (&row.version, &row.source, &row.sha256),
        (&None, &None, &None)
    );
    assert_eq!(row.functions, "");
}
