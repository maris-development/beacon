//! `INSTALL` and `UNINSTALL` of function plugins from `file://` sources.

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use beacon_core::plugins::archive::sha256_hex;
use beacon_core::AuthIdentity;
use common::plugins::{
    example_plugin_dir, plugin_archive, put_plugin_folder, read_record, sql_manifest,
};
use common::{restartable_runtime, scalar_i64, TestRuntime};

const DEMO_SQL: &str = "CREATE FUNCTION demo_double(x BIGINT) RETURNS BIGINT RETURN x * 2";

async fn new_runtime(tag: &str) -> TestRuntime {
    restartable_runtime(tag, |b| b).await
}

fn file_url(path: &Path) -> String {
    url::Url::from_file_path(path)
        .expect("an absolute path")
        .to_string()
}

fn install_sql(name: &str, path: &Path) -> String {
    format!("INSTALL {name} FROM '{}'", file_url(path))
}

/// The SQL-only `demo` plugin as an unpacked directory.
fn demo_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let manifest = sql_manifest("demo", "0.1.0", ">=2", &["sql/functions.sql"]);
    common::write_file(&dir.path().join("beacon-plugin.toml"), &manifest);
    common::write_file(&dir.path().join("sql").join("functions.sql"), DEMO_SQL);
    dir
}

/// Writes `archive` as `<file>` into a new directory, with a `SHA256SUMS` line when given.
fn release_dir(file: &str, archive: &[u8], sha256: Option<&str>) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(file);
    std::fs::write(&path, archive).unwrap();
    if let Some(sha256) = sha256 {
        std::fs::write(dir.path().join("SHA256SUMS"), format!("{sha256}  {file}\n")).unwrap();
    }
    (dir, path)
}

fn demo_archive() -> Vec<u8> {
    let manifest = sql_manifest("demo", "0.1.0", ">=2", &["sql/functions.sql"]);
    plugin_archive(&[
        ("beacon-plugin.toml", &manifest),
        ("sql/functions.sql", DEMO_SQL),
    ])
}

async fn sql_error(rt: &TestRuntime, sql: &str) -> String {
    match rt.try_sql(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(error) => format!("{error:#}"),
    }
}

fn scalar_f64(batches: &[RecordBatch]) -> f64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("expected a Float64 column")
        .value(0)
}

/// `(name, status, source, sha256)` of each row in `beacon.system.plugins`.
async fn plugin_rows(rt: &TestRuntime) -> Vec<(String, String, Option<String>, Option<String>)> {
    let batches = rt
        .sql("SELECT name, status, source, sha256 FROM beacon.system.plugins ORDER BY name")
        .await;
    let mut rows = Vec::new();
    for batch in &batches {
        let column = |col: usize| {
            batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a Utf8 column")
                .clone()
        };
        let (name, status, source, sha256) = (column(0), column(1), column(2), column(3));
        let optional = |column: &StringArray, row: usize| {
            (!column.is_null(row)).then(|| column.value(row).to_string())
        };
        for row in 0..batch.num_rows() {
            rows.push((
                name.value(row).to_string(),
                status.value(row).to_string(),
                optional(&source, row),
                optional(&sha256, row),
            ));
        }
    }
    rows
}

/// Whether the plugins directory holds a folder for plugin `name`.
fn is_stored(rt: &TestRuntime, name: &str) -> bool {
    rt.plugins_dir().join(name).exists()
}

/// The folders in the plugins directory whose name starts with `.tmp-`.
fn temp_folders(rt: &TestRuntime) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(rt.plugins_dir()) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(".tmp-"))
        .map(|entry| entry.path())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn install_without_a_plugins_dir_is_refused() {
    let rt = common::runtime_with("install-no-dir", |mut b| {
        b.plugins_dir = None;
        b
    })
    .await;
    let dir = demo_dir();
    let error = sql_error(&rt, &install_sql("demo", dir.path())).await;
    assert!(error.contains("no plugins directory"), "{error}");
    let error = match rt
        .runtime
        .install_plugin_archive("demo", demo_archive().into(), "test")
        .await
    {
        Ok(()) => panic!("expected the install to fail"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("no plugins directory"), "{error}");
    assert!(!rt.plugins_dir().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn install_from_a_directory() {
    let rt = new_runtime("install-dir").await;
    let dir = example_plugin_dir(false);
    rt.sql(&install_sql("example", dir.path())).await;
    assert_eq!(
        scalar_f64(&rt.sql("SELECT example_add_one(1.5)").await),
        2.5
    );
    assert_eq!(
        scalar_f64(&rt.sql("SELECT example_add_two(1.0)").await),
        3.0
    );
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, "loaded");
    assert_eq!(rows[0].2.as_deref(), Some(file_url(dir.path()).as_str()));
    assert!(is_stored(&rt, "example"));
}

#[tokio::test(flavor = "multi_thread")]
async fn install_from_an_archive_with_sha256sums() {
    let rt = new_runtime("install-archive").await;
    let archive = demo_archive();
    let sha256 = sha256_hex(&archive);
    let (_dir, path) = release_dir("demo-0.1.0-any.tar.gz", &archive, Some(&sha256));
    rt.sql(&install_sql("demo", &path)).await;
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows[0].3.as_deref(), Some(sha256.as_str()));
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_sha256_is_refused() {
    let rt = new_runtime("install-bad-sha").await;
    let wrong = "0".repeat(64);
    let (_dir, path) = release_dir("demo-0.1.0-any.tar.gz", &demo_archive(), Some(&wrong));
    let error = sql_error(&rt, &install_sql("demo", &path)).await;
    assert!(error.contains("SHA-256 mismatch"), "{error}");
    assert!(plugin_rows(&rt).await.is_empty());
    assert!(!is_stored(&rt, "demo"));
}

#[tokio::test(flavor = "multi_thread")]
async fn name_must_match_the_manifest() {
    let rt = new_runtime("install-name").await;
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/example-plugin");
    let error = sql_error(&rt, &install_sql("other", &example)).await;
    assert!(error.contains("manifest name 'example'"), "{error}");
    assert!(plugin_rows(&rt).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn install_twice_fails() {
    let rt = new_runtime("install-twice").await;
    let dir = demo_dir();
    rt.sql(&install_sql("demo", dir.path())).await;
    let error = sql_error(&rt, &install_sql("demo", dir.path())).await;
    assert!(error.contains("already installed"), "{error}");
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_installs_of_one_name() {
    let rt = new_runtime("install-parallel").await;
    let dir = demo_dir();
    let sql = install_sql("demo", dir.path());
    let (first, second) = tokio::join!(rt.try_sql(&sql), rt.try_sql(&sql));
    let errors: Vec<String> = [first, second]
        .into_iter()
        .filter_map(|result| result.err().map(|error| format!("{error:#}")))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("already installed"), "{errors:?}");
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstall_removes_the_functions() {
    let rt = new_runtime("uninstall").await;
    let dir = example_plugin_dir(false);
    rt.sql(&install_sql("example", dir.path())).await;
    rt.sql("UNINSTALL example").await;
    for sql in ["SELECT example_add_one(1.0)", "SELECT example_inc(1.0)"] {
        let error = sql_error(&rt, sql).await;
        assert!(error.contains("example_"), "{error}");
    }
    assert!(!is_stored(&rt, "example"));
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, "uninstalled");

    let rt = rt.restart().await;
    let error = sql_error(&rt, "SELECT example_add_one(1.0)").await;
    assert!(error.contains("example_add_one"), "{error}");
    assert!(plugin_rows(&rt).await.is_empty());
    assert!(!is_stored(&rt, "example"));
    // On Windows the library stays loaded in this process, so its temp folder may stay.
    let left: Vec<_> = std::fs::read_dir(rt.plugins_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| !temp_folders(&rt).contains(path))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn reinstall_after_uninstall_works() {
    let rt = new_runtime("reinstall").await;
    let dir = demo_dir();
    rt.sql(&install_sql("demo", dir.path())).await;
    rt.sql("UNINSTALL demo").await;
    let error = sql_error(&rt, "SELECT demo_double(21)").await;
    assert!(error.contains("demo_double"), "{error}");
    rt.sql(&install_sql("demo", dir.path())).await;
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows[0].1, "loaded");
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstall_a_failed_plugin() {
    let rt = new_runtime("uninstall-failed").await;
    rt.sql("CREATE FUNCTION demo_double(x BIGINT) RETURNS BIGINT RETURN x + 1")
        .await;
    put_plugin_folder(&rt.plugins_dir(), "demo", &demo_archive());
    common::write_file(
        &rt.plugins_dir().join("broken").join("install.json"),
        "not json",
    );
    let rt = rt.restart().await;
    let statuses: Vec<_> = plugin_rows(&rt)
        .await
        .into_iter()
        .map(|row| (row.0, row.1))
        .collect();
    assert_eq!(
        statuses,
        [
            ("broken".to_string(), "failed".to_string()),
            ("demo".to_string(), "failed".to_string())
        ]
    );

    rt.sql("UNINSTALL demo").await;
    rt.sql("UNINSTALL broken").await;
    assert!(!is_stored(&rt, "demo"));
    assert!(!is_stored(&rt, "broken"));
    // The user function keeps its name.
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 22);
    let rows = plugin_rows(&rt).await;
    assert!(rows.iter().all(|row| row.1 == "uninstalled"), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstall_of_an_unknown_name_fails() {
    let rt = new_runtime("uninstall-unknown").await;
    let error = sql_error(&rt, "UNINSTALL nope").await;
    assert!(error.contains("plugin 'nope' is not installed"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn non_super_user_is_refused() {
    let rt = new_runtime("install-refused").await;
    let dir = demo_dir();
    for sql in [
        install_sql("demo", dir.path()),
        "UNINSTALL demo".to_string(),
    ] {
        let error = match rt.try_sql_as(&sql, AuthIdentity::empty()).await {
            Ok(_) => panic!("expected `{sql}` to be refused"),
            Err(error) => format!("{error:#}"),
        };
        assert!(error.contains("requires super-user privileges"), "{error}");
    }
    assert!(plugin_rows(&rt).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_with_parent_dir_is_refused() {
    let rt = new_runtime("install-unsafe").await;
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    let name = &mut header.as_old_mut().name;
    name.fill(0);
    name[..8].copy_from_slice(b"../evil\0");
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(1);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append(&header, &b"x"[..]).unwrap();
    let tar_bytes = builder.into_inner().unwrap();
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&tar_bytes).unwrap();
    let archive = gz.finish().unwrap();
    let (_dir, path) = release_dir("evil-0.1.0-any.tar.gz", &archive, None);
    let error = sql_error(&rt, &install_sql("evil", &path)).await;
    assert!(error.contains("unsafe path"), "{error}");
    assert!(!rt.plugins_dir().join("evil").exists());
    assert!(plugin_rows(&rt).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn install_from_https() {
    let archive = demo_archive();
    let file = "demo-0.1.0-any.tar.gz";
    let (dir, _) = release_dir(file, &archive, Some(&sha256_hex(&archive)));
    let server = common::plugins::serve_dir(dir.path().to_path_buf()).await;
    let sql = format!("INSTALL demo FROM '{}/'", server.base_url);

    // Any source is allowed: only a super-user can run INSTALL.
    let rt = new_runtime("install-https").await;
    rt.sql(&sql).await;
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows[0].0, "demo");
    assert_eq!(rows[0].1, "loaded");
    assert_eq!(
        rows[0].2.as_deref(),
        Some(format!("{}/", server.base_url).as_str())
    );
    assert_eq!(rows[0].3.as_deref(), Some(sha256_hex(&archive).as_str()));
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
}

/// The `installed_at` stamp in the `install.json` of `name`.
fn installed_at(rt: &TestRuntime, name: &str) -> String {
    read_record(&rt.plugins_dir(), name).installed_at
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_plugins_install_once() {
    let dir = demo_dir();
    let url = file_url(dir.path());
    let rt = restartable_runtime("startup-once", move |b| {
        b.with_startup_plugins(vec![("demo".to_string(), url.clone())])
    })
    .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
    let first = installed_at(&rt, "demo");

    let rt = rt.restart().await;
    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT demo_double(21)").await), 42);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, "loaded");
    assert_eq!(installed_at(&rt, "demo"), first);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_startup_plugin_does_not_stop_the_server() {
    let missing = tempfile::tempdir().unwrap().path().join("gone");
    let url = file_url(&missing);
    let rt = restartable_runtime("startup-failed", move |b| {
        b.with_startup_plugins(vec![("demo".to_string(), url.clone())])
    })
    .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT 1").await), 1);
    assert!(plugin_rows(&rt).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_startup_check_shows_in_system_plugins() {
    // The manifest says `demo`, the configured name is `other`.
    let dir = demo_dir();
    let url = file_url(dir.path());
    let rt = restartable_runtime("startup-check-failed", move |b| {
        b.with_startup_plugins(vec![("other".to_string(), url.clone())])
    })
    .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT 1").await), 1);
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1.as_str()),
        ("other", "failed")
    );
    let batches = rt
        .sql("SELECT error FROM beacon.system.plugins WHERE name = 'other'")
        .await;
    let error = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a Utf8 column")
        .value(0)
        .to_string();
    assert!(error.contains("does not match the plugin name"), "{error}");
    assert!(!is_stored(&rt, "other"));

    // The failed entry blocks a second try until UNINSTALL.
    let again = sql_error(&rt, &install_sql("other", dir.path())).await;
    assert!(again.contains("already installed"), "{again}");
    rt.sql("UNINSTALL other").await;
    let rows = plugin_rows(&rt).await;
    assert_eq!(rows[0].1, "uninstalled", "{rows:?}");
}
