//! A bare `RuntimeBuilder` — no `with_default_store` / `with_default_object_store`
//! — resolves *relative* dataset paths against the process's current working
//! directory, DuckDB-style, and creates nothing on the way.
//!
//! beacon-core has no opinion about storage: the datasets store and the URL it is
//! registered under are builder inputs. This pins what an embedder gets when it
//! supplies neither and then uses relative paths.
//!
//! These tests mutate the process-wide working directory, so they serialize on
//! [`CWD_LOCK`]: only one runs at a time, each pointing the cwd at its own temp
//! directory for the duration. The cwd-*independent* dynamic-mode behaviour
//! (absolute paths, `file://`, object-store schemes) lives in `dynamic_store.rs`,
//! which needs no such lock.

use std::path::{Path, PathBuf};

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;

/// Serializes every test that repoints the process-wide working directory. Held
/// across the whole test body (an async mutex, so the guard survives `.await`).
static CWD_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Restores the previous cwd on drop, so a failing assertion cannot leave the
/// process pointed at a temp directory that is about to be removed.
struct CwdGuard(PathBuf);

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

fn set_cwd(dir: &Path) -> CwdGuard {
    let previous = std::env::current_dir().expect("a readable cwd");
    std::env::set_current_dir(dir).expect("cwd should be settable");
    CwdGuard(previous)
}

async fn bare_runtime() -> Runtime {
    RuntimeBuilder::new()
        .build()
        .await
        .expect("a bare builder should build")
}

/// Runs a statement as the system identity. A bare builder bootstraps no
/// super-user, and `auth_enforce` is off by default.
async fn sql(runtime: &Runtime, sql: &str) -> Vec<RecordBatch> {
    runtime
        .run_query(
            beacon_core::query::Query::sql(sql.to_string()),
            AuthIdentity::system(),
        )
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
        .into_record_stream()
        .expect("a record stream")
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|e| panic!("collecting {sql} failed: {e}"))
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 count")
        .value(0)
}

/// A temp root standing in for "wherever the process was started". Canonicalized
/// because macOS reports `/var/folders/...` as `/private/var/folders/...` once it is
/// the cwd, and the two must compare equal.
fn temp_cwd() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::Builder::new()
        .prefix("beacon-core-test-cwd-default-")
        .tempdir()
        .expect("create temp root");
    let path = std::fs::canonicalize(root.path()).expect("canonical temp root");
    (root, path)
}

fn write(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

/// The whole point of the cwd default: a relative `LOCATION` finds the files sitting
/// next to the process, and reading them leaves no trace.
///
/// Asserting the row count (not merely that the statement succeeds) is deliberate —
/// a `LOCATION` resolved against the wrong root yields a table that is silently
/// *empty* rather than an error.
#[tokio::test(flavor = "multi_thread")]
async fn bare_builder_reads_from_the_cwd_and_writes_nothing_to_it() {
    let _lock = CWD_LOCK.lock().await;
    let (_root, cwd) = temp_cwd();
    write(&cwd.join("obs/a.csv"), "v,name\n1,a\n2,b\n");

    let _guard = set_cwd(&cwd);

    // No `with_default_store`: the runtime is in dynamic mode, so a relative path
    // resolves against the process's current working directory (DuckDB-style).
    let runtime = bare_runtime().await;

    sql(&runtime, "CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'").await;

    assert_eq!(
        scalar_i64(&sql(&runtime, "SELECT count(*) FROM obs").await),
        2,
        "a relative LOCATION should resolve against the cwd and list its files"
    );

    // DuckDB-style: `db_path` is `None`, so the catalog `CREATE EXTERNAL TABLE`
    // persists lives in the in-memory tables store, not in a file next to the data.
    let mut entries: Vec<String> = std::fs::read_dir(&cwd)
        .expect("read cwd")
        .map(|e| {
            e.expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    entries.sort();
    assert_eq!(
        entries,
        vec!["obs".to_string()],
        "a default runtime should not create anything in the working directory"
    );
}

/// A relative single-file `LOCATION` (no trailing slash) opens exactly that file
/// under the cwd.
#[tokio::test(flavor = "multi_thread")]
async fn relative_single_file_resolves_against_cwd() {
    let _lock = CWD_LOCK.lock().await;
    let (_root, cwd) = temp_cwd();
    write(&cwd.join("solo.csv"), "v\n1\n2\n3\n");

    let _guard = set_cwd(&cwd);
    let runtime = bare_runtime().await;

    sql(
        &runtime,
        "CREATE EXTERNAL TABLE solo STORED AS CSV LOCATION 'solo.csv'",
    )
    .await;
    assert_eq!(scalar_i64(&sql(&runtime, "SELECT count(*) FROM solo").await), 3);
}

/// A relative recursive glob resolves against the cwd and reaches nested files.
#[tokio::test(flavor = "multi_thread")]
async fn relative_recursive_glob_resolves_against_cwd() {
    let _lock = CWD_LOCK.lock().await;
    let (_root, cwd) = temp_cwd();
    write(&cwd.join("tree/a.csv"), "v\n1\n");
    write(&cwd.join("tree/deep/b.csv"), "v\n2\n3\n");

    let _guard = set_cwd(&cwd);
    let runtime = bare_runtime().await;

    let count = scalar_i64(&sql(&runtime, "SELECT count(*) FROM read_csv('tree/**/*.csv')").await);
    assert_eq!(
        count, 3,
        "a relative recursive glob should resolve against the cwd across subdirs"
    );
}

/// `read_csv` with a relative path resolves against the cwd, like `LOCATION` does.
#[tokio::test(flavor = "multi_thread")]
async fn read_csv_relative_resolves_against_cwd() {
    let _lock = CWD_LOCK.lock().await;
    let (_root, cwd) = temp_cwd();
    write(&cwd.join("data/one.csv"), "v,name\n1,a\n2,b\n");

    let _guard = set_cwd(&cwd);
    let runtime = bare_runtime().await;

    assert_eq!(
        scalar_i64(&sql(&runtime, "SELECT count(*) FROM read_csv('data/one.csv')").await),
        2
    );
    assert_eq!(
        scalar_i64(&sql(&runtime, "SELECT v FROM read_csv('data/one.csv') WHERE name = 'b'").await),
        2
    );
}

/// An **absolute** `LOCATION` is resolved to itself, ignoring the cwd entirely:
/// with the cwd pointed at an empty directory, an absolute path elsewhere still
/// reads its files.
#[tokio::test(flavor = "multi_thread")]
async fn absolute_location_ignores_the_cwd() {
    let _lock = CWD_LOCK.lock().await;
    let (_empty_root, empty_cwd) = temp_cwd(); // cwd points here — no datasets
    let (_data_root, data_dir) = temp_cwd(); // data lives here, elsewhere
    write(&data_dir.join("obs/a.csv"), "v\n1\n2\n");

    let _guard = set_cwd(&empty_cwd);
    let runtime = bare_runtime().await;

    let location = format!("{}/", data_dir.join("obs").display());
    sql(
        &runtime,
        &format!("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION '{location}'"),
    )
    .await;
    assert_eq!(
        scalar_i64(&sql(&runtime, "SELECT count(*) FROM obs").await),
        2,
        "an absolute LOCATION must resolve to itself, not against the (empty) cwd"
    );
}

/// The dataset-listing UDTF with no pattern globs the cwd in dynamic mode.
#[tokio::test(flavor = "multi_thread")]
async fn list_datasets_lists_the_cwd() {
    let _lock = CWD_LOCK.lock().await;
    let (_root, cwd) = temp_cwd();
    write(&cwd.join("a.csv"), "v\n1\n");
    write(&cwd.join("sub/b.csv"), "v\n2\n");

    let _guard = set_cwd(&cwd);
    let runtime = bare_runtime().await;

    // Bare `list_datasets()` uses the default pattern `**/*`, which in dynamic
    // mode resolves against the cwd.
    let count = scalar_i64(&sql(&runtime, "SELECT count(*) FROM list_datasets()").await);
    assert_eq!(count, 2, "both CSVs under the cwd should be discovered");
}
