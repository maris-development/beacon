//! A bare `RuntimeBuilder` — no `with_storage`, no `with_default_store` — resolves
//! dataset paths against the process's current working directory, DuckDB-style, and
//! creates nothing on the way.
//!
//! beacon-core has no opinion about storage: the datasets store and the URL it is
//! registered under are builder inputs. This pins what an embedder gets when it
//! supplies neither.
//!
//! This lives in its own test binary because it sets the process-wide cwd, which
//! would race with anything running concurrently in the same binary.

use std::path::{Path, PathBuf};

use arrow::array::Int64Array;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;

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

/// Runs a statement as the system identity. A bare builder bootstraps no
/// super-user, and `auth_enforce` is off by default.
async fn sql(runtime: &Runtime, sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
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

/// The whole point of the cwd default: a relative `LOCATION` finds the files sitting
/// next to the process, and reading them leaves no trace.
///
/// Both halves are asserted in one test because the cwd is process-wide: a second
/// `#[tokio::test]` doing the same thing runs concurrently and repoints the cwd out
/// from under this one.
///
/// Asserting the row count (not merely that the statement succeeds) is deliberate —
/// a `LOCATION` resolved against the wrong root yields a table that is silently
/// *empty* rather than an error.
#[tokio::test(flavor = "multi_thread")]
async fn bare_builder_reads_from_the_cwd_and_writes_nothing_to_it() {
    let (_root, cwd) = temp_cwd();
    std::fs::create_dir_all(cwd.join("obs")).expect("create obs dir");
    std::fs::write(cwd.join("obs/a.csv"), "v,name\n1,a\n2,b\n").expect("write dataset");

    let _guard = set_cwd(&cwd);

    // Neither `with_storage` nor `with_default_store`: the datasets store is whatever
    // `StorageConfig::default()` describes, which is a local filesystem at the cwd.
    let runtime = RuntimeBuilder::new()
        .build()
        .await
        .expect("a bare builder should build");

    sql(&runtime, "CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'").await;

    let batches = sql(&runtime, "SELECT count(*) FROM obs").await;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 count")
        .value(0);
    assert_eq!(
        count, 2,
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
