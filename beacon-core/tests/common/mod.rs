//! Shared harness for beacon-core's integration tests.
//!
//! beacon-core is configured only through [`RuntimeBuilder`] — it reads no
//! environment and knows nothing about `beacon-config` — so tests describe the
//! runtime they want explicitly. Each [`TestRuntime`] gets its own temp root and an
//! ephemeral (in-memory) tables store, so tests are isolated and persist nothing.

#![allow(dead_code)] // each test binary uses a different subset of these helpers.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::{AuthIdentity, Credential};
use beacon_object_storage::StorageConfig;
use futures::TryStreamExt;
use tempfile::TempDir;

pub const ADMIN_USERNAME: &str = "test-admin";
pub const ADMIN_PASSWORD: &str = "test-admin-password";
/// The built-in anonymous principal (re-exported through beacon-core).
pub const ANONYMOUS_USERNAME: &str = beacon_core::beacon_auth::ANONYMOUS_USERNAME;

/// Rebuilds an identical runtime over the same storage. `Fn` (not `FnOnce`) so
/// [`TestRuntime::restart`] can apply it again.
type Rebuild = Arc<dyn Fn(RuntimeBuilder) -> RuntimeBuilder + Send + Sync>;

/// A runtime plus the temp root backing it. Dropping it removes the root.
pub struct TestRuntime {
    pub runtime: Runtime,
    pub datasets_dir: PathBuf,
    /// Owns the temp root; the directory is removed when this is dropped.
    _root: Arc<TempDir>,
    /// Present only for runtimes built by [`restartable_runtime`].
    rebuild: Option<Rebuild>,
    storage: StorageConfig,
}

impl TestRuntime {
    /// The `datasets://` root, where tests write the files they then query.
    pub fn datasets_dir(&self) -> &Path {
        &self.datasets_dir
    }

    /// The tmp store root — where query-output files are written and read back.
    pub fn tmp_dir(&self) -> &Path {
        &self.storage.tmp_dir
    }

    /// Drops this runtime and builds a fresh one over the same storage, so a test can
    /// assert what survived. Only valid for [`restartable_runtime`].
    pub async fn restart(self) -> TestRuntime {
        let TestRuntime {
            runtime,
            datasets_dir,
            _root,
            rebuild,
            storage,
        } = self;
        let rebuild = rebuild.expect("restart() requires a runtime from restartable_runtime()");

        // Must drop before reopening: the redb tables store holds an exclusive lock
        // on its file for as long as the runtime that opened it is alive.
        drop(runtime);

        let runtime = build_runtime(&storage, rebuild.as_ref()).await;
        TestRuntime {
            runtime,
            datasets_dir,
            _root,
            rebuild: Some(rebuild),
            storage,
        }
    }

    /// Authenticates the bootstrapped super-user.
    pub async fn admin(&self) -> AuthIdentity {
        self.runtime
            .authenticate(&Credential::basic(ADMIN_USERNAME, ADMIN_PASSWORD))
            .await
            .expect("admin should authenticate")
    }

    /// Runs a statement as the super-user and collects its rows.
    pub async fn sql(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql_as(sql, self.admin().await).await
    }

    /// Runs a statement as `identity` and collects its rows, panicking on error.
    pub async fn sql_as(&self, sql: &str, identity: AuthIdentity) -> Vec<RecordBatch> {
        self.try_sql_as(sql, identity)
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
    }

    /// Runs a statement as `identity`, surfacing errors to the caller.
    pub async fn try_sql_as(
        &self,
        sql: &str,
        identity: AuthIdentity,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let batches = self
            .runtime
            .run_query(beacon_core::query::Query::sql(sql.to_string()), identity)
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(batches)
    }
}

/// Creates the temp root and the storage layout under it.
fn new_root(tag: &str) -> (Arc<TempDir>, StorageConfig) {
    let root = tempfile::Builder::new()
        .prefix(&format!("beacon-core-test-{tag}-"))
        .tempdir()
        .expect("create temp root");
    let storage = storage_in(root.path());
    for dir in [&storage.data_dir, &storage.datasets_dir, &storage.tmp_dir] {
        std::fs::create_dir_all(dir).expect("create storage dir");
    }
    (Arc::new(root), storage)
}

/// Builds a runtime over `storage`, with the super-user bootstrapped.
async fn build_runtime(
    storage: &StorageConfig,
    customize: &dyn Fn(RuntimeBuilder) -> RuntimeBuilder,
) -> Runtime {
    let builder = RuntimeBuilder::new()
        .with_storage(storage.clone())
        .with_tmp_dir_path(storage.tmp_dir.clone())
        .with_admin_credentials(ADMIN_USERNAME.to_string(), ADMIN_PASSWORD.to_string());
    customize(builder)
        .build()
        .await
        .expect("runtime should build")
}

/// Builds a runtime with an isolated temp root, an ephemeral tables store and a
/// bootstrapped super-user. `customize` tweaks the builder for the test at hand.
pub async fn runtime_with(
    tag: &str,
    customize: impl FnOnce(RuntimeBuilder) -> RuntimeBuilder,
) -> TestRuntime {
    let (root, storage) = new_root(tag);
    let builder = RuntimeBuilder::new()
        .with_storage(storage.clone())
        .with_tmp_dir_path(storage.tmp_dir.clone())
        .with_admin_credentials(ADMIN_USERNAME.to_string(), ADMIN_PASSWORD.to_string());
    let runtime = customize(builder)
        .build()
        .await
        .expect("runtime should build");

    TestRuntime {
        runtime,
        datasets_dir: storage.datasets_dir.clone(),
        _root: root,
        rebuild: None,
        storage,
    }
}

/// The default test runtime: isolated storage, ephemeral tables, admin bootstrapped.
pub async fn runtime(tag: &str) -> TestRuntime {
    runtime_with(tag, |b| b).await
}

/// Builds a runtime whose tables store persists to a redb file under the temp root,
/// so [`TestRuntime::restart`] can prove state survives a restart. `customize` is
/// re-applied on every restart, so both runtimes are configured identically.
pub async fn restartable_runtime(
    tag: &str,
    customize: impl Fn(RuntimeBuilder) -> RuntimeBuilder + Send + Sync + 'static,
) -> TestRuntime {
    let (root, mut storage) = new_root(tag);
    // Persistent single-file tables store: without this the catalog is in-memory
    // and a restart would trivially "lose" everything.
    storage.db_path = Some(root.path().join("beacon.db"));

    let rebuild: Rebuild = Arc::new(customize);
    let runtime = build_runtime(&storage, rebuild.as_ref()).await;

    TestRuntime {
        runtime,
        datasets_dir: storage.datasets_dir.clone(),
        _root: root,
        rebuild: Some(rebuild),
        storage,
    }
}

/// Storage rooted at `root`, with an ephemeral (in-memory) tables store and no
/// filesystem watching — tests trigger crawls explicitly unless they opt in.
pub fn storage_in(root: &Path) -> StorageConfig {
    StorageConfig {
        data_dir: root.to_path_buf(),
        datasets_dir: root.join("datasets"),
        // In-memory tables store: independent per runtime, nothing persists.
        db_path: None,
        tmp_dir: root.join("tmp"),
        enable_fs_events: false,
        ..Default::default()
    }
}

/// Writes `contents` to `path`, creating parent directories.
pub fn write_file(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

/// The single scalar in a one-row, one-column result.
pub fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    use arrow::array::Int64Array;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected an Int64 column")
        .value(0)
}

/// Total rows across all batches.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
