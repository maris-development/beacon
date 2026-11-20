//! Shared harness for the transport integration tests.
//!
//! Each test gets its own server rooted in a fresh temporary directory — datasets,
//! tables store and scratch space alike — so nothing leaks between tests and the
//! whole lot is removed when the `TempDir` drops.

// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use beacon_auth::{AuthIdentity, Credential};
use beacon_core::query::Query;
use beacon_server::Server;
use futures::TryStreamExt;
use tempfile::TempDir;

/// Credentials of the config-defined super-user every test server bootstraps.
pub const ADMIN_USERNAME: &str = "beacon-admin";
pub const ADMIN_PASSWORD: &str = "beacon-password";

/// A server plus the temp root backing it. Dropping this removes the root, so it
/// has to outlive every use of `server`.
pub struct TestServer {
    pub server: Arc<Server>,
    _root: TempDir,
}

/// Config with explicit auth + SQL settings; everything else takes its defaults.
///
/// Built from `Config::default_for_tests`-style defaults rather than the process
/// environment, so a developer's shell cannot change what the tests assert.
pub fn config(enforce: bool) -> beacon_server_config::Config {
    let mut config = beacon_server_config::Config::load().expect("load config");
    config.admin.username = ADMIN_USERNAME.to_string();
    config.admin.password = ADMIN_PASSWORD.to_string();
    config.auth.enforce = enforce;
    config.auth.anonymous_enabled = true;
    config.sql.enable = true;
    config
}

/// A server with grant enforcement off (the default posture).
pub async fn test_server() -> TestServer {
    server_with(config(false)).await
}

/// A server built from `config`, with every path relocated under a fresh temp root.
///
/// The server itself is an ordinary persistent one; throwaway state comes from the
/// directory it is pointed at, not from a special mode on `Server`.
pub async fn server_with(mut config: beacon_server_config::Config) -> TestServer {
    let root = tempfile::tempdir().expect("create temp data dir");
    let base = root.path();

    config.data.datasets = base.join("datasets");
    config.data.tmp = base.join("tmp");
    config.data.db_file = base.join("tables").join("beacon.db");
    // A bucket would outlive the temp root, so tests are always local.
    config.s3.datasets_on_s3 = false;

    for dir in [
        &config.data.datasets,
        &config.data.tmp,
        &base.join("tables"),
    ] {
        std::fs::create_dir_all(dir).expect("create temp data dir");
    }

    // Tests run queries on the runtime they run on; only the binary has two.
    let server = Server::open(Arc::new(config), tokio::runtime::Handle::current())
        .await
        .expect("server should open");
    TestServer {
        server: Arc::new(server),
        _root: root,
    }
}

/// An HTTP `Authorization: Basic` header value.
pub fn basic(username: &str, password: &str) -> String {
    use base64::{engine::general_purpose, Engine as _};
    format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{username}:{password}"))
    )
}

/// A name unique to this test run, so tests sharing a server cannot collide.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

impl TestServer {
    /// The datasets directory, where a test writes the files that it queries.
    pub fn datasets_dir(&self) -> &Path {
        &self.server.config().data.datasets
    }

    /// The path of the auth store file, next to `beacon.db`.
    pub fn auth_store_path(&self) -> std::path::PathBuf {
        beacon_server::auth::auth_store_path(self.server.config())
    }

    /// Drops this server and opens a new one with the same config over the same temp root.
    ///
    /// # Panics
    ///
    /// Panics when a clone of `server` is still alive, or when the new server does not open.
    pub async fn restart(self) -> TestServer {
        let TestServer { server, _root } = self;
        let config = Arc::clone(server.config());
        // The redb file `beacon.db` stays locked while a server lives.
        assert_eq!(
            Arc::strong_count(&server),
            1,
            "drop each clone of the server before a restart"
        );
        drop(server);

        let server = Server::open(config, tokio::runtime::Handle::current())
            .await
            .expect("server should open again");
        TestServer {
            server: Arc::new(server),
            _root,
        }
    }

    /// Logs in with a username and a password, and panics when the login fails.
    pub async fn login(&self, username: &str, password: &str) -> AuthIdentity {
        self.server
            .authenticate(&Credential::basic(username, password))
            .await
            .unwrap_or_else(|e| panic!("{username} should log in: {e}"))
    }

    /// Logs in as the super-user of the config.
    pub async fn admin(&self) -> AuthIdentity {
        self.login(ADMIN_USERNAME, ADMIN_PASSWORD).await
    }

    /// Runs a statement as the super-user and collects its rows. Panics on an error.
    pub async fn sql(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql_as(sql, self.admin().await).await
    }

    /// Runs a statement as `identity` and collects its rows. Panics on an error.
    pub async fn sql_as(&self, sql: &str, identity: AuthIdentity) -> Vec<RecordBatch> {
        self.try_sql_as(sql, identity)
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e:#}"))
    }

    /// Runs a statement as the super-user and returns the error to the caller.
    pub async fn try_sql(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        self.try_sql_as(sql, self.admin().await).await
    }

    /// Runs a statement as `identity` through `Server::run_query` and collects its rows.
    ///
    /// # Errors
    ///
    /// Returns an error when the server refuses the statement or when execution fails.
    pub async fn try_sql_as(
        &self,
        sql: &str,
        identity: AuthIdentity,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let batches = self
            .server
            .run_query(Query::sql(sql.to_string()), identity)
            .await?
            .into_record_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(batches)
    }
}

/// The number of rows in all batches.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The values of the string column at `col`, in row order across the batches.
///
/// The column can be a plain, a large or a view string array.
pub fn column_strings(batches: &[RecordBatch], col: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = arrow::compute::cast(batch.column(col), &DataType::Utf8)
                .expect("the column should cast to Utf8");
            let values = column.as_string::<i32>();
            (0..values.len())
                .map(|row| values.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The single string of a one-row result. The first column holds the string.
pub fn scalar_string(batches: &[RecordBatch]) -> String {
    let values = column_strings(batches, 0);
    assert_eq!(values.len(), 1, "expected exactly one row");
    values.into_iter().next().expect("one row")
}
