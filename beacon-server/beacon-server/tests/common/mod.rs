//! Shared harness for the transport integration tests.
//!
//! Each test gets its own server rooted in a fresh temporary directory — datasets,
//! tables store and scratch space alike — so nothing leaks between tests and the
//! whole lot is removed when the `TempDir` drops.

use std::sync::Arc;

use beacon_server::Server;
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
