//! Shared harness for the transport integration tests.
//!
//! Each test gets its own lake rooted in a fresh temporary directory — datasets,
//! tables store and scratch space alike — so nothing leaks between tests and the
//! whole lot is removed when the `TempDir` drops.

use std::sync::Arc;

use beacon_datalake::DataLake;
use tempfile::TempDir;

/// Credentials of the config-defined super-user every test lake bootstraps.
pub const ADMIN_USERNAME: &str = "beacon-admin";
pub const ADMIN_PASSWORD: &str = "beacon-password";

/// A lake plus the temp root backing it. Dropping this removes the root, so it
/// has to outlive every use of `lake`.
pub struct TestLake {
    pub lake: Arc<DataLake>,
    _root: TempDir,
}

/// Config with explicit auth + SQL settings; everything else takes its defaults.
///
/// Built from `Config::default_for_tests`-style defaults rather than the process
/// environment, so a developer's shell cannot change what the tests assert.
pub fn config(enforce: bool) -> beacon_datalake_config::Config {
    let mut config = beacon_datalake_config::Config::load().expect("load config");
    config.admin.username = ADMIN_USERNAME.to_string();
    config.admin.password = ADMIN_PASSWORD.to_string();
    config.auth.enforce = enforce;
    config.auth.anonymous_enabled = true;
    config.sql.enable = true;
    config
}

/// A lake with grant enforcement off (the default posture).
pub async fn test_lake() -> TestLake {
    lake_with(config(false)).await
}

/// A lake built from `config`, with every path relocated under a fresh temp root.
///
/// The lake itself is an ordinary persistent one; throwaway state comes from the
/// directory it is pointed at, not from a special mode on `DataLake`.
pub async fn lake_with(mut config: beacon_datalake_config::Config) -> TestLake {
    let root = tempfile::tempdir().expect("create temp data dir");
    let base = root.path();

    config.data.datasets = base.join("datasets");
    config.data.tmp = base.join("tmp");
    config.data.db_file = base.join("tables").join("beacon.db");
    // A bucket would outlive the temp root, so tests are always local.
    config.s3.data_lake = false;

    for dir in [
        &config.data.datasets,
        &config.data.tmp,
        &base.join("tables"),
    ] {
        std::fs::create_dir_all(dir).expect("create temp data dir");
    }

    let lake = DataLake::open(Arc::new(config))
        .await
        .expect("lake should open");
    TestLake {
        lake: Arc::new(lake),
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

/// A name unique to this test run, so tests sharing a lake cannot collide.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}
