//! Proves the runtime takes its configuration as an argument (rather than from a
//! process-global) and that two runtimes in one process honor their own config.

use std::sync::Arc;

use beacon_core::runtime::Runtime;

/// Build a config from the environment defaults, then override the default table
/// and isolate the data directories under a per-runtime temp root. This both
/// constructs the config in code (no reliance on a process-global) and gives each
/// runtime its own object stores.
fn config_with(default_table: &str, tag: &str) -> Arc<beacon_config::Config> {
    let mut config = beacon_config::Config::load().expect("load base config");
    config.sql.default_table = default_table.to_string();

    let root = std::env::temp_dir().join(format!("beacon-runtime-config-test-{tag}"));
    config.storage.datasets_dir = root.join("datasets");
    config.storage.db_path = None; // in-memory tables store: independent per runtime
    config.storage.tmp_dir = root.join("tmp");
    config.data.indexes = root.join("indexes");
    config.data.cache = root.join("cache");
    config.storage.data_dir = root;
    for dir in [
        &config.storage.data_dir,
        &config.storage.datasets_dir,
        &config.storage.tmp_dir,
        &config.data.indexes,
        &config.data.cache,
    ] {
        std::fs::create_dir_all(dir).expect("create data dir");
    }

    Arc::new(config)
}

/// Two runtimes built from different configs in the same process each reflect
/// their own configuration — confirming the config is owned per-runtime and not
/// read from a process-global.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_runtimes_honor_their_own_config() {
    let rt_alpha = Runtime::new(config_with("alpha_table", "alpha"))
        .await
        .expect("alpha runtime should boot");
    let rt_bravo = Runtime::new(config_with("bravo_table", "bravo"))
        .await
        .expect("bravo runtime should boot");

    assert_eq!(rt_alpha.default_table(), "alpha_table");
    assert_eq!(rt_bravo.default_table(), "bravo_table");

    // The first runtime is unaffected by the second's construction.
    assert_eq!(rt_alpha.default_table(), "alpha_table");
}
