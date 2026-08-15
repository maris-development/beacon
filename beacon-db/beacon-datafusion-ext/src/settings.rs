//! The `beacon.*` configuration namespace.
//!
//! [`BeaconOptions`] is a DataFusion [`ConfigExtension`], which is what makes
//! `SET beacon.netcdf.use_rust_reader = true` reach beacon at all: DataFusion's
//! `SET` writes `ConfigOptions`, and `ConfigOptions` routes a namespaced key to
//! the extension registered under its prefix. A value set this way is visible to
//! every later query, because plan- and execution-time code reads the options off
//! the session it is handed rather than off a snapshot taken at startup.
//!
//! This is deliberately *not* `SessionConfig::with_extension`, the `TypeId`-keyed
//! map beacon used before. That map is invisible to `SET`, so a setting published
//! there could only ever be read, never changed.
//!
//! # Scope
//!
//! A `SET` applies to the whole server, not to one client: beacon runs one shared
//! `SessionContext` for every transport and every user. That is why `SET` is
//! super-user-only (`validate_query_plan`), exactly as `SET datafusion.*` already
//! was.
//!
//! # Layers
//!
//! A format setting has three layers, narrowest last:
//!
//! 1. the runtime default, from the `BEACON_*` environment variable,
//! 2. this namespace, changed with `SET`,
//! 3. the per-table `CREATE EXTERNAL TABLE ... OPTIONS (...)` override.

use std::any::Any;
use std::collections::HashMap;

use datafusion::catalog::Session;
use datafusion::common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ConfigOptions, ExtensionOptions, Visit,
};
use datafusion::common::config_namespace;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionConfig;

/// The namespace every setting in this module is addressed under.
pub const BEACON_PREFIX: &str = "beacon";

config_namespace! {
    /// Result-stream coalescing: the small record batches a plan emits are merged
    /// into client-sized ones before they leave the server.
    pub struct StreamCoalesceOptions {
        /// Whether to coalesce at all. Disabled passes batches through untouched.
        pub enabled: bool, default = true

        /// Buffer batches until at least this many rows have accumulated.
        pub target_rows: usize, default = 64 * 1024

        /// Flush a non-empty buffer after this long, even below `target_rows`, so a
        /// slow-producing plan stays responsive. `0` disables the timeout.
        pub flush_timeout_ms: u64, default = 25

        /// Hard upper bound on a buffered batch, so one oversized input batch cannot
        /// grow the buffer without limit.
        pub max_rows: usize, default = 256 * 1024
    }
}

config_namespace! {
    /// How a client query is compiled and how its result is streamed back.
    pub struct SqlOptions {
        /// Result-stream coalescing.
        pub stream_coalesce: StreamCoalesceOptions, default = Default::default()
    }
}

config_namespace! {
    /// NetCDF reader settings. Each is also a per-table `OPTIONS (...)` key.
    pub struct NetcdfOptions {
        /// Whether reads consult the shared reader cache. The cache's *capacity* is
        /// fixed when the runtime starts (`BEACON_NETCDF_READER_CACHE_SIZE`).
        pub use_reader_cache: bool, default = true

        /// Whether to compute per-file statistics during planning, used to prune the
        /// files a query cannot match. Needs the pure-Rust reader.
        pub enable_statistics: bool, default = true

        /// Whether reads go through the pure-Rust reader instead of netcdf-c.
        pub use_rust_reader: bool, default = false
    }
}

config_namespace! {
    /// HDF5 reader settings. Each is also a per-table `OPTIONS (...)` key.
    pub struct Hdf5Options {
        /// Whether reads consult the shared reader cache. Only the pure-Rust reader
        /// has a cache of its own; under netcdf-c the netCDF cache applies instead.
        pub use_reader_cache: bool, default = true

        /// Whether to compute per-file statistics during planning. Needs the
        /// pure-Rust reader.
        pub enable_statistics: bool, default = true

        /// Whether reads go through the pure-Rust reader instead of netcdf-c.
        pub use_rust_reader: bool, default = false
    }
}

config_namespace! {
    /// Zarr reader settings.
    pub struct ZarrOptions {
        /// Whether to compute per-store statistics during planning. A store answers
        /// from its metadata where it can, and otherwise reads only its rank-0 and
        /// rank-1 arrays.
        pub enable_statistics: bool, default = true
    }
}

config_namespace! {
    /// Atlas reader settings. Each is also a per-table `OPTIONS (...)` key.
    pub struct AtlasOptions {
        /// Whether reads consult the shared reader cache. The cache's *capacity* is
        /// fixed when the runtime starts (`BEACON_ATLAS_READER_CACHE_SIZE`).
        pub use_reader_cache: bool, default = true

        /// Whether a predicate scan drops the datasets that cannot match before
        /// reading them. A pure optimization.
        pub use_pruning: bool, default = true
    }
}

config_namespace! {
    /// Beacon Binary Format settings.
    pub struct BbfOptions {
        /// Whether to split each record batch into `batch_size`-row slices, which
        /// bounds peak memory on a wide table.
        pub split_streams_slice: bool, default = false
    }
}

config_namespace! {
    /// Managed-Lance settings. An empty value means "leave it to Lance", which is
    /// what an unset `BEACON_LANCE_*` variable meant.
    ///
    /// The first four apply when beacon *writes* a Lance table, so they change the
    /// files a later `CREATE TABLE`/`INSERT` produces, never the ones already on
    /// disk. `materialization` is a read setting and applies to the next scan.
    pub struct LanceOptions {
        /// Block compression for string columns: `fsst`, `zstd`, `lz4`, or `none`.
        pub compression: String, default = String::new()

        /// Block compression for numeric columns: `zstd`, `lz4`, or `none`. `none`
        /// also disables bitpacking and RLE, which usually measures *larger*.
        pub numeric_compression: String, default = String::new()

        /// Lance file format version: `2.0`, `2.1`, or `2.2`.
        pub version: String, default = String::new()

        /// Minichunk size in bytes. Needs `version` = `2.2` to take effect.
        pub minichunk: String, default = String::new()

        /// Column materialization on a scan: `late` or `early`.
        pub materialization: String, default = String::new()
    }
}

config_namespace! {
    /// Every runtime-settable beacon setting.
    ///
    /// Registered on the session as a [`ConfigExtension`], so `SET beacon.x = y`,
    /// `RESET beacon.x`, `SHOW beacon.x` and `information_schema.df_settings` all
    /// work against it with no further wiring.
    pub struct BeaconOptions {
        /// The table a JSON query without a `from` resolves against. SQL always
        /// names its own source, so this only affects the JSON query API.
        pub default_table: String, default = "default".to_string()

        /// Whether the JSON query compiler pushes the selected columns into the scan.
        pub enable_pushdown_projection: bool, default = true

        /// Whether the N-dimensional pipeline optimizer sinks element-wise
        /// projections and filters below the grid broadcast. The base nd pipeline
        /// always runs; this only enables the node-rewriting optimization.
        pub enable_nd_pipeline: bool, default = false

        /// How client queries are compiled and streamed.
        pub sql: SqlOptions, default = Default::default()

        /// NetCDF reader settings.
        pub netcdf: NetcdfOptions, default = Default::default()

        /// HDF5 reader settings.
        pub hdf5: Hdf5Options, default = Default::default()

        /// Zarr reader settings.
        pub zarr: ZarrOptions, default = Default::default()

        /// Atlas reader settings.
        pub atlas: AtlasOptions, default = Default::default()

        /// Beacon Binary Format settings.
        pub bbf: BbfOptions, default = Default::default()

        /// Managed-Lance settings.
        pub lance: LanceOptions, default = Default::default()
    }
}

impl ConfigExtension for BeaconOptions {
    const PREFIX: &'static str = BEACON_PREFIX;
}

impl ExtensionOptions for BeaconOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// The key arrives without the namespace: DataFusion splits `beacon.netcdf.x`
    /// once and hands the extension `netcdf.x`.
    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        ConfigField::set(self, key, value)
    }

    /// Emits **fully qualified** keys (`beacon.netcdf.use_rust_reader`).
    ///
    /// This is why the trait is written out rather than generated by
    /// `extensions_options!`, whose `entries()` emits the bare field name. The
    /// qualified form is what `information_schema.df_settings` displays, and what
    /// `SHOW <key>` validates a name against — a bare `use_rust_reader` would make
    /// `SHOW beacon.netcdf.use_rust_reader` fail as an unknown variable.
    fn entries(&self) -> Vec<ConfigEntry> {
        struct Collector(Vec<ConfigEntry>);

        impl Visit for Collector {
            fn some<V: std::fmt::Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                });
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                });
            }
        }

        let mut collector = Collector(Vec::new());
        self.visit(&mut collector, BEACON_PREFIX, "");
        collector.0
    }
}

impl BeaconOptions {
    /// The options published on `config`, or `None` when the namespace is absent.
    ///
    /// Absent means the session was not built by beacon's runtime — a bare
    /// `SessionContext` in a unit test, or an embedder wiring a format factory up
    /// by hand. A caller that holds its own configuration should fall back to it
    /// rather than to the compiled defaults, which is why this is separate from
    /// [`Self::from_config`].
    pub fn try_from_config(config: &SessionConfig) -> Option<Self> {
        config.options().extensions.get::<BeaconOptions>().cloned()
    }

    /// [`Self::try_from_config`] for the `&dyn Session` a `TableProvider` or
    /// `FileFormatFactory` is handed, where no `SessionContext` is in reach.
    pub fn try_from_session(session: &dyn Session) -> Option<Self> {
        Self::try_from_config(session.config())
    }

    /// The options published on `config`, or the compiled defaults when the
    /// namespace is absent.
    pub fn from_config(config: &SessionConfig) -> Self {
        Self::try_from_config(config).unwrap_or_default()
    }

    /// [`Self::from_config`] for the `&dyn Session` a `TableProvider` or
    /// `FileFormatFactory` is handed, where no `SessionContext` is in reach.
    pub fn from_session(session: &dyn Session) -> Self {
        Self::from_config(session.config())
    }

    /// Whether `key` names a setting in this namespace, with or without the
    /// `beacon.` prefix.
    pub fn has_key(key: &str) -> bool {
        let qualified = match key.strip_prefix("beacon.") {
            Some(_) => key.to_string(),
            None => format!("{BEACON_PREFIX}.{key}"),
        };
        Self::default()
            .entries()
            .iter()
            .any(|entry| entry.key == qualified)
    }

    /// Every key in this namespace, fully qualified and sorted.
    pub fn keys() -> Vec<String> {
        let mut keys: Vec<String> = Self::default()
            .entries()
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        keys.sort();
        keys
    }
}

/// The value every setting held when the runtime started, before any `SET`.
///
/// `RESET beacon.x` restores from here rather than from DataFusion's
/// `ConfigOptions::reset`, which would reinstate DataFusion's *compiled* default
/// and silently discard the value the operator's environment supplied. Published
/// as a plain typed session extension, since it never changes after startup.
///
/// Two layers, because the two `RESET` statements return to different places:
///
/// * `startup` — what the server actually came up with: the environment, then any
///   `ALTER SYSTEM SET` value replayed over it. Plain `RESET` restores this.
/// * `environment` — the same snapshot *before* the replay. `ALTER SYSTEM RESET`
///   restores this, since it is deleting the persisted value and must not put it
///   straight back.
///
/// On a runtime with nothing persisted the two are identical.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootSettings {
    startup: HashMap<String, String>,
    environment: HashMap<String, String>,
}

impl BootSettings {
    /// Snapshots every set option, beacon's namespace and DataFusion's alike,
    /// as both layers — the state before any persisted value is replayed.
    ///
    /// An option with no value (an unset `Option<T>`) is skipped: there is no
    /// string that would restore it, so those fall back to DataFusion's own
    /// `RESET`.
    pub fn capture(options: &ConfigOptions) -> Self {
        let values = Self::values(options);
        Self {
            startup: values.clone(),
            environment: values,
        }
    }

    /// This snapshot with `startup` re-taken from `options`, keeping the original
    /// `environment` layer. Called once, after the persisted settings are
    /// replayed, so plain `RESET` returns to the state the server came up with.
    pub fn with_startup(&self, options: &ConfigOptions) -> Self {
        Self {
            startup: Self::values(options),
            environment: self.environment.clone(),
        }
    }

    fn values(options: &ConfigOptions) -> HashMap<String, String> {
        options
            .entries()
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect()
    }

    /// The value of `key` before any persisted override — what an
    /// `ALTER SYSTEM RESET` returns to.
    pub fn environment(&self, key: &str) -> Option<&str> {
        self.environment.get(key).map(String::as_str)
    }

    /// The value of `key` the server actually started with — what a plain `RESET`
    /// returns to.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.startup.get(key).map(String::as_str)
    }

    /// The options published on `config`, or an empty snapshot for a session
    /// beacon did not build.
    pub fn from_config(config: &SessionConfig) -> Self {
        config
            .get_extension::<BootSettings>()
            .map(|boot| (*boot).clone())
            .unwrap_or_default()
    }
}

/// The `BEACON_*` variable behind a `beacon.*` key that can only be set at
/// startup, or `None` when the key is not one of those.
///
/// Every one of these decides something built once — a socket, a directory, a
/// credential, a thread pool, a cache's capacity — so a `SET` would appear to
/// work and change nothing. Rejecting it by name, with the variable to edit, is
/// the useful answer.
pub fn startup_only_env_var(key: &str) -> Option<&'static str> {
    let key = key.strip_prefix("beacon.").unwrap_or(key);
    let var = match key {
        "port" => "BEACON_PORT",
        "host" => "BEACON_HOST",
        "worker_threads" => "BEACON_WORKER_THREADS",
        "log_level" => "BEACON_LOG_LEVEL",
        "base_path" => "BEACON_BASE_PATH",
        "web_ui_dir" => "BEACON_WEB_UI_DIR",
        "max_upload_bytes" => "BEACON_MAX_UPLOAD_BYTES",
        "vm_memory_size" => "BEACON_VM_MEMORY_SIZE",
        "enable_sql" => "BEACON_ENABLE_SQL",
        "enable_sys_info" => "BEACON_ENABLE_SYS_INFO",
        "data_dir" => "BEACON_DATA_DIR",
        "secrets_key" => "BEACON_SECRETS_KEY",
        "stats_cache_capacity" => "BEACON_STATS_CACHE_CAPACITY",
        "netcdf.reader_cache_size" => "BEACON_NETCDF_READER_CACHE_SIZE",
        "hdf5.reader_cache_size" => "BEACON_HDF5_READER_CACHE_SIZE",
        "atlas.reader_cache_size" => "BEACON_ATLAS_READER_CACHE_SIZE",
        "admin.username" => "BEACON_ADMIN_USERNAME",
        "admin.password" => "BEACON_ADMIN_PASSWORD",
        "auth.enforce" => "BEACON_AUTH_ENFORCE",
        "auth.anonymous_enabled" => "BEACON_AUTH_ANONYMOUS_ENABLED",
        "crawler.enable" => "BEACON_CRAWLER_ENABLE",
        "crawler.default_interval_secs" => "BEACON_CRAWLER_DEFAULT_INTERVAL_SECS",
        "file_stats.enable" => "BEACON_FILE_STATS_ENABLE",
        "file_stats.interval_secs" => "BEACON_FILE_STATS_INTERVAL_SECS",
        "file_stats.on_startup" => "BEACON_FILE_STATS_ON_STARTUP",
        "file_stats.concurrency" => "BEACON_FILE_STATS_CONCURRENCY",
        "file_stats.batch_files" => "BEACON_FILE_STATS_BATCH_FILES",
        other => return startup_only_family(other),
    };
    Some(var)
}

/// The startup-only keys that share a prefix, matched as a family so a whole
/// group answers with one entry rather than twenty.
fn startup_only_family(key: &str) -> Option<&'static str> {
    let (prefix, _) = key.split_once('.')?;
    let var = match prefix {
        "s3" => "BEACON_S3_*",
        "oidc" => "BEACON_OIDC_*",
        "flight_sql" => "BEACON_FLIGHT_SQL_*",
        "cors" => "BEACON_CORS_*",
        "api" => "BEACON_API_*",
        "file_stats" => "BEACON_FILE_STATS_*",
        "crawler" => "BEACON_CRAWLER_*",
        _ => return None,
    };
    Some(var)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every key must be addressable by the exact name `entries()` advertises.
    /// `SHOW <key>` validates against that list and `df_settings` displays it, so
    /// a key that reports one name and accepts another is broken in both.
    #[test]
    fn every_advertised_key_round_trips() {
        let mut options = BeaconOptions::default();
        for key in BeaconOptions::keys() {
            let unqualified = key
                .strip_prefix("beacon.")
                .unwrap_or_else(|| panic!("`{key}` is not in the beacon namespace"));
            // A value every field type accepts: `bool` takes "true", the numeric
            // fields would not, so probe with the type's own current rendering.
            let current = options
                .entries()
                .into_iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| entry.value)
                .unwrap_or_default();
            ExtensionOptions::set(&mut options, unqualified, &current)
                .unwrap_or_else(|e| panic!("`{key}` is advertised but not settable: {e}"));
        }
    }

    #[test]
    fn keys_are_fully_qualified_and_cover_every_namespace() {
        let keys = BeaconOptions::keys();
        assert!(keys.iter().all(|key| key.starts_with("beacon.")));
        for expected in [
            "beacon.default_table",
            "beacon.enable_pushdown_projection",
            "beacon.enable_nd_pipeline",
            "beacon.sql.stream_coalesce.target_rows",
            "beacon.netcdf.use_rust_reader",
            "beacon.hdf5.use_rust_reader",
            "beacon.zarr.enable_statistics",
            "beacon.atlas.use_pruning",
            "beacon.bbf.split_streams_slice",
            "beacon.lance.materialization",
        ] {
            assert!(keys.iter().any(|key| key == expected), "missing {expected}");
        }
    }

    #[test]
    fn set_changes_the_value_entries_reports() {
        let mut options = BeaconOptions::default();
        assert!(!options.netcdf.use_rust_reader);

        ExtensionOptions::set(&mut options, "netcdf.use_rust_reader", "true").unwrap();
        assert!(options.netcdf.use_rust_reader);

        let entry = options
            .entries()
            .into_iter()
            .find(|entry| entry.key == "beacon.netcdf.use_rust_reader")
            .expect("key is advertised");
        assert_eq!(entry.value.as_deref(), Some("true"));
    }

    #[test]
    fn unknown_key_is_rejected() {
        let mut options = BeaconOptions::default();
        let err = ExtensionOptions::set(&mut options, "netcdf.nope", "true").unwrap_err();
        assert!(err.to_string().contains("nope"), "unhelpful error: {err}");
    }

    /// A `SET` writes through `ConfigOptions`, so the extension has to be reachable
    /// by its prefix — the whole point of registering it as a `ConfigExtension`.
    #[test]
    fn config_options_routes_the_beacon_prefix() {
        let mut options = ConfigOptions::default();
        options.extensions.insert(BeaconOptions::default());

        options
            .set("beacon.sql.stream_coalesce.target_rows", "1024")
            .unwrap();

        let beacon = options.extensions.get::<BeaconOptions>().unwrap();
        assert_eq!(beacon.sql.stream_coalesce.target_rows, 1024);
    }

    /// `SHOW ALL` and `information_schema.df_settings` both read `entries()`, so the
    /// beacon keys have to show up there alongside DataFusion's own.
    #[test]
    fn beacon_keys_appear_in_config_options_entries() {
        let mut options = ConfigOptions::default();
        options.extensions.insert(BeaconOptions::default());

        let keys: Vec<String> = options.entries().into_iter().map(|e| e.key).collect();
        assert!(keys.iter().any(|k| k == "beacon.default_table"));
        assert!(keys.iter().any(|k| k == "datafusion.execution.batch_size"));
    }

    #[test]
    fn has_key_accepts_qualified_and_bare_names() {
        assert!(BeaconOptions::has_key("beacon.default_table"));
        assert!(BeaconOptions::has_key("default_table"));
        assert!(!BeaconOptions::has_key("beacon.port"));
        assert!(!BeaconOptions::has_key("datafusion.execution.batch_size"));
    }

    #[test]
    fn boot_settings_record_the_values_the_runtime_started_with() {
        let mut options = ConfigOptions::default();
        options.extensions.insert(BeaconOptions::default());
        options.execution.batch_size = 4096;
        options.set("beacon.default_table", "observations").unwrap();

        let boot = BootSettings::capture(&options);

        // Both namespaces are captured, so `RESET` restores an operator's
        // environment value rather than DataFusion's compiled default.
        assert_eq!(boot.get("beacon.default_table"), Some("observations"));
        assert_eq!(boot.get("datafusion.execution.batch_size"), Some("4096"));
        assert_eq!(boot.get("beacon.nope"), None);
    }

    /// The two `RESET` statements return to different places, so the snapshot
    /// keeps both layers. Getting this wrong is silent: `ALTER SYSTEM RESET` would
    /// restore the very value it just deleted.
    #[test]
    fn the_snapshot_separates_the_environment_from_a_persisted_value() {
        let mut options = ConfigOptions::default();
        options.extensions.insert(BeaconOptions::default());
        options.set("beacon.default_table", "from_env").unwrap();

        // At build time both layers are the environment's.
        let boot = BootSettings::capture(&options);
        assert_eq!(boot.get("beacon.default_table"), Some("from_env"));
        assert_eq!(boot.environment("beacon.default_table"), Some("from_env"));

        // A persisted value is replayed over it, and only `startup` moves.
        options.set("beacon.default_table", "persisted").unwrap();
        let boot = boot.with_startup(&options);
        assert_eq!(
            boot.get("beacon.default_table"),
            Some("persisted"),
            "a plain RESET returns to what the server came up with"
        );
        assert_eq!(
            boot.environment("beacon.default_table"),
            Some("from_env"),
            "an ALTER SYSTEM RESET returns to the environment, not the value it is deleting"
        );
    }

    #[test]
    fn startup_only_keys_name_their_variable() {
        assert_eq!(startup_only_env_var("beacon.port"), Some("BEACON_PORT"));
        assert_eq!(startup_only_env_var("port"), Some("BEACON_PORT"));
        assert_eq!(
            startup_only_env_var("beacon.netcdf.reader_cache_size"),
            Some("BEACON_NETCDF_READER_CACHE_SIZE")
        );
        // Families answer for every member.
        assert_eq!(
            startup_only_env_var("beacon.s3.bucket"),
            Some("BEACON_S3_*")
        );
        assert_eq!(
            startup_only_env_var("beacon.flight_sql.port"),
            Some("BEACON_FLIGHT_SQL_*")
        );
        // A settable key must not be claimed as startup-only.
        assert_eq!(startup_only_env_var("beacon.netcdf.use_rust_reader"), None);
        assert_eq!(startup_only_env_var("beacon.default_table"), None);
    }

    /// The two tables must not overlap: a key claimed as startup-only would be
    /// rejected even though the extension can set it.
    #[test]
    fn no_settable_key_is_also_startup_only() {
        for key in BeaconOptions::keys() {
            assert_eq!(
                startup_only_env_var(&key),
                None,
                "`{key}` is settable but also listed as startup-only"
            );
        }
    }
}
