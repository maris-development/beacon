//! Core-owned runtime settings.
//!
//! beacon-core is configured exclusively through [`crate::runtime_builder::RuntimeBuilder`];
//! it does not read the environment and has no dependency on `beacon-config`. The
//! embedding layer (beacon-api) is responsible for translating its own configuration
//! into builder calls.
//!
//! The subset of settings that plan-time code needs to reach without a `Runtime`
//! handle is published as a `SessionConfig` extension ([`CoreSettings`]) and read
//! back via [`CoreSettings::from_session`].

use std::sync::Arc;

use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

/// The object-store URLs beacon's stores are registered under and that plan-time
/// code resolves against:
///
/// - `datasets` — where dataset paths resolve: a `CREATE EXTERNAL TABLE`
///   `LOCATION`, a `COPY … TO` target, a JSON query's `from` paths;
/// - `tables` — the redb tables store (`db://`): table definitions, managed Lance
///   data and materialized-view data;
/// - `tmp` — query output (`tmp://`).
///
/// Published as a single `SessionConfig` extension because plan-time code has no
/// `Runtime` handle. There is exactly one store per URL per runtime, and each URL
/// here is the same one its store is registered under (see
/// [`register_object_stores`](crate::object_stores::register_object_stores)). Should
/// a URL here ever drift from the URL its store was registered under, queries, the
/// crawler and materialized views would address a store nothing populated, with no
/// error at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreUrls {
    pub datasets: ObjectStoreUrl,
    pub tables: ObjectStoreUrl,
    pub tmp: ObjectStoreUrl,
}

impl Default for ObjectStoreUrls {
    /// Beacon's conventional schemes. This is the single definition of them:
    /// embedders override the datasets URL (and the store behind it) through
    /// [`RuntimeBuilder::with_default_store`](crate::runtime_builder::RuntimeBuilder::with_default_store),
    /// and the tables/tmp URLs through
    /// [`with_tables_store_url`](crate::runtime_builder::RuntimeBuilder::with_tables_store_url)
    /// / [`with_tmp_store_url`](crate::runtime_builder::RuntimeBuilder::with_tmp_store_url).
    fn default() -> Self {
        Self {
            datasets: ObjectStoreUrl::parse("datasets://").expect("a valid store URL"),
            tables: ObjectStoreUrl::parse("db://").expect("a valid store URL"),
            tmp: ObjectStoreUrl::parse("tmp://").expect("a valid store URL"),
        }
    }
}

impl ObjectStoreUrls {
    /// Reads the URLs published on `session`, falling back to the defaults when the
    /// extension is absent (e.g. a bare `SessionContext` in a unit test).
    pub fn from_session(session: &SessionContext) -> Arc<Self> {
        session
            .state()
            .config()
            .get_extension::<Self>()
            .unwrap_or_default()
    }

    pub fn datasets(&self) -> &ObjectStoreUrl {
        &self.datasets
    }

    pub fn tables(&self) -> &ObjectStoreUrl {
        &self.tables
    }

    pub fn tmp(&self) -> &ObjectStoreUrl {
        &self.tmp
    }
}

/// Settings that plan-time code reads out of the `SessionConfig` extensions.
///
/// The secrets master key is deliberately not here: it lives on the
/// `SecretStore` extension, which the lower layers (beacon-sql-databases) can
/// reach without depending on beacon-core.
#[derive(Debug, Clone, Default)]
pub struct CoreSettings {
    pub sql: SqlSettings,
}

impl CoreSettings {
    /// Reads the settings published on `session`, falling back to defaults when the
    /// extension is absent (e.g. a bare `SessionContext` in a unit test).
    pub fn from_session(session: &SessionContext) -> Arc<Self> {
        session
            .state()
            .config()
            .get_extension::<Self>()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct SqlSettings {
    /// Table used by a JSON query that omits `from`.
    pub default_table: String,
    /// Push the projected column set into the scan when compiling JSON queries.
    pub enable_pushdown_projection: bool,
    pub stream_coalesce: StreamCoalesceSettings,
}

impl Default for SqlSettings {
    fn default() -> Self {
        Self {
            default_table: "default".to_string(),
            enable_pushdown_projection: true,
            stream_coalesce: StreamCoalesceSettings::default(),
        }
    }
}

/// Coalescing of small output batches into larger ones before they reach the client.
#[derive(Debug, Clone)]
pub struct StreamCoalesceSettings {
    pub enabled: bool,
    pub target_rows: usize,
    pub flush_timeout_ms: u64,
    pub max_rows: usize,
}

impl Default for StreamCoalesceSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            target_rows: 65536,
            flush_timeout_ms: 25,
            max_rows: 262144,
        }
    }
}
