//! Selecting the storage engine for managed `CREATE TABLE`.
//!
//! The effective engine is resolved per statement: a session-scoped
//! `SET beacon.table_engine = 'iceberg'|'lance'` overrides the global default
//! ([`beacon_config::SqlConfig::default_table_engine`], itself from
//! `BEACON_DEFAULT_TABLE_ENGINE`, defaulting to Lance).
//!
//! The override is a DataFusion [`ConfigExtension`] so it rides the normal `SET`
//! machinery and is visible to session-scoped execution.

use beacon_config::{Config, TableEngine};
use datafusion::config::ConfigExtension;
use datafusion::prelude::SessionContext;

datafusion::common::extensions_options! {
    /// Beacon's session-scoped SQL options, settable via `SET beacon.<key>`.
    pub struct BeaconSqlOptions {
        /// Managed `CREATE TABLE` engine override (`lance` or `iceberg`). Empty
        /// means "use the global default".
        pub table_engine: String, default = String::new()
    }
}

impl ConfigExtension for BeaconSqlOptions {
    const PREFIX: &'static str = "beacon";
}

/// Resolve the engine for a managed `CREATE TABLE`: a non-empty session
/// `SET beacon.table_engine` wins; otherwise the global config default.
pub(crate) fn resolve_table_engine(session: &SessionContext) -> anyhow::Result<TableEngine> {
    let state = session.state();

    if let Some(options) = state.config_options().extensions.get::<BeaconSqlOptions>() {
        let value = options.table_engine.trim();
        if !value.is_empty() {
            return value
                .parse::<TableEngine>()
                .map_err(|e| anyhow::anyhow!("invalid SET beacon.table_engine: {e}"));
        }
    }

    let config = state
        .config()
        .get_extension::<Config>()
        .ok_or_else(|| anyhow::anyhow!("Beacon configuration is unavailable"))?;
    Ok(config.sql.default_table_engine)
}
