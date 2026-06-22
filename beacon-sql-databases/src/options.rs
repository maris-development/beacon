//! Translation of Beacon's user-facing connection options into the parameter
//! maps expected by `datafusion-table-providers` connection pools.
//!
//! Beacon exposes a small, engine-neutral set of `OPTIONS` keys
//! (`host`, `port`, `user`, `database`, `sslmode`, …) in the `CREATE EXTERNAL
//! TABLE` DDL. The password is handled separately (encrypted), so it is never
//! part of the persisted `options` map. This module maps the neutral keys to
//! each engine's pool keys (e.g. MySQL wants `tcp_port`, both want `db`/`pass`).

use std::collections::{BTreeMap, HashMap};

use datafusion_table_providers::util::secrets::to_secret_map;
use secrecy::SecretString;

use crate::SqlEngine;

/// Build the engine-specific connection-pool parameter map from beacon's
/// neutral `options` plus the decrypted `password` (if any).
pub(crate) fn build_pool_params(
    engine: SqlEngine,
    options: &BTreeMap<String, String>,
    password: Option<SecretString>,
) -> HashMap<String, SecretString> {
    let mut params: HashMap<String, String> = HashMap::with_capacity(options.len() + 1);
    for (key, value) in options {
        let mapped = match key.as_str() {
            // Both pools use `db` for the database name.
            "database" | "dbname" => "db",
            // Postgres uses `port`; MySQL uses `tcp_port`.
            "port" => engine.port_key(),
            other => other,
        };
        params.insert(mapped.to_string(), value.clone());
    }

    let mut secret_params = to_secret_map(params);
    if let Some(password) = password {
        // Both pools read the password from `pass`.
        secret_params.insert("pass".to_string(), password);
    }
    secret_params
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret as _;

    #[cfg(feature = "mysql")]
    #[test]
    fn maps_neutral_keys_to_mysql_pool_keys() {
        let mut opts = BTreeMap::new();
        opts.insert("host".to_string(), "db.internal".to_string());
        opts.insert("port".to_string(), "3306".to_string());
        opts.insert("database".to_string(), "shop".to_string());
        let params = build_pool_params(
            SqlEngine::MySql,
            &opts,
            Some(SecretString::from("pw".to_string())),
        );
        assert!(params.contains_key("tcp_port"));
        assert!(!params.contains_key("port"));
        assert_eq!(params["db"].expose_secret(), "shop");
        assert_eq!(params["pass"].expose_secret(), "pw");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn keeps_port_for_postgres() {
        let mut opts = BTreeMap::new();
        opts.insert("port".to_string(), "5432".to_string());
        let params = build_pool_params(SqlEngine::Postgres, &opts, None);
        assert!(params.contains_key("port"));
        assert!(!params.contains_key("pass"));
    }
}
