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
    // ODBC is configured by a single connection string, not pooled host/port keys, so it takes a
    // different assembly path.
    #[cfg(feature = "odbc")]
    if engine == SqlEngine::Odbc {
        return build_odbc_params(options, password);
    }

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

/// Build the ODBC pool's single `connection_string` parameter from beacon's options.
///
/// Two ways to configure it:
/// - a raw `connection_string` option, passed through verbatim (full control); or
/// - individual options joined as ODBC `Key=Value;` pairs — the user supplies ODBC key names
///   (`Driver`, `Server`, `Database`, `UID`, …), since connection-string keys are driver-specific
///   and beacon does not invent an abstraction over them.
///
/// The password is stored encrypted (never in `options`) and injected here as `PWD`, unless the
/// connection string already carries one.
#[cfg(feature = "odbc")]
fn build_odbc_params(
    options: &BTreeMap<String, String>,
    password: Option<SecretString>,
) -> HashMap<String, SecretString> {
    use secrecy::ExposeSecret as _;

    let mut connection_string = match options.get("connection_string") {
        Some(raw) => raw.clone(),
        None => options
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(";"),
    };

    if let Some(password) = password {
        if !connection_string.to_ascii_lowercase().contains("pwd=") {
            if !connection_string.is_empty() && !connection_string.ends_with(';') {
                connection_string.push(';');
            }
            connection_string.push_str("PWD=");
            connection_string.push_str(password.expose_secret());
        }
    }

    let mut params = HashMap::new();
    params.insert(
        "connection_string".to_string(),
        SecretString::from(connection_string),
    );
    params
}

#[cfg(all(test, feature = "odbc"))]
mod odbc_tests {
    use super::*;
    use secrecy::ExposeSecret as _;

    #[test]
    fn assembles_a_connection_string_from_options_and_injects_the_password() {
        let mut opts = BTreeMap::new();
        opts.insert("Driver".to_string(), "{ODBC Driver 18 for SQL Server}".to_string());
        opts.insert("Server".to_string(), "sql.internal,1433".to_string());
        opts.insert("Database".to_string(), "sales".to_string());
        opts.insert("UID".to_string(), "reader".to_string());

        let params = build_odbc_params(&opts, Some(SecretString::from("s3cret".to_string())));
        let cs = params["connection_string"].expose_secret();
        // BTreeMap orders keys, so this is deterministic.
        assert!(cs.contains("Driver={ODBC Driver 18 for SQL Server}"));
        assert!(cs.contains("Server=sql.internal,1433"));
        assert!(cs.contains("Database=sales"));
        assert!(cs.contains("UID=reader"));
        assert!(cs.ends_with("PWD=s3cret"));
    }

    #[test]
    fn a_raw_connection_string_is_passed_through() {
        let mut opts = BTreeMap::new();
        opts.insert(
            "connection_string".to_string(),
            "Driver={x};Server=h;Database=d".to_string(),
        );
        let params = build_odbc_params(&opts, None);
        assert_eq!(
            params["connection_string"].expose_secret(),
            "Driver={x};Server=h;Database=d"
        );
    }

    #[test]
    fn an_existing_pwd_is_not_duplicated() {
        let mut opts = BTreeMap::new();
        opts.insert("connection_string".to_string(), "Server=h;PWD=inline".to_string());
        let params = build_odbc_params(&opts, Some(SecretString::from("other".to_string())));
        // The connection string already has a password, so the secret is not appended.
        assert_eq!(params["connection_string"].expose_secret(), "Server=h;PWD=inline");
    }
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

    /// `dbname` is the libpq spelling; both it and `database` must land on the
    /// pools' `db` key.
    #[test]
    fn dbname_is_an_alias_for_database() {
        let mut opts = BTreeMap::new();
        opts.insert("dbname".to_string(), "shop".to_string());
        let params = build_pool_params(crate::source::tests::engine(), &opts, None);
        assert_eq!(params["db"].expose_secret(), "shop");
        assert!(!params.contains_key("dbname"));
    }

    /// Keys beacon does not translate are handed to the pool verbatim, so new
    /// engine options work without a code change here.
    #[test]
    fn unknown_keys_pass_through_unchanged() {
        let mut opts = BTreeMap::new();
        opts.insert("sslmode".to_string(), "require".to_string());
        opts.insert("user".to_string(), "beacon".to_string());
        let params = build_pool_params(crate::source::tests::engine(), &opts, None);
        assert_eq!(params["sslmode"].expose_secret(), "require");
        assert_eq!(params["user"].expose_secret(), "beacon");
    }

    /// The password is never carried in `options` (it is stored encrypted and
    /// injected here), so a stray `pass` option must not survive as the password.
    #[test]
    fn the_supplied_password_wins_over_an_options_entry() {
        let mut opts = BTreeMap::new();
        opts.insert("pass".to_string(), "stale".to_string());
        let params = build_pool_params(
            crate::source::tests::engine(),
            &opts,
            Some(SecretString::from("fresh".to_string())),
        );
        assert_eq!(params["pass"].expose_secret(), "fresh");
    }

    /// Empty options with no password produce an empty parameter map rather than
    /// injected defaults.
    #[test]
    fn empty_options_produce_no_params() {
        let params = build_pool_params(crate::source::tests::engine(), &BTreeMap::new(), None);
        assert!(params.is_empty());
    }
}
