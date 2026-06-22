//! External SQL-database table providers for Beacon (PostgreSQL, MySQL).
//!
//! Registers an existing remote database table as a Beacon table so it can be
//! queried and joined alongside Parquet, Delta, Iceberg, and remote-Beacon
//! tables. Built on `datafusion-table-providers`, which pushes filters,
//! projections, and aggregations down to the source database via
//! `datafusion-federation`.
//!
//! The shape mirrors the federated remote-Beacon table
//! (`beacon_datafusion_ext::remote`): a serializable [`SqlEngine`]-tagged
//! definition builds a DataFusion provider, which is wrapped
//! ([`BeaconSqlDatabaseTable`]) so the catalog can recover the definition for
//! persistence.
//!
//! Each engine is gated behind a cargo feature; [`SqlEngine`] is the single
//! per-engine dispatch point, so adding an engine is an additive change
//! (a variant + feature + `STORED AS` keyword).

mod definition;
mod options;
mod secret;
mod wrapper;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;

pub use definition::{unresolved_schema, SqlDatabaseTableDefinition};
pub use secret::EncryptedSecret;
pub use wrapper::BeaconSqlDatabaseTable;

/// The external database engines Beacon can connect to. Each variant is gated
/// by a cargo feature; this enum is the single per-engine dispatch point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlEngine {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
}

impl SqlEngine {
    /// Map a `STORED AS <keyword>` token to an engine, if enabled. Case-insensitive.
    pub fn from_stored_as(keyword: &str) -> Option<Self> {
        match () {
            #[cfg(feature = "postgres")]
            _ if keyword.eq_ignore_ascii_case("POSTGRES")
                || keyword.eq_ignore_ascii_case("POSTGRESQL") =>
            {
                Some(Self::Postgres)
            }
            #[cfg(feature = "mysql")]
            _ if keyword.eq_ignore_ascii_case("MYSQL") => Some(Self::MySql),
            _ => None,
        }
    }

    /// Human-readable engine name (used in error messages and docs).
    pub fn as_str(self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => "postgres",
            #[cfg(feature = "mysql")]
            Self::MySql => "mysql",
        }
    }

    /// The connection-pool key this engine uses for the TCP port. Postgres uses
    /// `port`; MySQL uses `tcp_port`.
    fn port_key(self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => "port",
            #[cfg(feature = "mysql")]
            Self::MySql => "tcp_port",
        }
    }

    /// Build a federated `TableProvider` for `table_ref` by constructing the
    /// engine's connection pool from `params` and resolving the table.
    async fn build_table_provider(
        self,
        params: HashMap<String, SecretString>,
        table_ref: TableReference,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => build_postgres_provider(params, table_ref).await,
            #[cfg(feature = "mysql")]
            Self::MySql => build_mysql_provider(params, table_ref).await,
        }
    }
}

#[cfg(feature = "postgres")]
async fn build_postgres_provider(
    params: HashMap<String, SecretString>,
    table_ref: TableReference,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    use datafusion_table_providers::postgres::PostgresTableFactory;
    use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

    let pool = PostgresConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!("PostgreSQL connection failed: {e}"))?;
    let factory = PostgresTableFactory::new(Arc::new(pool));
    factory
        .table_provider(table_ref)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve PostgreSQL table: {e}"))
}

#[cfg(feature = "mysql")]
async fn build_mysql_provider(
    params: HashMap<String, SecretString>,
    table_ref: TableReference,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    use datafusion_table_providers::mysql::MySQLTableFactory;
    use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;

    let pool = MySQLConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!("MySQL connection failed: {e}"))?;
    let factory = MySQLTableFactory::new(Arc::new(pool));
    factory
        .table_provider(table_ref)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve MySQL table: {e}"))
}
