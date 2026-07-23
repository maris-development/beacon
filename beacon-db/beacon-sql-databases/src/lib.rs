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
//! definition builds a bare `FederatedTableProviderAdaptor` over the concrete
//! `SQLTableSource` (so the federation optimizer still pushes work down and
//! rewrites the remote table name), whose inner `SQLTable` is wrapped
//! ([`BeaconSqlTable`]) so the catalog can recover the definition for
//! persistence.
//!
//! Each engine is gated behind a cargo feature; [`SqlEngine`] is the single
//! per-engine dispatch point, so adding an engine is an additive change
//! (a variant + feature + `STORED AS` keyword).

mod definition;
mod options;
mod secret;
mod source;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use datafusion_federation::sql::SQLTableSource;
use datafusion_federation::FederatedTableProviderAdaptor;
use secrecy::SecretString;

pub use definition::{unresolved_schema, SqlDatabaseTableDefinition};
pub use secret::EncryptedSecret;
pub use source::BeaconSqlTable;

/// Recover a [`SqlDatabaseTableDefinition`] from a registered provider, if it is
/// an external SQL-database table.
///
/// Like the remote-Beacon table, the catalog registers a bare
/// [`FederatedTableProviderAdaptor`] over the concrete `SQLTableSource` (so the
/// federation optimizer recognizes it and rewrites the remote table name); this
/// digs through that source's inner [`BeaconSqlTable`] to recover the definition
/// for persistence.
pub fn sql_database_table_definition(
    provider: &dyn TableProvider,
) -> Option<SqlDatabaseTableDefinition> {
    let adaptor = provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()?;
    let source = adaptor.source.as_any().downcast_ref::<SQLTableSource>()?;
    let table = source.table.as_any().downcast_ref::<BeaconSqlTable>()?;
    Some(table.definition().clone())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::tests::{definition, remote_table};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
    use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
    use datafusion_federation::sql::{SQLExecutor, SQLTable};
    use datafusion_federation::sql::SQLFederationProvider;

    /// A no-op executor: `sql_database_table_definition` only walks the provider
    /// structure, so nothing is ever executed.
    struct StubExecutor;

    #[async_trait::async_trait]
    impl SQLExecutor for StubExecutor {
        fn name(&self) -> &str {
            "stub"
        }
        fn compute_context(&self) -> Option<String> {
            Some("stub".to_string())
        }
        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(DefaultDialect {})
        }
        fn execute(
            &self,
            _query: &str,
            _schema: arrow::datatypes::SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> DataFusionResult<SendableRecordBatchStream> {
            unimplemented!("the stub executor never runs a query")
        }
        async fn table_names(&self) -> DataFusionResult<Vec<String>> {
            Ok(vec![])
        }
        async fn get_table_schema(
            &self,
            _table_name: &str,
        ) -> DataFusionResult<arrow::datatypes::SchemaRef> {
            unimplemented!("the stub executor never resolves a schema")
        }
    }

    fn schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// Reproduce the exact provider shape `build_provider` registers: a bare
    /// `FederatedTableProviderAdaptor` over a concrete `SQLTableSource` whose
    /// inner table is a `BeaconSqlTable`.
    fn federated_provider(inner: Arc<dyn SQLTable>) -> Arc<dyn TableProvider> {
        let federation = Arc::new(SQLFederationProvider::new(Arc::new(StubExecutor)));
        let source = Arc::new(SQLTableSource::new_with_table(federation, inner));
        Arc::new(FederatedTableProviderAdaptor::new(source))
    }

    #[test]
    fn recovers_the_definition_from_a_registered_provider() {
        let schema = schema();
        let inner: Arc<dyn SQLTable> = Arc::new(BeaconSqlTable::new(
            remote_table(schema.clone()),
            definition(schema.clone()),
        ));
        let provider = federated_provider(inner);

        let recovered =
            sql_database_table_definition(provider.as_ref()).expect("definition should be found");
        assert_eq!(recovered.name, "pg_companies");
        assert_eq!(recovered.remote_table, "public.companies");
        assert_eq!(recovered.engine, crate::source::tests::engine());
    }

    /// A federated provider whose inner table is *not* a `BeaconSqlTable` (e.g. a
    /// remote-Beacon table) must not be mistaken for an external SQL table.
    #[test]
    fn ignores_providers_without_a_beacon_sql_table() {
        let provider = federated_provider(remote_table(schema()));
        assert!(sql_database_table_definition(provider.as_ref()).is_none());

        // A completely unrelated provider is likewise ignored.
        let mem = datafusion::datasource::empty::EmptyTable::new(schema());
        assert!(sql_database_table_definition(&mem).is_none());
    }

    #[test]
    fn stored_as_keywords_map_case_insensitively() {
        #[cfg(feature = "postgres")]
        {
            assert_eq!(
                SqlEngine::from_stored_as("postgres"),
                Some(SqlEngine::Postgres)
            );
            assert_eq!(
                SqlEngine::from_stored_as("PostgreSQL"),
                Some(SqlEngine::Postgres)
            );
            assert_eq!(SqlEngine::Postgres.as_str(), "postgres");
        }
        #[cfg(feature = "mysql")]
        {
            assert_eq!(SqlEngine::from_stored_as("MYSQL"), Some(SqlEngine::MySql));
            assert_eq!(SqlEngine::from_stored_as("mysql"), Some(SqlEngine::MySql));
            assert_eq!(SqlEngine::MySql.as_str(), "mysql");
        }
        // Unknown / non-database `STORED AS` keywords fall through to other
        // handlers rather than being claimed by this crate.
        assert_eq!(SqlEngine::from_stored_as("PARQUET"), None);
        assert_eq!(SqlEngine::from_stored_as(""), None);
        assert_eq!(SqlEngine::from_stored_as("postgres_"), None);
    }

    /// The engine tag is part of the persisted `table.json`; its serialized
    /// spelling must stay stable across restarts.
    #[test]
    fn engine_serialization_round_trips() {
        for engine in [
            #[cfg(feature = "postgres")]
            SqlEngine::Postgres,
            #[cfg(feature = "mysql")]
            SqlEngine::MySql,
        ] {
            let json = serde_json::to_string(&engine).unwrap();
            assert_eq!(serde_json::from_str::<SqlEngine>(&json).unwrap(), engine);
        }
        #[cfg(feature = "postgres")]
        assert_eq!(
            serde_json::to_string(&SqlEngine::Postgres).unwrap(),
            "\"postgres\""
        );
        #[cfg(feature = "mysql")]
        assert_eq!(
            serde_json::to_string(&SqlEngine::MySql).unwrap(),
            "\"my_sql\""
        );
    }
}
