//! A [`SQLTable`] wrapper that carries the table's definition.
//!
//! `datafusion-federation` federates a scan only when the registered provider is
//! a bare [`FederatedTableProviderAdaptor`] whose `source` is the concrete
//! [`SQLTableSource`]. Its `RewriteTableScanAnalyzer` then rewrites the local
//! catalog name to the table's `table_reference()` before generating the remote
//! SQL. Wrapping the *source* (as a previous version did) makes that
//! `downcast_ref::<SQLTableSource>()` fail, so the rewrite is silently skipped
//! and the remote database receives Beacon's local table name (e.g.
//! `pg_companies`) instead of the configured `LOCATION` (`public.companies`).
//!
//! So, exactly like the remote-Beacon table's `BeaconRemoteSqlTable`, we keep the
//! concrete `SQLTableSource` and instead wrap its inner [`SQLTable`], delegating
//! every method to the engine's original table while carrying the
//! [`SqlDatabaseTableDefinition`]. The catalog recovers the definition from a
//! live provider via [`crate::sql_database_table_definition`].
//!
//! [`FederatedTableProviderAdaptor`]: datafusion_federation::FederatedTableProviderAdaptor
//! [`SQLTableSource`]: datafusion_federation::sql::SQLTableSource

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::remote::geometry_literals_to_calls;
use datafusion::sql::TableReference;
use datafusion_federation::sql::{AstAnalyzer, LogicalOptimizer, SQLTable, SqlQueryRewriter};

use crate::definition::SqlDatabaseTableDefinition;

/// A [`SQLTable`] that delegates to the engine's original table while carrying
/// the [`SqlDatabaseTableDefinition`] it was built from.
pub struct BeaconSqlTable {
    inner: Arc<dyn SQLTable>,
    definition: SqlDatabaseTableDefinition,
}

impl BeaconSqlTable {
    pub fn new(inner: Arc<dyn SQLTable>, definition: SqlDatabaseTableDefinition) -> Self {
        Self { inner, definition }
    }

    /// The persisted definition this table was built from.
    pub fn definition(&self) -> &SqlDatabaseTableDefinition {
        &self.definition
    }
}

impl std::fmt::Debug for BeaconSqlTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeaconSqlTable")
            .field("table", &self.definition.name)
            .field("engine", &self.definition.engine)
            .finish()
    }
}

impl SQLTable for BeaconSqlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_reference(&self) -> TableReference {
        self.inner.table_reference()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// The engine's own rewrite, preceded by the geometry-constant repair where the engine
    /// understands a PostGIS constructor.
    ///
    /// A constant `ST_GeomFromText('...')` folds to an Arrow union or struct at plan time, and
    /// the SQL unparser has no syntax for either, so `plan_to_sql` stops with `Unsupported
    /// scalar`. Rebuilding the call first lets the predicate reach a PostGIS database whole. See
    /// [`SqlEngine::rebuilds_geometry_constants`] for why the other engines keep the error.
    ///
    /// [`SqlEngine::rebuilds_geometry_constants`]: crate::SqlEngine::rebuilds_geometry_constants
    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let mut inner = self.inner.logical_optimizer();
        if !self.definition.engine.rebuilds_geometry_constants() {
            return inner;
        }
        Some(Box::new(move |plan| {
            let plan = geometry_literals_to_calls(plan)?;
            match inner.as_mut() {
                Some(next) => next(plan),
                None => Ok(plan),
            }
        }))
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        self.inner.ast_analyzer()
    }

    fn sql_query_rewriter(&self) -> Option<SqlQueryRewriter> {
        self.inner.sql_query_rewriter()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_federation::sql::RemoteTable;
    use std::collections::BTreeMap;

    /// A definition naming the *local* table `pg_companies` while the remote
    /// table is `public.companies` — the pair that made the original bug visible.
    pub(crate) fn definition(schema: SchemaRef) -> SqlDatabaseTableDefinition {
        SqlDatabaseTableDefinition {
            name: "pg_companies".to_string(),
            engine: engine(),
            remote_table: "public.companies".to_string(),
            schema,
            options: BTreeMap::from([("host".to_string(), "db.internal".to_string())]),
            secret: None,
        }
    }

    /// The engine to test with — either enabled variant works for these
    /// engine-agnostic assertions.
    pub(crate) fn engine() -> crate::SqlEngine {
        #[cfg(feature = "postgres")]
        {
            crate::SqlEngine::Postgres
        }
        #[cfg(all(not(feature = "postgres"), feature = "mysql"))]
        {
            crate::SqlEngine::MySql
        }
    }

    pub(crate) fn remote_table(schema: SchemaRef) -> Arc<dyn SQLTable> {
        Arc::new(RemoteTable::new(
            TableReference::parse_str("public.companies").into(),
            schema,
        ))
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// A definition for one named engine, so the geometry gate can be read per engine.
    fn definition_for(engine: crate::SqlEngine) -> SqlDatabaseTableDefinition {
        let mut definition = definition(test_schema());
        definition.engine = engine;
        definition
    }

    /// Only PostGIS spells both `ST_GeomFromText` and `ST_SetSRID` the way the repair emits them.
    /// MySQL sets an SRID with `ST_SRID`, and SQL Server over ODBC has neither name.
    #[test]
    fn only_postgres_rebuilds_geometry_constants() {
        #[cfg(feature = "postgres")]
        assert!(crate::SqlEngine::Postgres.rebuilds_geometry_constants());
        #[cfg(feature = "mysql")]
        assert!(!crate::SqlEngine::MySql.rebuilds_geometry_constants());
        #[cfg(feature = "odbc")]
        assert!(!crate::SqlEngine::Odbc.rebuilds_geometry_constants());
    }

    /// A Postgres table gains the repair even though the table it wraps supplies no optimizer of
    /// its own, and the repair leaves a plan without a geometry constant alone.
    #[cfg(feature = "postgres")]
    #[test]
    fn a_postgres_table_gains_the_geometry_repair() {
        let schema = test_schema();
        let wrapped = BeaconSqlTable::new(
            remote_table(schema),
            definition_for(crate::SqlEngine::Postgres),
        );

        let mut optimizer = wrapped
            .logical_optimizer()
            .expect("a postgres table repairs geometry constants");
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(true)
            .build()
            .expect("the empty plan should build");
        assert_eq!(
            plan.clone(),
            optimizer(plan).expect("the repair should succeed"),
            "a plan with no geometry constant must pass through untouched"
        );
    }

    /// Every other engine keeps whatever the wrapped table supplies, which here is nothing.
    #[cfg(feature = "mysql")]
    #[test]
    fn a_mysql_table_only_delegates() {
        let schema = test_schema();
        let wrapped =
            BeaconSqlTable::new(remote_table(schema), definition_for(crate::SqlEngine::MySql));
        assert!(
            wrapped.logical_optimizer().is_none(),
            "MySQL must not receive a PostGIS constructor call"
        );
    }

    /// The wrapper must report the *remote* table reference (and schema) of the
    /// table it wraps, not beacon's local name: `RewriteTableScanAnalyzer` uses
    /// `table_reference()` to rewrite the generated SQL, so returning the local
    /// name would send `pg_companies` to the remote database.
    #[test]
    fn delegates_table_reference_and_schema_to_the_inner_table() {
        let schema = test_schema();
        let wrapped = BeaconSqlTable::new(remote_table(schema.clone()), definition(schema.clone()));

        assert_eq!(
            wrapped.table_reference().to_string(),
            TableReference::parse_str("public.companies").to_string(),
            "the remote LOCATION must survive the wrapper"
        );
        assert_ne!(
            wrapped.table_reference().table(),
            "pg_companies",
            "beacon's local table name must never reach the remote database"
        );
        assert_eq!(wrapped.schema(), schema);
    }

    /// The wrapper carries the definition so the catalog can persist `table.json`
    /// from a live provider.
    #[test]
    fn carries_its_definition_and_is_downcastable() {
        let schema = test_schema();
        let table: Arc<dyn SQLTable> =
            Arc::new(BeaconSqlTable::new(remote_table(schema.clone()), definition(schema)));

        let recovered = table
            .as_any()
            .downcast_ref::<BeaconSqlTable>()
            .expect("as_any must expose the concrete wrapper");
        assert_eq!(recovered.definition().name, "pg_companies");
        assert_eq!(recovered.definition().remote_table, "public.companies");
    }

    /// Debug is used in plan/EXPLAIN output; it must name the beacon table and
    /// engine (and never dump the whole definition, which carries the secret).
    #[test]
    fn debug_names_the_table_and_engine() {
        let schema = test_schema();
        let wrapped = BeaconSqlTable::new(remote_table(schema.clone()), definition(schema));
        let debug = format!("{wrapped:?}");
        assert!(debug.contains("pg_companies"), "{debug}");
        assert!(debug.contains("BeaconSqlTable"), "{debug}");
    }
}
