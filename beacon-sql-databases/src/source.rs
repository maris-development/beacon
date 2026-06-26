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

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        self.inner.logical_optimizer()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        self.inner.ast_analyzer()
    }

    fn sql_query_rewriter(&self) -> Option<SqlQueryRewriter> {
        self.inner.sql_query_rewriter()
    }
}
