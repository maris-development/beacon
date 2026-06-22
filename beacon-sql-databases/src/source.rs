//! Federated-source wrapper that carries the table's definition.
//!
//! `datafusion-federation` only federates a table scan when the registered
//! provider downcasts to [`FederatedTableProviderAdaptor`] and its `source`
//! yields a [`FederatedTableSource`] (see `get_table_source` in the federation
//! optimizer). Wrapping the adaptor in another `TableProvider` would defeat that
//! detection and silently disable pushdown.
//!
//! So, exactly like the remote-Beacon table, we keep the registered provider a
//! bare `FederatedTableProviderAdaptor` and instead wrap its inner
//! `FederatedTableSource` in this newtype, which delegates every
//! [`TableSource`]/[`FederatedTableSource`] method to the original source while
//! carrying the [`SqlDatabaseTableDefinition`]. The catalog recovers the
//! definition from a live provider via [`crate::sql_database_table_definition`].

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Constraints;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{
    Expr, LogicalPlan, TableProviderFilterPushDown, TableSource, TableType,
};
use datafusion_federation::{FederatedTableSource, FederationProvider};

use crate::definition::SqlDatabaseTableDefinition;

/// A [`FederatedTableSource`] that delegates to the engine's original source
/// while carrying the [`SqlDatabaseTableDefinition`] it was built from.
pub struct BeaconSqlFederatedSource {
    inner: Arc<dyn FederatedTableSource>,
    definition: SqlDatabaseTableDefinition,
}

impl BeaconSqlFederatedSource {
    pub fn new(
        inner: Arc<dyn FederatedTableSource>,
        definition: SqlDatabaseTableDefinition,
    ) -> Self {
        Self { inner, definition }
    }

    /// The persisted definition this table was built from.
    pub fn definition(&self) -> &SqlDatabaseTableDefinition {
        &self.definition
    }
}

impl std::fmt::Debug for BeaconSqlFederatedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeaconSqlFederatedSource")
            .field("table", &self.definition.name)
            .field("engine", &self.definition.engine)
            .finish()
    }
}

impl TableSource for BeaconSqlFederatedSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
}

impl FederatedTableSource for BeaconSqlFederatedSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        self.inner.federation_provider()
    }
}
