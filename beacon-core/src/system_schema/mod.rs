//! `beacon.system`: runtime introspection exposed as ordinary SQL tables.
//!
//! Everything the runtime knows about itself that is not user data lives here —
//! the registered functions, the recorded query metrics — so it is reachable
//! through the one query endpoint rather than through a typed method on
//! [`Runtime`](crate::runtime::Runtime) and a bespoke HTTP route per item.
//!
//! The tables are in-memory and read-only: each is a fixed schema plus a closure
//! that snapshots the runtime's state at scan time (see [`table::SystemTable`]).
//! Making one persistent later is a change of snapshot source, not of the SQL
//! surface consumers see.
//!
//! Unlike `information_schema`, this schema is **not** exempt from grant checks
//! (see `statement_plan::authz`), because `query_metrics` exposes the full text
//! and plans of queries other users ran.

mod functions;
mod query_metrics;
mod table;

use std::{any::Any, collections::HashMap, sync::Arc};

use beacon_functions::function_doc::FunctionDoc;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};

pub use query_metrics::QueryMetricsMap;

use crate::statement_plan::SessionCell;

/// The schema name these tables are registered under, in the `beacon` catalog.
pub const SYSTEM_SCHEMA_NAME: &str = "system";

/// A fixed, read-only set of runtime-introspection tables.
#[derive(Debug)]
pub struct SystemSchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl SystemSchemaProvider {
    /// Builds the schema over the runtime's live state.
    ///
    /// `session` is the planner's late-filled weak handle rather than an
    /// `Arc<SessionContext>`: the session owns the catalog that owns this
    /// provider, so holding a strong reference back would leak the whole runtime.
    pub fn new(
        session: SessionCell,
        table_function_docs: Vec<FunctionDoc>,
        query_metrics: QueryMetricsMap,
    ) -> Self {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        tables.insert(
            "functions".to_string(),
            Arc::new(functions::functions_table(session)),
        );
        tables.insert(
            "table_functions".to_string(),
            Arc::new(functions::table_functions_table(table_function_docs)),
        );
        tables.insert(
            "query_metrics".to_string(),
            Arc::new(query_metrics::query_metrics_table(query_metrics)),
        );
        Self { tables }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    /// The table set is fixed at construction; there is nothing to register.
    fn register_table(
        &self,
        name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "cannot create table `{name}`: `{SYSTEM_SCHEMA_NAME}` is a read-only system schema"
        )))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "cannot drop table `{name}`: `{SYSTEM_SCHEMA_NAME}` is a read-only system schema"
        )))
    }
}
