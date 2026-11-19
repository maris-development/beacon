//! `beacon.system`: runtime introspection exposed as ordinary SQL tables.
//!
//! What the runtime knows about itself that is not user data lives here — the
//! comments, the plugins and the recorded query metrics — so it is reachable
//! through the one query endpoint rather than through a typed method on
//! [`Runtime`](crate::runtime::Runtime) and a bespoke HTTP route per item.
//!
//! `query_metrics` is not a table of its own: it is the internal managed table
//! [`QUERY_METRICS_TABLE`] under a public name, resolved on each access, so what
//! this schema shows is exactly what was persisted.
//! Functions are the exception: DataFusion already catalogs them in
//! `information_schema.routines`, which `SHOW FUNCTIONS` reads, so beacon does
//! not keep a second copy.
//!
//! The tables are in-memory and read-only: each is a fixed schema plus a closure
//! that snapshots the runtime's state at scan time (see [`table::SystemTable`]).
//! Making one persistent later is a change of snapshot source, not of the SQL
//! surface consumers see.
//!
//! A host application can add tables of its own with
//! [`Runtime::register_system_table`](crate::runtime::Runtime::register_system_table).
//! The server adds its users and roles this way.
//!
//! This schema and `information_schema` describe the instance rather than hold
//! user data (see [`is_metadata_schema`]): `query_metrics` carries the text and
//! plans of queries other users ran, and `information_schema` names every table
//! in every catalog. A [`PlanValidator`](crate::plan_validator::PlanValidator)
//! can refuse them through
//! [`PlanFacts::metadata_surface`](crate::plan_validator::PlanFacts::metadata_surface).

mod comments;
mod plugins;
mod table;
mod table_sizes;
mod table_statistics;

pub use table::{Snapshot, SystemTable};
pub use table_statistics::TABLE_STATISTICS_FUNCTION;
pub(crate) use table_statistics::{TableStatisticsFunc, TableStatisticsTable};

use std::{collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use parking_lot::RwLock;

use crate::query_metrics_store::QUERY_METRICS_TABLE;
use crate::statement_plan::{upgrade_session, SessionCell};

/// The schema name these tables are registered under, in the `beacon` catalog.
pub const SYSTEM_SCHEMA_NAME: &str = "system";

/// The schema DataFusion registers its own catalog views under.
pub const INFORMATION_SCHEMA_NAME: &str = "information_schema";

/// Whether `schema` is one of beacon's metadata schemas — this one or
/// `information_schema`.
///
/// Matched on the schema name alone, as beacon's other schema gates are, so an
/// attached catalog's `information_schema` (or a schema it happens to call
/// `system`) is covered too — erring toward hiding metadata rather than
/// exposing it.
pub fn is_metadata_schema(schema: &str) -> bool {
    schema.eq_ignore_ascii_case(SYSTEM_SCHEMA_NAME)
        || schema.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAME)
}

/// The name `query_metrics` carries in this schema.
const QUERY_METRICS: &str = "query_metrics";

/// A read-only set of runtime-introspection tables.
#[derive(Debug)]
pub struct SystemSchemaProvider {
    /// The snapshot tables built over live runtime state. The lock lets the host add tables
    /// after the session exists.
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Late-filled weak handle to the session, used to resolve `query_metrics`
    /// to the managed table that backs it.
    session: SessionCell,
}

impl SystemSchemaProvider {
    /// Builds the schema over the runtime's live state: the session through which
    /// `query_metrics` resolves to its managed table and `table_sizes` reads the catalog.
    pub fn new(session: SessionCell, plugins: Arc<crate::plugins::PluginRegistry>) -> Self {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        tables.insert(
            "comments".to_string(),
            Arc::new(comments::comments_table(session.clone())),
        );
        tables.insert(
            "table_sizes".to_string(),
            Arc::new(table_sizes::table_sizes_table(session.clone())),
        );
        tables.insert(
            "plugins".to_string(),
            Arc::new(plugins::plugins_table(plugins)),
        );
        Self {
            tables: RwLock::new(tables),
            session,
        }
    }

    /// Adds the table `name` of the host application.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema already holds a table with this name.
    pub(crate) fn add_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> anyhow::Result<()> {
        let mut tables = self.tables.write();
        if name == QUERY_METRICS || tables.contains_key(name) {
            anyhow::bail!("`{SYSTEM_SCHEMA_NAME}.{name}` already exists");
        }
        tables.insert(name.to_string(), table);
        Ok(())
    }

    /// The managed table behind `query_metrics`, or `None` when it was never
    /// created (a read-only database records nothing).
    async fn query_metrics_table(&self) -> Option<Arc<dyn TableProvider>> {
        upgrade_session(&self.session, "beacon.system.query_metrics")
            .ok()?
            .table_provider(QUERY_METRICS_TABLE)
            .await
            .ok()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.read().keys().cloned().collect();
        names.push(QUERY_METRICS.to_string());
        names.sort();
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().contains_key(name) || name == QUERY_METRICS
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if name == QUERY_METRICS {
            return Ok(self.query_metrics_table().await);
        }
        Ok(self.tables.read().get(name).cloned())
    }

    /// SQL cannot register a table here; only the host application can add one.
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
