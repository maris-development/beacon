//! `beacon.system`: runtime introspection exposed as ordinary SQL tables.
//!
//! What the runtime knows about itself that is not user data lives here — the
//! auth directory and the recorded query metrics — so it is reachable through
//! the one query endpoint rather than through a typed method on
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
//! Reads of this schema — like reads of `information_schema` — are
//! **super-user-only, unconditionally** (see [`is_metadata_schema`] and
//! `statement_plan::authz`). It describes the instance rather than holding user
//! data: `users`/`roles` are the auth directory, `query_metrics` carries the
//! text and plans of queries other users ran, and `information_schema` names
//! every table in every catalog. Regular callers enumerate the catalog through
//! [`Runtime::visible_tables`](crate::runtime::Runtime::visible_tables), which
//! returns only what their roles grant.

mod auth;
mod file_stats;
mod table;

pub(crate) use file_stats::{FileStatisticsFunc, FileStatisticsTable};

/// The name `file_statistics(path)` is registered under.
pub const FILE_STATISTICS_FUNCTION: &str = "file_statistics";

use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};

use crate::query_metrics_store::QUERY_METRICS_TABLE;
use crate::statement_plan::{upgrade_session, SessionCell};

/// The schema name these tables are registered under, in the `beacon` catalog.
pub const SYSTEM_SCHEMA_NAME: &str = "system";

/// The schema DataFusion registers its own catalog views under.
pub const INFORMATION_SCHEMA_NAME: &str = "information_schema";

/// Whether `schema` is one of beacon's metadata schemas — this one or
/// `information_schema`.
///
/// Both are super-user-only, and unconditionally so: like the internal
/// `__beacon_*` tables, a gate that depended on grant enforcement (which is off
/// by default) would leave the auth directory and every table name in the
/// instance readable on a default runtime.
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

/// A fixed, read-only set of runtime-introspection tables.
#[derive(Debug)]
pub struct SystemSchemaProvider {
    /// The snapshot tables built over live runtime state.
    tables: HashMap<String, Arc<dyn TableProvider>>,
    /// Late-filled weak handle to the session, used to resolve `query_metrics`
    /// to the managed table that backs it.
    session: SessionCell,
}

impl SystemSchemaProvider {
    /// Builds the schema over the runtime's live state: the auth context that
    /// owns users and roles, and the session through which `query_metrics`
    /// resolves to its managed table.
    pub fn new(
        session: SessionCell,
        auth: Arc<beacon_auth::AuthContext>,
        file_stats: beacon_file_stats::FileStatsHandle,
    ) -> Self {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        tables.insert(
            "users".to_string(),
            Arc::new(auth::users_table(auth.clone())),
        );
        tables.insert("roles".to_string(), Arc::new(auth::roles_table(auth)));
        // The handle is late-filled, so these read whatever the subsystem has
        // at query time -- including nothing, when it never started.
        tables.insert(
            "file_stats".to_string(),
            Arc::new(file_stats::file_stats_table(file_stats.clone())),
        );
        tables.insert(
            "file_stats_segments".to_string(),
            Arc::new(file_stats::segments_table(file_stats)),
        );
        Self { tables, session }
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.push(QUERY_METRICS.to_string());
        names.sort();
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name) || name == QUERY_METRICS
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if name == QUERY_METRICS {
            return Ok(self.query_metrics_table().await);
        }
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
