mod auth_store;
pub mod crawler;
pub mod embedded;
pub mod extensions;
pub mod file_stats;
pub mod metrics;
pub mod parser;
pub mod query;
pub(crate) mod query_metrics_store;
pub mod query_result;
pub mod runtime;
pub mod runtime_builder;
pub mod schema_persistence;
pub(crate) mod secret_persistence;
pub mod settings;
mod statement_plan;
pub(crate) mod system_schema;

// Re-export the auth types the transports (HTTP, Flight SQL) need, so they depend
// on the auth model through beacon-core rather than directly on beacon-auth.
pub use beacon_auth::{self, AuthContext, AuthIdentity, Credential};

/// How a transport names a table for [`Runtime::table_arrow_schema`](runtime::Runtime::table_arrow_schema):
/// re-exported so naming one does not require a direct DataFusion dependency.
pub use datafusion::sql::TableReference;
