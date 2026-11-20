pub mod crawler;
pub mod embedded;
pub mod comments;
pub mod metrics;
mod name_rule;
pub mod plan_validator;
pub mod plugins;
pub mod query;
pub mod query_cpu;
pub mod query_row_limit;
pub mod query_executor;
pub(crate) mod query_metrics_store;
pub mod query_result;
pub mod runtime;
pub mod runtime_builder;
pub mod schema_persistence;
pub(crate) mod secret_persistence;
pub mod settings;
mod statement_plan;
mod table_name;
pub(crate) mod system_schema;

/// The building block of a `beacon.system` table, for
/// [`Runtime::register_system_table`](runtime::Runtime::register_system_table).
pub use system_schema::{Snapshot, SystemTable};

/// How a transport names a table for [`Runtime::table_arrow_schema`](runtime::Runtime::table_arrow_schema):
/// re-exported so naming one does not require a direct DataFusion dependency.
pub use datafusion::sql::TableReference;
