pub mod api;
pub mod dataset_files;
pub mod dataset_uploads;
pub mod extensions;
pub mod metrics;
pub mod parser;
pub mod query;
pub mod query_result;
pub mod runtime;
mod statement_plan;
pub mod sys;

// Re-export the auth types the transports (HTTP, Flight SQL) need, so they depend
// on the auth model through beacon-core rather than directly on beacon-auth.
pub use beacon_auth::{self, AuthContext, AuthIdentity, Credential};
