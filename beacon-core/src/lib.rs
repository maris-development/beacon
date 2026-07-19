pub mod api;
mod auth_store;
pub mod crawler;
pub mod extensions;
pub mod metrics;
pub mod parser;
pub mod query;
pub mod query_result;
pub mod runtime;
pub mod runtime_builder;
pub mod schema_persistence;
mod statement_plan;

// Re-export the auth types the transports (HTTP, Flight SQL) need, so they depend
// on the auth model through beacon-core rather than directly on beacon-auth.
pub use beacon_auth::{self, AuthContext, AuthIdentity, Credential};
