pub mod api;
pub mod parser;
pub mod query_result;
pub mod runtime;
mod statement_handlers;
pub mod sys;

/// Re-export of the authentication identity returned by [`runtime::Runtime::authenticate`].
pub use beacon_auth::AuthIdentity;
