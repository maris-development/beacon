//! Axum-based HTTP transport for the Beacon API.

mod admin;
#[cfg(test)]
mod admin_datasets_http_tests;
mod auth;
#[cfg(test)]
mod auth_http_tests;
mod client;
#[cfg(test)]
mod rbac_http_tests;
mod router;

pub(crate) use router::setup_router;
