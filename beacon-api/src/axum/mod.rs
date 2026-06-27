//! Axum-based HTTP transport for the Beacon API.

mod admin;
mod auth;
#[cfg(test)]
mod auth_http_tests;
mod client;
mod router;

pub(crate) use router::setup_router;