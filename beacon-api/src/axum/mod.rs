//! Axum-based HTTP transport for the Beacon API.

mod admin;
mod auth;
mod client;
mod router;

pub(crate) use router::setup_router;