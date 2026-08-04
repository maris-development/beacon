//! The Beacon server: an application built on the beacon-db engine.
//!
//! A [`Server`](server::Server) owns the datasets object store and hosts a
//! `beacon_core::Runtime` as the processing unit behind it. The transports —
//! HTTP ([`axum`]) and Arrow Flight SQL ([`flight_sql`]) — are surfaces over that
//! pair.
//!
//! This is a library so the binary and the integration tests under `tests/` are
//! both consumers of the same public API, rather than the tests having to live
//! inside `src/` to reach it.
//!
//! The JSON contract those transports speak lives in [`api`]: the runtime hands
//! back Arrow (`SchemaRef`, `RecordBatch`) and its own domain types, and this
//! crate maps them to the documented wire shapes.

pub mod api;
pub mod auth;
pub mod axum;
pub mod server;
pub mod flight_sql;

pub use server::Server;
