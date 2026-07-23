//! The beacon data lake: an application built on the beacon-db engine.
//!
//! A [`DataLake`](datalake::DataLake) owns the datasets object store and hosts a
//! `beacon_core::Runtime` as the processing unit behind it. The transports —
//! HTTP ([`axum`]) and Arrow Flight SQL ([`flight_sql`]) — are surfaces over that
//! pair.
//!
//! This is a library so the binary and the integration tests under `tests/` are
//! both consumers of the same public API, rather than the tests having to live
//! inside `src/` to reach it.

pub mod auth;
pub mod axum;
pub mod datalake;
pub mod flight_sql;

pub use datalake::DataLake;
