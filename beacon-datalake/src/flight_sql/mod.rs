//! Arrow Flight SQL transport for Beacon.
//!
//! The implementation is split into focused submodules for authentication,
//! metadata generation, statement handle storage, protocol helpers, and the
//! service implementation itself.

#![allow(clippy::result_large_err)]

mod auth;
mod metadata;
mod service;
mod storage;
mod util;

#[cfg(test)]
mod tests;

pub(crate) use service::serve;
