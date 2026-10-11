//! OPeNDAP DAP2 endpoint for Beacon tables.
//!
//! Each table is a DAP2 dataset with one Sequence named `s`. A client subsets it with a
//! DAP2 constraint. Every query runs through
//! [`beacon_core::runtime::Runtime::run_query`] as the caller, so authentication, RBAC and
//! the CPU budget are the same as on `/api/query`.

pub mod compile;
pub mod constraint;
pub mod das;
pub mod dds;
pub mod error;
pub mod headers;
pub mod names;
pub mod types;
pub mod xdr;

mod http;

pub use http::{basic_challenge, router};
