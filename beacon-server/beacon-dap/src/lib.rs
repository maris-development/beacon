//! OPeNDAP DAP2 endpoint for Beacon tables.
//!
//! Each table is a DAP2 dataset with one Sequence named `s`. A client subsets it with a
//! DAP2 constraint. Every query runs through
//! [`beacon_core::runtime::Runtime::run_query`] as the caller, so authentication, RBAC and
//! the CPU budget are the same as on `/api/query`.

pub mod error;
pub mod headers;
pub mod names;
