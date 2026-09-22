//! Listing the datasets store.
//!
//! `list_datasets` is a table over a store walk. The walk streams, the rows
//! stream behind it, and nothing runs until the plan executes.

pub mod classify;
pub mod exec;
pub mod list_datasets;
pub mod provider;

pub use exec::DatasetsExec;
pub use list_datasets::{ListDatasetsFunc, list_datasets};
pub use provider::{DatasetsTable, list_datasets_schema};
