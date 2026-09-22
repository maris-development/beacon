//! `list_datasets`: a table over a streaming walk of the datasets store.

pub mod classify;
pub mod exec;
pub mod list_datasets;
pub mod provider;

pub use exec::DatasetsExec;
pub use list_datasets::{ListDatasetsFunc, list_datasets};
pub use provider::{DatasetsTable, list_datasets_schema};
