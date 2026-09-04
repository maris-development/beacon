//! Listing the datasets store.
//!
//! Two table functions over one provider. `list_datasets` globs and descends the
//! whole tree; `browse_datasets` reads one directory level. They differ only in
//! how deep they go, so the provider is shared and each function is a thin
//! reader of its own arguments.
//!
//! The difference is not a constant factor. One level is a single delimiter
//! request; a recursive walk of a 2 853 217-object bucket took 79.9 s where the
//! delimiter took 14 ms. A folder view wants the second, and used to pay for the
//! first.

pub mod browse_datasets;
pub mod exec;
pub mod list_datasets;
pub mod provider;

pub use browse_datasets::BrowseDatasetsFunc;
pub use exec::DatasetsExec;
pub use list_datasets::{list_datasets, ListDatasetsFunc};
pub use provider::{list_datasets_schema, DatasetsTable, Listing, Row};

pub(crate) mod args;
