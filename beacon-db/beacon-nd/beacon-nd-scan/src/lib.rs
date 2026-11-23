//! The DataFusion scan of the Beacon ND framework.
//!
//! One scan reads each n-dimensional file format of Beacon.
//! A format crate opens a file as an [`beacon_nd_dataset::NdDataset`].
//! This crate cuts the dataset into chunks and reads them into record batches.
#![warn(missing_docs)]

pub(crate) mod error;
pub mod metrics;
pub mod opener;
mod output;
mod planner;
mod prune;
pub mod schema;
mod slices;
pub mod source;
pub mod statistics;
#[cfg(test)]
pub(crate) mod testing;

pub use metrics::ScanMetrics;
pub use opener::{NdFileOpener, NdScanOptions, open_for_read};
pub use schema::file_schema;
pub use source::NdSource;
pub use statistics::file_statistics;
