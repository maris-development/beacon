//! The DataFusion scan of the Beacon ND framework.
//!
//! One scan reads each n-dimensional file format of Beacon.
//! A format crate opens a file as an [`beacon_nd_dataset::NdDataset`].
//! This crate cuts the dataset into chunks and reads them into record batches.
#![warn(missing_docs)]

pub(crate) mod error;
pub mod metrics;
// The scan tasks use the output step; until they land, only the tests call it.
#[allow(dead_code)]
mod output;
// Later tasks call the prune; until then its items have no caller.
#[allow(dead_code)]
mod prune;
// Later tasks use the slices.
#[allow(dead_code)]
mod slices;
#[cfg(test)]
pub(crate) mod testing;

pub use metrics::ScanMetrics;
