//! The DataFusion scan of the Beacon ND framework.
//!
//! One scan reads each n-dimensional file format of Beacon.
//! This crate cuts each file into chunks, prunes the chunks with the filter
//! of the query, and reads the chunks into `beacon.nd`-encoded record batches.
//!
//! A format crate writes two parts:
//! - An [`NdFileOpener`]. It opens one file and reads only its metadata.
//! - A [`beacon_nd_dataset::NdDataset`]. The opener gives it, and it reads the chunks.
//!
//! The format puts an [`NdSource`] in a `FileScanConfig`. [`file_schema`] and
//! [`file_statistics`] give the schema and the statistics of one file.
//! `beacon_nd_execution::exec::nd_scan_plan` then makes this plan:
//!
//! ```text
//! NdBroadcastExec          flat record batches of the logical schema
//!   NdSourceExec           decodes the encoded batches
//!     DataSourceExec       the NdSource: one morsel for each chunk
//! ```
#![warn(missing_docs)]

mod error;
mod limit;
mod metrics;
mod opener;
mod output;
mod planner;
mod prune;
mod schema;
mod slices;
mod source;
mod statistics;
#[cfg(test)]
mod tests;

pub use error::to_datafusion;
pub use opener::{NdFileOpener, NdScanOptions, open_for_read};
pub use schema::file_schema;
pub use source::{NdSource, encoded_table_schema};
pub use statistics::file_statistics;
