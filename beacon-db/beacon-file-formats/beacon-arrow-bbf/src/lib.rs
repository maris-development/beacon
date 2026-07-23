//! `beacon-arrow-bbf` provides the DataFusion integration for the Beacon Binary
//! Format (BBF), wrapping the `beacon-binary-format` reader as a DataFusion
//! [`datafusion::BBFFormat`] / [`datafusion::BBFFormatFactory`] with pruning,
//! stream sharing and metrics.

/// DataFusion integration for reading BBF files.
pub mod datafusion;
