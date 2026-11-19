//! `beacon-arrow-netcdf` bridges NetCDF files and Beacon ND Arrow arrays.
//!
//! The crate provides:
//! - Reading NetCDF variables and attributes into lazy ND Arrow arrays.
//! - Writing Arrow record batches into NetCDF files.
//! - Decoder/encoder building blocks for extending conversion behaviour.
//!
//! # Reading and writing
//!
//! [`oxcdf_reader`] reads through [`oxcdf`], a pure-Rust reader. It holds no
//! global lock and reads through `object_store`, so scans run in parallel and
//! S3, GCS and Azure need no local copy. [`writer`] and the N-dimensional sink
//! write through [`oxcdf`].
//!
//! # Key modules
//! - [`oxcdf_reader`]: high-level NetCDF -> Arrow reader API.
//! - [`writer`]: high-level Arrow -> NetCDF writer API.
//! - [`encoders`]: pluggable conversion components for the writer.
//! - [`cf_time`]: CF time conversion.
//! - [`dimensions`]: unifies the dimensions netCDF invents for a plain HDF5
//!   file, so two groups of one file broadcast against each other.

/// Encoder implementations for writing Arrow values to NetCDF variables.
pub mod encoders;
/// High-level NetCDF writer.
pub mod writer;
/// Folds `variable.attribute` columns into NetCDF attributes on write.
pub mod attributes;
/// Re-export of the pure-Rust `oxcdf` reader used by [`oxcdf_reader`].
pub use oxcdf;
/// CF time conversion.
pub mod cf_time;
#[cfg(test)]
mod cf_time_fill_tests;
/// DataFusion integration components.
pub mod datafusion;
/// Unifies the dimensions netCDF invents for a file that names none.
pub mod dimensions;
/// Pure-Rust NetCDF reader over `object_store`.
pub mod oxcdf_reader;
