//! `beacon-arrow-netcdf` bridges NetCDF files and Beacon ND Arrow arrays.
//!
//! The crate provides:
//! - Reading NetCDF variables and attributes into lazy ND Arrow arrays.
//! - Writing Arrow record batches into NetCDF files.
//! - Decoder/encoder building blocks for extending conversion behaviour.
//!
//! # Reading and writing
//!
//! [`nd`] reads NetCDF through the ND framework (`beacon-nd-dataset`,
//! `beacon-nd-scan`). The NetCDF scan uses it. [`oxcdf_reader`] stays for the
//! `beacon-arrow-hdf5` crate, which uses its file open and its variable
//! conversion. Both read through [`oxcdf`], a pure-Rust reader. It holds no
//! global lock and reads through `object_store`, so scans run in parallel and
//! S3, GCS and Azure need no local copy. [`writer`] and the N-dimensional sink
//! write through [`oxcdf`].
//!
//! # Key modules
//! - [`nd`]: NetCDF on the ND framework, with the CF decode.
//! - [`oxcdf_reader`]: the older NetCDF -> ND array reader, for HDF5.
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
/// DataFusion integration components.
pub mod datafusion;
/// Unifies the dimensions netCDF invents for a file that names none.
pub mod dimensions;
/// Pure-Rust NetCDF reader over `object_store`.
pub mod oxcdf_reader;
/// NetCDF on the ND framework (`beacon-nd-dataset`, `beacon-nd-scan`).
pub mod nd;
