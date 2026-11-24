//! The pure-Rust NetCDF read path, built on [`oxcdf`].
//!
//! [`oxcdf`] parses the HDF5 container in Rust. It holds no lock, and it reads
//! byte ranges through [`object_store`]. Scans run in parallel, and S3, GCS and
//! Azure need no local copy.
//!
//! # Limits
//!
//! This module reads files. It does not write them. The
//! [`writer`](crate::writer) and the DataFusion sinks write through [`oxcdf`].
//!
//! # Layout
//!
//! * [`backend`] holds the lazy array backends that read variable data.
//! * [`compat`] turns one variable or attribute into an ND array.
//! * [`reader`] opens a file.

/// Lazy array backends that read variable data through [`oxcdf`].
pub mod backend;
/// Conversion from [`oxcdf`] variables and attributes to ND arrays.
pub mod compat;
/// Opens a NetCDF object through `object_store`.
pub mod reader;

pub use reader::open_file;
