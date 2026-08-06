//! The pure-Rust NetCDF read path, built on [`oxcdf`].
//!
//! This module is the second of the two readers in this crate. The first one,
//! [`crate::reader`], calls netcdf-c. Both produce the same
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset), so a query gets the
//! same schema and the same values from either one.
//!
//! # Why a second reader
//!
//! netcdf-c is not thread safe. Its Rust bindings put one process-global mutex
//! around every call, and that mutex covers the input, the decompression and
//! the type conversion. A scan of many files therefore runs one file at a time.
//! netcdf-c also opens a local path or an `http(s)` URL only, so a file in an
//! object store needs a local copy first.
//!
//! [`oxcdf`] parses the HDF5 container in Rust. It holds no lock, and it reads
//! byte ranges through [`object_store`]. Scans run in parallel, and S3, GCS and
//! Azure need no local copy.
//!
//! # Which reader runs
//!
//! netcdf-c stays the default. Set the `use_rust_reader` flag on
//! [`NetcdfConfig`](crate::datafusion::NetcdfConfig), or the `use_rust_reader`
//! option of one table, to select this path.
//!
//! # Limits
//!
//! [`oxcdf`] reads files. It does not write them. The
//! [`writer`](crate::writer) and the DataFusion sinks always use netcdf-c.
//!
//! # Layout
//!
//! * [`backend`] holds the lazy array backends that read variable data.
//! * [`compat`] turns one variable or attribute into an ND array.
//! * [`reader`] opens a file and assembles the dataset.

/// Lazy array backends that read variable data through [`oxcdf`].
pub mod backend;
/// Conversion from [`oxcdf`] variables and attributes to ND arrays.
pub mod compat;
/// High-level reader that opens an object and returns a dataset.
pub mod reader;

pub use reader::{open_dataset, read_arrays};
