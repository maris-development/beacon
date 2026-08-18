//! The pure-Rust HDF5 read path, built on [`oxcdf`] and [`oxcdf_hdf5`].
//!
//! This module is the second of the two readers this crate offers. The first
//! one is netcdf-c, reached by delegating to
//! [`beacon_arrow_netcdf`](beacon_arrow_netcdf::datafusion). Both produce an
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset), so a query gets the
//! same schema and the same values from either one.
//!
//! # Why a second reader
//!
//! netcdf-c is not thread safe. Its Rust bindings put one process-global mutex
//! around every call, and that mutex covers the input, the decompression and
//! the type conversion. A scan of many files therefore runs one file at a time.
//! netcdf-c also opens a local path or an `http(s)` URL only, so an object in a
//! store needs a local copy first.
//!
//! This reader parses the HDF5 container in Rust. It holds no lock, and it
//! reads byte ranges through [`object_store`]. Scans run in parallel, and S3,
//! GCS and Azure need no local copy.
//!
//! # Which reader runs
//!
//! This reader is the default. netcdf-c is the fallback: clear the
//! `use_rust_reader` flag on [`Hdf5Config`](crate::Hdf5Config), or the
//! `use_rust_reader` option of one table, to read through it instead.
//!
//! # What this reader adds over the netCDF one
//!
//! `beacon-arrow-netcdf` holds a reader on the same [`oxcdf`] library. It reads
//! the root group only, and it reads the netCDF data model only, because that
//! is what netcdf-c reports. This reader covers the two layouts that model
//! cannot express:
//!
//! * **A nested group.** Every group is walked, depth first. A dataset outside
//!   the root group keeps its path as its name, so `observations/qc/flag` and
//!   `flag` are two different arrays.
//! * **A compound dataset.** Each member becomes its own array, named
//!   `dataset/member`. A member whose type this reader does not model is
//!   skipped, and a compound with no modelled member is reported by name.
//!
//! # Why [`oxcdf`] and not [`oxcdf_hdf5`] alone
//!
//! [`oxcdf_hdf5`] reads the container and knows nothing about conventions. A
//! NetCDF-4 file is an HDF5 file whose axes are named by HDF5 *dimension
//! scales*, and beacon's ND engine broadcasts by dimension name, so a reader
//! that ignores the scales gives `phony_dim_0` where the netcdf-c path gives
//! `time`. The two backends would then disagree about every NetCDF-4 file.
//!
//! [`oxcdf`] resolves the dimension scales and the CF conventions on top of
//! [`oxcdf_hdf5`], and this crate reuses
//! [`beacon_arrow_netcdf::oxcdf_reader::compat`] for the conversion, so a
//! NetCDF-4 file gives identical arrays on both backends by construction.
//! [`oxcdf_hdf5`] is used directly for the datatypes those conventions do not
//! model — see [`compound`]. A plain HDF5 file with no dimension scales still
//! reads: its axes get the phony names, which is the honest answer.
//!
//! # Limits
//!
//! This reader reads. It does not write. Every write path stays on netcdf-c.
//!
//! # Layout
//!
//! * [`compound`] expands a compound dataset into one array per member.
//! * [`open`] opens an object and assembles the dataset.

/// One array per member of a compound dataset.
pub mod compound;
/// High-level reader that opens an object and returns a dataset.
pub mod open;

pub use open::{open_dataset, read_arrays};
