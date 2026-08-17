//! `beacon-arrow-hdf5` — HDF5 file support for Beacon.
//!
//! This crate owns the HDF5 *identity*: the `.h5`/`.hdf5` extensions,
//! `STORED AS {H5,HDF5}` and the `read_hdf5` table function. It offers two
//! readers behind that identity, and [`Hdf5Config::backend`] picks between them.
//!
//! # The pure-Rust reader, the default
//!
//! Reads go through [`reader`]. It holds no process-global lock, so scans of one
//! file run in parallel; it reads byte ranges through [`object_store`], so an
//! object in S3, GCS or Azure needs no local copy; and it covers two layouts the
//! netCDF data model cannot express, a nested group and a compound dataset.
//!
//! # netcdf-c, the fallback
//!
//! A NetCDF-4 file *is* an HDF5 file, and the netCDF-c library's HDF5 dispatch
//! also opens *plain* HDF5 files (their datasets read as variables/arrays). Set
//! the backend to `netcdf-c` and this crate re-implements no reader at all: it
//! delegates to [`beacon_arrow_netcdf`], the way it did before the Rust reader
//! existed.
//!
//! Writes always use netcdf-c, whatever the backend says. The Rust reader reads.
//!
//! # The public surface
//!
//! - [`ReadHdf5Func`] — the `read_hdf5` table function.
//! - [`Hdf5FormatFactory`] — a `FileFormat` factory recognizing `.h5`/`.hdf5`
//!   and answering to `STORED AS {h5,hdf5}`.
//! - [`Hdf5Format`] — the `FileFormat` the factory builds on the Rust reader.
//! - [`Hdf5Config`] — the runtime settings, including the reader backend.

mod config;
mod format;
mod open;
pub mod reader;
mod source;
mod table_function;

pub use config::Hdf5Config;
pub use format::{Hdf5Format, Hdf5FormatFactory};
pub use open::{fetch_schema, open_dataset};
pub use source::Hdf5Source;
pub use table_function::ReadHdf5Func;

/// The canonical HDF5 format name (`STORED AS HDF5`) and `get_ext`.
pub const HDF5_FORMAT_NAME: &str = "hdf5";

/// The filename extensions — and `STORED AS` spellings — HDF5 files answer to.
pub const HDF5_EXTENSIONS: [&str; 2] = ["h5", "hdf5"];
