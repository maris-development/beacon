//! `beacon-arrow-hdf5` — HDF5 file support for Beacon.
//!
//! This crate owns the HDF5 *identity*: the `.h5`/`.hdf5` extensions,
//! `STORED AS {H5,HDF5}` and the `read_hdf5` table function. It offers two
//! readers behind that identity, and the flag
//! [`Hdf5Config::use_rust_reader`] picks between them.
//!
//! # netcdf-c, the default
//!
//! A NetCDF-4 file *is* an HDF5 file, and the netCDF-c library's HDF5 dispatch
//! also opens *plain* HDF5 files (their datasets read as variables/arrays). So
//! by default this crate re-implements no reader at all: it delegates to
//! [`beacon_arrow_netcdf`], the way it always has. Leave the flag off and a
//! server behaves exactly as it did before.
//!
//! # The pure-Rust reader
//!
//! Turn the flag on and reads go through [`reader`] instead. That reader holds
//! no process-global lock, so scans of one file run in parallel; it reads byte
//! ranges through [`object_store`], so an object in S3, GCS or Azure needs no
//! local copy; and it covers two layouts the netCDF data model cannot express,
//! a nested group and a compound dataset.
//!
//! Writes always use netcdf-c, whatever the flag says. The Rust reader reads.
//!
//! # The public surface
//!
//! - [`ReadHdf5Func`] — the `read_hdf5` table function.
//! - [`Hdf5FormatFactory`] — a `FileFormat` factory recognizing `.h5`/`.hdf5`
//!   and answering to `STORED AS {h5,hdf5}`.
//! - [`Hdf5Format`] — the `FileFormat` the factory builds on the Rust reader.
//! - [`Hdf5Config`] — the runtime settings, including the reader flag.

mod cache;
mod config;
mod format;
pub mod reader;
mod source;
mod table_function;

pub use cache::{fetch_schema, open_dataset, Hdf5ReaderCache};
pub use config::Hdf5Config;
pub use format::{Hdf5Format, Hdf5FormatFactory};
pub use source::Hdf5Source;
pub use table_function::ReadHdf5Func;

/// The canonical HDF5 format name (`STORED AS HDF5`) and `get_ext`.
pub const HDF5_FORMAT_NAME: &str = "hdf5";

/// The filename extensions — and `STORED AS` spellings — HDF5 files answer to.
pub const HDF5_EXTENSIONS: [&str; 2] = ["h5", "hdf5"];
