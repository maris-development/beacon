//! `beacon-arrow-hdf5` — HDF5 file support for Beacon.
//!
//! A NetCDF-4 file *is* an HDF5 file, and the netCDF-c library's HDF5 dispatch also opens *plain*
//! HDF5 files (their datasets read as variables/arrays). So beacon reads and writes HDF5 through
//! exactly the same machinery it uses for netCDF, and this crate does **not** re-implement a
//! reader. Instead it owns the HDF5 *identity* and delegates the work to
//! [`beacon_arrow_netcdf`]:
//!
//! - [`ReadHdf5Func`] — the `read_hdf5` table function (delegates to the netCDF reader).
//! - [`Hdf5FormatFactory`] — a `FileFormat` factory recognizing `.h5`/`.hdf5` and answering to
//!   `STORED AS {h5,hdf5}` (delegates to the netCDF format).
//!
//! Keeping this in its own crate means HDF5 support lives on its own axis: it can grow a
//! dedicated, non-netCDF reader (e.g. over `hdf5-metno`, for compound/nested layouts the netCDF
//! data model can't express) without disturbing the netCDF crate.

mod format;
mod table_function;

pub use format::Hdf5FormatFactory;
pub use table_function::ReadHdf5Func;

/// The canonical HDF5 format name (`STORED AS HDF5`) and `get_ext`.
pub const HDF5_FORMAT_NAME: &str = "hdf5";

/// The filename extensions — and `STORED AS` spellings — HDF5 files answer to.
pub const HDF5_EXTENSIONS: [&str; 2] = ["h5", "hdf5"];
