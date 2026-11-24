//! DataFusion integration of NetCDF.
//!
//! * [`factory`]: turns the options of a table into a format.
//! * [`format`]: the [`FileFormat`](datafusion::datasource::file_format::FileFormat):
//!   schema inference, statistics, and the read and write plans.
//! * `open`: opens one file for a read. The schema, the scan and the statistics share it.
//! * `scan`: the morsel scan that reads the chunks of a file.
//! * [`sink`]: the writers of `COPY ... STORED AS NC`.
//! * [`table_function`]: `read_netcdf(...)`.

mod factory;
mod format;
mod open;
pub mod options;
mod scan;
pub mod sink;
pub mod statistics;
pub mod table_function;
#[cfg(test)]
pub(crate) mod test_util;

pub use factory::{NetCDFFormatFactory, NetcdfConfig};
pub use format::NetcdfFormat;
pub use scan::NetcdfSource;
pub use table_function::ReadNetCDFFunc;

pub const NETCDF_EXTENSION: &str = "nc";

/// Every filename extension that a netCDF file uses, canonical first.
///
/// `cdf` is not here: it is also the NASA Common Data Format, which this reader cannot open.
pub const NETCDF_EXTENSIONS: [&str; 4] = [NETCDF_EXTENSION, "nc3", "nc4", "netcdf"];
