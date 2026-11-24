//! The NetCDF scan on the DataFusion morsel API.
//!
//! A scan opens one file, cuts it into chunks, takes the range of its slice,
//! drops the chunks a filter excludes, and reads one chunk per morsel.

mod chunks;
mod morsel;
mod output;
mod prune;
mod slices;
mod source;

pub use source::NetcdfSource;
