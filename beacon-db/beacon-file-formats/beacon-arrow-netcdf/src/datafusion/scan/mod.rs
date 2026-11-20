//! The NetCDF scan on the DataFusion morsel API.
//!
//! A scan opens one file, cuts it into chunks, takes the range of its slice,
//! drops the chunks a filter excludes, and reads one chunk per morsel.

mod chunks;
mod prune;
mod slices;
mod output;
mod morsel;
