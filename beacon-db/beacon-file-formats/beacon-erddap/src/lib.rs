//! ERDDAP datasets as Beacon external tables.
//!
//! `CREATE EXTERNAL TABLE t STORED AS ERDDAP LOCATION 'https://host/erddap/tabledap/<id>'`
//! reads a tabledap or griddap dataset over HTTP. Projection, filters and limits go
//! into the ERDDAP request. ERDDAP is not a file format: no `read_*` function and no
//! `COPY` target exist for it.

pub mod location;
pub mod options;

pub use location::{ErddapLocation, Protocol};
pub use options::ErddapOptions;
