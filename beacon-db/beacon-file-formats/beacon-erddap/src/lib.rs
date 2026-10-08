//! ERDDAP datasets as Beacon external tables.
//!
//! `CREATE EXTERNAL TABLE t STORED AS ERDDAP LOCATION 'https://host/erddap/tabledap/<id>'`
//! reads a tabledap dataset over HTTP. Projection and filters go into the ERDDAP
//! request. griddap datasets are not supported yet. ERDDAP is not a file format:
//! no `read_*` function and no `COPY` target exist for it.

pub mod client;
pub mod definition;
pub mod encode;
pub mod exec;
pub mod fixture;
pub mod info;
pub mod location;
pub mod options;
pub mod provider;
pub mod tabledap;

pub use client::{ErddapClient, ErddapError};
pub use definition::ErddapTableDefinition;
pub use info::DatasetInfo;
pub use location::{ErddapLocation, Protocol};
pub use options::ErddapOptions;
pub use provider::ErddapTable;
