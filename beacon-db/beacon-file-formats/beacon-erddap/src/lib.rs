//! ERDDAP datasets as Beacon external tables.
//!
//! ```sql
//! CREATE EXTERNAL TABLE t STORED AS ERDDAP
//!   LOCATION 'https://host/erddap/tabledap/<datasetID>'
//!   OPTIONS ('request_timeout_secs' '900');
//! ```
//!
//! The table reads a tabledap dataset over HTTP. The columns come from the dataset
//! and are pinned when the table is created. Projection and filters go into the
//! ERDDAP request, and DataFusion applies each filter again to the result.
//!
//! The only option is `request_timeout_secs` (default 600). A griddap URL is
//! rejected for now. ERDDAP is not a file format: no `read_*` function and no
//! `COPY` target exist for it.

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
