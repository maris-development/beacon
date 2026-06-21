//! External Iceberg tables: reading (and appending to) an Iceberg table that
//! already exists at a user-chosen `datasets://` location, via a per-location
//! [`FileCatalog`](iceberg_file_catalog::FileCatalog).
//!
//! This is the Iceberg counterpart to beacon's Delta external tables: it backs
//! both `CREATE EXTERNAL TABLE ... STORED AS ICEBERG LOCATION '...'` (through
//! [`ExternalIcebergTableDefinition`]) and the [`ReadIcebergFunc`]
//! (`read_iceberg(...)`) table function. Unlike the managed tables in the rest of
//! this crate, beacon does not own these tables — `DROP` only deregisters them.

pub mod definition;
pub mod loader;
pub mod provider;
pub mod table_function;

pub use definition::ExternalIcebergTableDefinition;
pub use loader::load_external_iceberg_table;
pub use provider::ExternalIcebergTable;
pub use table_function::ReadIcebergFunc;
