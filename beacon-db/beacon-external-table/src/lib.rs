//! External tables: `CREATE EXTERNAL TABLE` over a set of files.
//!
//! The crate has one path from a table description to a table provider:
//!
//! | Module | Holds |
//! |---|---|
//! | [`definition`] | [`ExternalTableDefinition`], the form Beacon stores. |
//! | [`builder`] | [`ExternalTableBuilder`], which turns a definition into a table. |
//! | [`spec`] | [`ExternalTableSpec`], the resolved inputs a refresh lists again from. |
//! | [`table`] | [`ExternalTable`], the `TableProvider`. |
//! | [`factory`] | [`ExternalTableFactory`], the `CREATE EXTERNAL TABLE` entry point. |
//!
//! A `CREATE EXTERNAL TABLE` and a stored definition both go through
//! [`ExternalTableBuilder`], so a table that Beacon loads at startup is the same
//! table that the SQL statement created.
//!
//! # Stored definitions
//!
//! [`ExternalTableDefinition`] keeps the serde form and the typetag name
//! `"listing_table"` of older Beacon versions. A `beacon.db` that an older Beacon
//! wrote therefore loads without a migration.
//!
//! # Scan
//!
//! [`ExternalTable`] lists the files of its DataFusion `ListingTable`. An
//! [`ExternalTableStatisticsProvider`](statistics_cache::ExternalTableStatisticsProvider)
//! then drops the files that its statistics rule out. The format plans the
//! scan over the kept files only. The `read_*` table functions prune through
//! `FastObjectTable` and the file statistics store.
//!
//! The scan config gets the `PhysicalExprAdapterFactory` of the table type
//! widening strategy. DataFusion's Parquet source reads it. The other formats
//! adapt each file with the strategy of the session.
//!
//! With `skip_invalid_files_on_scan`, a file that the scan cannot open or read
//! ends at its error, and the scan continues with the next file.

pub mod builder;
pub mod definition;
pub mod factory;
mod scan;
mod skip_errors;
mod skip_errors_source;
pub mod spec;
pub mod statistics_cache;
pub mod table;

pub use builder::ExternalTableBuilder;
pub use definition::{
    ExternalTableDefinition, RefreshPolicy, SchemaSource, bool_table_option,
    parallel_schema_inference_from_options,
    type_widening_from_options,
};
pub use factory::ExternalTableFactory;
pub use spec::ExternalTableSpec;
pub use table::ExternalTable;
