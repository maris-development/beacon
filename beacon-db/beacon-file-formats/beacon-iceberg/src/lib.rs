//! Apache Iceberg integration for Beacon.
//!
//! An Iceberg table is a directory holding a `metadata` directory (metadata
//! files, manifest lists and manifests) plus data files — not a file glob — so
//! it cannot use Beacon's listing-table machinery. Instead this crate builds an
//! Iceberg `TableProvider` via the `iceberg` / `iceberg-datafusion` crates from
//! the Apache iceberg-rust project, reading every byte through Beacon's datasets
//! store, so local-FS and S3 both work transparently.
//!
//! Two surfaces are exposed:
//! - [`ReadIcebergFunc`], the `read_iceberg(...)` table function for ad-hoc queries.
//! - [`IcebergTableDefinition`], the persisted `CREATE EXTERNAL TABLE ... STORED AS
//!   ICEBERG` definition.
//!
//! # Read only
//!
//! Beacon reads an Iceberg table another system writes. There is no write, no
//! `MERGE` and no snapshot expiry: [`crate::storage::BeaconStorage`] refuses
//! every mutating operation, so a write cannot half-apply. Beacon's own managed
//! tables are Lance (see `beacon-lance`).
//!
//! # No catalog, yet
//!
//! A table is named by its location: the directory, on disk or in S3. The
//! current metadata file is found from `metadata/version-hint.text`, or by
//! taking the highest-versioned `*.metadata.json` in the metadata directory.
//!
//! A REST catalog and a Glue catalog are the next step, and they replace exactly
//! one function: [`provider::resolve_metadata_location`], which turns a table
//! into the metadata file to read. A catalog would answer that from its own
//! store and hand back the same path; everything downstream — the storage
//! bridge, the provider, the definition — is unchanged. That is the seam.

pub mod definition;
pub mod provider;
pub mod storage;
pub mod table_function;
pub mod wrapper;

pub use definition::IcebergTableDefinition;
pub use provider::{open_iceberg_table, resolve_metadata_location, OpenedTable};
pub use storage::BeaconStorage;
pub use table_function::ReadIcebergFunc;
pub use wrapper::{iceberg_table_definition, BeaconIcebergTable};
