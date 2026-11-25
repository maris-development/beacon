//! Delta Lake integration for Beacon.
//!
//! A Delta table is a directory with a `_delta_log` transaction log plus Parquet
//! data files — not a file glob — so it cannot use Beacon's listing-table
//! machinery. Instead this crate builds a Delta `TableProvider` via the
//! `deltalake` (delta-rs) crate, resolving Beacon's datasets store from the
//! session's object-store registry so local-FS and S3 both work transparently.
//!
//! Two surfaces are exposed:
//! - [`ReadDeltaFunc`], the `read_delta(...)` table function for ad-hoc queries.
//! - [`DeltaTableDefinition`], the persisted `CREATE EXTERNAL TABLE ... STORED AS
//!   DELTA` definition.
//!
//! The provider returned by [`open_delta_provider`] supports reads, time travel,
//! and `INSERT INTO` (append/overwrite) natively.

pub mod definition;
pub mod provider;
pub mod table_function;
pub mod wrapper;

pub use definition::DeltaTableDefinition;
pub use provider::{open_delta_provider, TimeTravel};
pub use table_function::ReadDeltaFunc;
pub use wrapper::BeaconDeltaTable;
