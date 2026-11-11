//! Beacon's listing table for the `read_*` functions.
//!
//! [`FastObjectTable`] wraps a `ListingTable` and adds two things to it.
//!
//! # Schemas are merged, not required to agree
//!
//! `ListingTable` takes one schema. The files behind a `read_*` need not agree
//! on a column's width, so each URL's schema is inferred and the session's
//! [`ArrowTypeWidening`](crate::type_widening::ArrowTypeWidening) strategy
//! merges them into the one the table reports. Inference goes through
//! [`schema::infer_url_schemas`].
//!
//! # The format still plans its own scan
//!
//! `scan` asks the listing table what it would read — `list_files_for_scan`,
//! which lists the files and groups them — builds the `FileScanConfig`, and
//! lets the format turn it into a plan. `create_physical_plan` is what turns the
//! config into a plan, so every format keeps the shape it wants: netCDF and HDF5
//! stack decode and broadcast nodes over their scan, and Zarr and Atlas expand
//! a store directory into partitions and reduce it to the marker at its root.
//! Nothing here knows about any of that.
//!
//! A `read_*` scan does not prune files with statistics. Only an external
//! table with `use_statistics_pruning` does (crate `beacon-file-statistics`).

pub mod schema;
pub mod table;

pub use schema::infer_url_schemas;
pub use table::FastObjectTable;
