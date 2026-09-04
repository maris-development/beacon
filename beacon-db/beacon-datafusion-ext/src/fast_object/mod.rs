//! Beacon's listing table: a `ListingTable` that prunes before it scans.
//!
//! [`FastObjectTable`] wraps one and adds two things to it.
//!
//! # Schemas are merged, not required to agree
//!
//! `ListingTable` takes one schema. The files behind a `read_*` need not agree
//! on a column's width, so each URL's schema is inferred and the session's
//! [`ArrowTypeWidening`](crate::type_widening::ArrowTypeWidening) strategy
//! merges them into the one the table reports.
//!
//! Inference goes through [`schema::infer_url_schemas`], which asks the schema
//! cache about each file before opening it. Deriving a schema from every file on
//! every query was 83% of a netCDF query over a hundred thousand files, and the
//! statistics collector already computes those schemas and used to drop them.
//!
//! # Pruning happens inside `scan`
//!
//! `scan` asks the listing table what it would read — `list_files_for_scan`,
//! which lists, collects each file's statistics and groups them — builds the
//! `FileScanConfig`, lets the format turn it into a plan, and then drops the
//! files whose recorded column ranges say they cannot match. The plan handed
//! back scans only what is left, and `EXPLAIN` prints only those files.
//!
//! Pruning runs after the format has planned, not before, because a format
//! decides its own file list. Zarr and Atlas expand a store *directory* into
//! the groups their reader opens and reduce it to the marker at its root:
//! dropping files from the listing first would take a store's analysed root
//! marker and leave its unanalysed children behind, and the format would then
//! read one of those as a store. What is registered, and so what pruning can
//! reason about, is the list the format settled on.
//!
//! Pruning reads the statistics store, so it is work `scan` does rather than
//! work the caller waits on before planning starts. It runs in parallel above
//! 65 536 candidates: see [`prune`].
//!
//! A file is looked up by what the listing said about it, not by its path
//! alone: the size and modification time, or the etag, must still match the
//! record the statistics were written against. A file rewritten after its
//! analysis therefore reads as unknown and is kept. The background pass notices
//! the same change and marks the record stale, but it runs every
//! `BEACON_FILE_STATS_INTERVAL_SECS`, and over a store Beacon never lists it
//! never runs at all. The scan holds the metadata already, so the check costs
//! no request.
//!
//! An entry a format planned itself may carry no such metadata — Zarr states
//! the group keys its reader opens and nothing else — so there is nothing to
//! compare, and the record stands as it always did. For those the pass remains
//! the only check.
//!
//! What it costs is peak memory. `list_files_for_scan` materialises a
//! `PartitionedFile` per listed file — ~280 bytes plus a path — before pruning
//! sees any of them, so a selective query over a very large collection still
//! pays for the whole listing once. Pruning shrinks what the plan carries, not
//! what building it touched.
//!
//! # The format still plans its own scan
//!
//! `create_physical_plan` is what turns the config into a plan, so every format
//! keeps the shape it wants: netCDF and HDF5 stack decode and broadcast nodes
//! over their scan, and Zarr and Atlas expand a store directory into partitions
//! and reduce it to the marker at its root. Nothing here knows about any of
//! that.
//!
//! # What `EXPLAIN` shows
//!
//! The format's own scan node, so its file groups are printed as ever — and
//! pruning has already happened, so what it lists is what will be read.
//! `EXPLAIN ANALYZE` adds `file_stats_files_considered` and
//! `file_stats_files_pruned` under that node.
//!
//! Its statistics still describe every file the format planned, so after a
//! prune they are an overestimate — the same one the plan-rewriting path has
//! always reported.

pub mod prune;
pub mod schema;
pub mod table;

pub use prune::{Pruned, Pruning, prune_file_groups, prune_plan};
pub use schema::infer_url_schemas;
pub use table::FastObjectTable;
