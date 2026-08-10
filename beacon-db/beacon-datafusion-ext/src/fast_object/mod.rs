//! Beacon's listing table and the scan under it.
//!
//! [`FastObjectTable`] is a `TableProvider` in its own right — there is no
//! `ListingTable` inside it. It lists a store once and hands the result to
//! [`FastObjectDataSource`], whose partitions share one queue of the files that
//! survive pruning.
//!
//! # Why not `ListingTable`
//!
//! `ListingTable` turns its listing into one `PartitionedFile` per file —
//! ~280 bytes plus a path, fixed at plan time, and there is no way to give it a
//! list or to make it lazy. At three million files that is over a gigabyte, per
//! plan, per concurrent query. This keeps the store's own [`ObjectMeta`]s
//! instead and builds a `PartitionedFile` only at the moment a file is opened.
//!
//! [`ObjectMeta`]: object_store::ObjectMeta
//!
//! # One queue, not a slice per partition
//!
//! Partitions used to own a fixed range of the listing, cut at plan time. A
//! range cannot answer skew the plan cannot see: file cost is not known before
//! the read, so a partition that draws slow files, cold objects, or files a
//! predicate keeps stalls while its peers finish and idle.
//!
//! Every partition now pops from one shared queue. A fast partition takes more.
//! No partition can strand work another could do, and nothing has to be guessed
//! at plan time.
//!
//! # Pruning is the first step of the pipeline
//!
//! Pruning used to be a plan-time phase: name every candidate, read the
//! segments its predicate columns live in, and hand the survivors to the plan.
//! That blocks the planner on reads, serially, before the query starts.
//!
//! It now runs at the head of the *pipeline* instead. The first partition to
//! poll prunes the whole listing behind a `OnceCell`, in parallel batches, and
//! fills the queue with what survives. Every other partition awaits the same
//! cell. The planner still reads nothing.
//!
//! The cost is the first row: it arrives after the whole listing is decided,
//! where it used to arrive after one chunk. The queue is filled batch by batch
//! as each one resolves, so releasing the cell early is a contained change if
//! that ever matters. It is not done here, because a consumer that can find the
//! queue empty has to park, and parking is what this design has none of.
//!
//! The visible consequence is unchanged: `EXPLAIN` cannot say how many files
//! were pruned, because nothing is pruned until the scan runs. `EXPLAIN
//! ANALYZE` reports it from the counters the shared prune increments.
//!
//! # Row order
//!
//! A shared queue decides which partition reads which file by scheduling, so
//! the mapping is not reproducible. A scan carrying a limit therefore does not
//! share: each partition reads one contiguous slice of the survivors, which
//! keeps `read_parquet(...) LIMIT 5` returning the same rows on every run. See
//! [`crate::ordered_union`], which is what makes that guarantee visible.
//!
//! # Directory-oriented formats
//!
//! Zarr and Atlas call a store a directory, but they never open one: their
//! readers take the marker object at its root — `zarr.json`, `atlas.json` — and
//! resolve the store from there. They need no plan of their own, only that one
//! object, so the listing is reduced to the outermost marker per store and the
//! scan hands it over like any other file. Everything else a store contains,
//! including its arrays' own markers and every chunk, would be rejected by the
//! reader.
//!
//! # The one thing `FileScanConfig` is still needed for
//!
//! `FileSource::create_file_opener(&self, store, base_config: &FileScanConfig,
//! partition)` is DataFusion's trait signature, implemented by every format —
//! DataFusion's own and Beacon's ten. Beacon's read `projected_schema()` from
//! it, Parquet reads `limit`, `preserve_order` and the expression adapter; none
//! reads the file list. There is no other API in this version that turns a
//! format into a `FileOpener`, so an empty one is built at `open()` purely as
//! that call's argument. It is a parameter block, not state.
//!
//! # Pushdown
//!
//! Projections and filters are delegated straight to the `FileSource`, which
//! owns them, so a narrow `SELECT` and a `WHERE` still reach the file reader
//! and still drive Parquet's row-group and page pruning inside each file.
//!
//! # What `EXPLAIN` shows
//!
//! `FastObjectScan: files=N, partitions=K[, split=S][, prune=stream]`. `split`
//! appears only when a large file was divided across partitions, which is the
//! one case where work items outnumber files.

pub mod data_source;
pub mod plan;
mod stream;
pub mod table;

pub use data_source::{FastObjectDataSource, projected_schema_of};
pub use plan::{StreamPruning, Work};
pub use table::FastObjectTable;
