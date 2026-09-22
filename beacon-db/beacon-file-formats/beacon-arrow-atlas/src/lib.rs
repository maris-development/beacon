//! `beacon-arrow-atlas` reads Atlas collections through Beacon's shared
//! `beacon-nd-array` engine.
//!
//! # The format
//!
//! An Atlas collection is one immutable `data.atlas` file, with an optional
//! `deleted.mask` beside it. One segment holds one variable across every
//! dataset. A footer records dataset names, byte ranges, and declared
//! arrays and attributes, so listing datasets costs no I/O. Array layout,
//! statistics and attribute values come from the variable's own segment,
//! read once per variable; array data then arrives block by block.
//!
//! # One module per stage of a read
//!
//! | Module | Stage |
//! |---|---|
//! | [`discover`] | find the collections in a listing, deal them to partitions |
//! | [`open`] | open a collection, through the reader cache |
//! | [`schema`] | the column-name and type mapping, and a collection's Arrow schema |
//! | [`view`] | resolve the scan's columns against one open collection |
//! | [`prune`] | drop the datasets a predicate cannot match |
//! | [`dataset`] | the lazy arrays one dataset reads through |
//! | [`scan`] | what a scan reads, the shared queue per collection, and the batches |
//! | [`format`](mod@format), [`source`] | the DataFusion traits over all of the above |
//!
//! # One collection is one unit of work
//!
//! Each partition shares a collection's queue: the first prunes and queues
//! its datasets, and parallelism is bounded by the dataset count. See [`source`].
//!
//! # Columns
//!
//! One column per array, under its own name. A per-array attribute becomes
//! `{array}.{attr}`; a dataset-level attribute becomes `.{attr}`.
//!
//! # A scan names its columns
//!
//! A dataset's grid follows the dimensions of the columns it reads. A scan of
//! no column is refused, so `COUNT(*)` fails and `COUNT(column)` passes. A
//! dataset whose columns read sit on more than one grid is refused too, so
//! `SELECT *` fails there unless `read_atlas(paths, dimensions)` names the grid.
//!
//! # Cancellation
//!
//! The scan reads through the query's cancellation token, see
//! [`beacon_datafusion_ext::cancel`]. The pruning pivot runs on a blocking
//! thread Tokio cannot abort, so it watches a child token instead.
//!
//! # What is not read
//!
//! Bool, List and FixedSizeList arrays, and list-valued attributes, are
//! dropped with a debug log rather than failing the scan. A timestamp
//! attribute never arises: atlas stores none.
//!
//! # No CF decoding
//!
//! `atlas create` applies `scale_factor`, `add_offset` and CF time units
//! before the write, so an atlas array reads exactly as stored, unlike
//! netCDF and Zarr.

pub use atlas;

pub mod dataset;
pub mod discover;
pub(crate) mod error;
pub mod format;
pub mod metrics;
pub mod open;
pub mod options;
pub mod prune;
pub mod scan;
pub mod schema;
pub mod source;
pub mod table_function;
pub mod view;

pub use format::{ATLAS_FORMAT, AtlasFormat, AtlasFormatFactory, nd_scan_plan};
pub use options::AtlasOptions;
pub use source::AtlasSource;
pub use table_function::ReadAtlasFunc;

#[cfg(test)]
pub(crate) mod test_support;
