//! `beacon-arrow-atlas` reads Atlas collections through Beacon's shared
//! `beacon-nd-array` engine.
//!
//! # The format
//!
//! An Atlas collection (<https://github.com/maris-development/atlas>) is one
//! immutable file, `data.atlas`, with an optional `deleted.mask` beside it:
//!
//! ```text
//! my_collection/
//! ├── data.atlas      ATLS │ temperature │ salinity │ … │ footer │ trailer
//! └── deleted.mask    optional: ordinals of deleted datasets
//! ```
//!
//! **One segment is one variable, not one dataset.** A segment holds one array
//! name across the whole collection, and each dataset's copy sits inside it
//! under the dataset's own name. A footer at the end records every dataset
//! name, every variable's byte range, and the arrays and attribute keys each
//! dataset declares — with their element types, and nothing more.
//!
//! Opening a collection reads that footer, so listing the datasets and asking
//! what one declares cost no further I/O, whatever the dataset count. Three
//! things are *not* in the footer, and each comes from the variable's own
//! segment: an array's layout (shape, chunking, dimension names, fill value),
//! its statistics, and every attribute value. One open answers each of those
//! for the whole collection, so reading `temperature` across a million datasets
//! opens one segment. Array data then arrives block by block, on demand.
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
//! The scan plans one entry per collection and deals every entry to every
//! partition, each in its own rotation. The first partition to open a
//! collection prunes every dataset in one pass over the footer and queues the
//! survivors in a shared queue. Every partition that opens the collection then
//! streams the datasets it pops off that queue. A pruned dataset therefore
//! costs nothing, a dataset is read once, and parallelism is bounded by the
//! dataset count. See [`source`].
//!
//! # Columns
//!
//! One column per array, under the array's own name. A per-array attribute
//! becomes `{array}.{attr}`, and a dataset-level attribute becomes `.{attr}`.
//! That is the convention netCDF and Zarr use, so a query reads the same
//! whichever format holds the data.
//!
//! # A scan names a column
//!
//! A dataset's row count follows the dimensions of the columns it reads. A
//! scan of no column names no dimension set, so a dataset that holds arrays
//! on different grids has no one count. The scan refuses such a query with a
//! planning error. `COUNT(*)` is one; `COUNT(column)` projects a column and
//! reads as any other query.
//!
//! # What is not read
//!
//! - A `Bool` array, and a `List` or `FixedSizeList` array. `array-format`
//!   stores no element of those types, so no such array can exist in a
//!   collection a Rust writer produced. The mapping refuses them all the same.
//! - A list-valued *attribute*. Beacon's ND model has no rank-0 list.
//!
//! A timestamp *attribute* does not arise: atlas stores none, because an
//! attribute would have to go to disk as a plain `i64` and could not come back
//! as a timestamp. An array element type still has its own timestamp.
//!
//! Each is dropped from the dataset with a `debug` log rather than failing the
//! scan. A collection can hold a million datasets, so a `warn` per skip would
//! be a flood.
//!
//! # No CF decoding
//!
//! Atlas has a native timestamp type, and the ingest path (`atlas create`)
//! applies `scale_factor`, `add_offset` and the CF time units *before* the
//! write. An atlas array is therefore read exactly as it is stored, unlike
//! netCDF and Zarr. A collection written by hand with packed integers and a CF
//! `units` attribute reads back as those integers.

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
