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
//! # What this crate does with it
//!
//! [`store`] finds a collection's marker and opens it, through a reader cache.
//! [`reader`] turns one dataset into a Beacon
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) whose columns are lazy
//! [`NdArrayD`](beacon_nd_array::NdArrayD) values backed by [`backend`], and
//! derives the Arrow schema of a whole collection. [`compat`] holds the type
//! and column-name mapping the two share.
//!
//! # One dataset is one unit of work
//!
//! The scan plans one entry per dataset and puts them all in one morsel queue,
//! as the netCDF and Zarr scans do for their files. A partition takes the next
//! dataset when it is free, and each dataset is cut on the chunk grid the
//! writer actually chose, so a dataset stored as one chunk yields one unit and
//! a chunked one yields many. See [`datafusion::source`].
//!
//! # Columns
//!
//! One column per array, under the array's own name. A per-array attribute
//! becomes `{array}.{attr}`, and a dataset-level attribute becomes `.{attr}`.
//! That is the convention netCDF and Zarr use, so a query reads the same
//! whichever format holds the data.
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

pub mod backend;
pub mod compat;
pub mod config;
pub mod datafusion;
pub mod reader;
pub mod store;

pub use config::AtlasConfig;
pub use datafusion::{AtlasFormat, AtlasFormatFactory, AtlasOptions, ReadAtlasFunc};

#[cfg(test)]
pub(crate) mod test_support;
