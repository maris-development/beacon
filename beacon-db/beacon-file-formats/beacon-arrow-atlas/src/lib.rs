//! `beacon-arrow-atlas` bridges Atlas array stores into Beacon's shared
//! `beacon-nd-array` engine.
//!
//! Atlas (<https://github.com/maris-development/atlas>) is a directory-based
//! store where a single metadata file (`atlas.json` / `atlas.msgpack`, with
//! optional `.zst` / `.lz4` suffix) describes one or more named datasets, each
//! a collection of N-dimensional arrays plus per-dataset and per-array
//! attributes. As of atlas 0.14 the store is opened directly over any
//! [`object_store`] backend (local filesystem, S3, GCS, Azure) — no path
//! translation or native-filesystem root is required.
//!
//! This crate mirrors `beacon-arrow-zarr`: the DataFusion
//! integration ([`datafusion`]) discovers atlas metadata markers, opens each
//! store as an atlas *collection* straight from the query's object store, and
//! exposes every dataset as a Beacon
//! [`AnyDataset`](beacon_nd_array::dataset::AnyDataset) via lazy
//! [`NdArrayD`](beacon_nd_array::NdArrayD) backends ([`backend`]). Each atlas
//! array becomes a column; dataset-level attributes become rank-0 columns and
//! per-array attributes become `{array}.{attr}` rank-0 columns.

pub use atlas;

pub mod backend;
pub mod compat;
pub mod datafusion;
pub mod reader;
pub mod util;
