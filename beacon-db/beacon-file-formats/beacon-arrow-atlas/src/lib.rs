//! `beacon-arrow-atlas` bridges Atlas array stores and Beacon ND Arrow arrays.
//!
//! Atlas (<https://github.com/robinskil/atlas>) is a directory-based store
//! where a single `atlas.json` registry describes one or more named
//! datasets, each containing its own set of arrays. This crate exposes
//! each atlas dataset as a Beacon `AnyDataset` via lazy `NdArrayD`
//! backends and registers an `AtlasFormat` / `AtlasFormatFactory` so the
//! datasets are queryable through DataFusion.

pub use atlas;

pub mod backend;
pub mod compat;
pub mod datafusion;
pub mod reader;
