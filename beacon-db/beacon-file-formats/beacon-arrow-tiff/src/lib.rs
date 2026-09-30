//! `beacon-arrow-tiff` bridges TIFF/GeoTIFF files and Beacon ND Arrow arrays.
//!
//! This crate is currently in active implementation.
//! Public APIs are intentionally small and may evolve as decoding support grows.

pub(crate) mod backend;
pub(crate) mod block;
/// DataFusion integration for TIFF file scanning.
pub mod datafusion;
/// High-level TIFF reader entry points.
pub mod reader;
