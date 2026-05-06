//! `beacon-arrow-tiff` bridges TIFF/GeoTIFF files and Beacon ND Arrow arrays.
//!
//! This crate is currently in active implementation.
//! Public APIs are intentionally small and may evolve as decoding support grows.

/// TIFF array backend implementations.
pub mod backend;
/// TIFF compatibility helpers and type mapping.
pub mod compat;
/// DataFusion integration for TIFF file scanning.
pub mod datafusion;
/// TIFF and GeoTIFF metadata extraction helpers.
pub mod metadata;
/// High-level TIFF reader entry points.
pub mod reader;
