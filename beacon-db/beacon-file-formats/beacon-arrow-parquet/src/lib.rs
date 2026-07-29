//! `beacon-arrow-parquet` provides the DataFusion integration for the Parquet
//! file format, wrapping DataFusion's built-in `ParquetFormat` with Beacon's
//! multi-file schema supertyping, dataset discovery and timestamp normalization
//! on write.

/// DataFusion integration for the Parquet file format.
pub mod datafusion;
