//! `beacon-arrow-geoparquet` provides the DataFusion GeoParquet write integration.
//!
//! It exposes a [`datafusion::GeoParquetFormat`] / [`datafusion::GeoParquetFormatFactory`]
//! that map longitude/latitude columns into a geometry column and write GeoParquet output.
//! The read path is not yet implemented.

/// DataFusion integration for GeoParquet output.
pub mod datafusion;
