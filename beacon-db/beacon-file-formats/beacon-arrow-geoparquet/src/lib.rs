//! `beacon-arrow-geoparquet` provides the DataFusion GeoParquet integration.
//!
//! It exposes a [`datafusion::GeoParquetFormat`] / [`datafusion::GeoParquetFormatFactory`]
//! supporting both directions:
//!
//! * **Write** — maps longitude/latitude columns into a geometry column and
//!   writes GeoParquet output.
//! * **Read** — infers the GeoArrow schema and scans GeoParquet files, decoding
//!   geometry columns described in the file's `geo` metadata to their native
//!   GeoArrow representation.

/// DataFusion integration for reading and writing GeoParquet.
pub mod datafusion;
