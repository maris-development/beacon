//! `beacon-arrow-csv` provides the DataFusion integration for the CSV file
//! format, wrapping DataFusion's built-in `CsvFormat` with Beacon's multi-file
//! schema supertyping and dataset discovery.

/// DataFusion integration for the CSV file format.
pub mod datafusion;
