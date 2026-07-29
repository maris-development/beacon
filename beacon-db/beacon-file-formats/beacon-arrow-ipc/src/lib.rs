//! `beacon-arrow-ipc` provides the DataFusion integration for the Arrow IPC
//! (Feather) file format, wrapping DataFusion's built-in `ArrowFormat` with
//! Beacon's multi-file schema supertyping and dataset discovery.

/// DataFusion integration for the Arrow IPC file format.
pub mod datafusion;
