//! The seam between the crate's own errors and DataFusion's.
//!
//! Inside the crate an error is an `anyhow::Error` with context added. At
//! the boundary it becomes one `DataFusionError::External`, chain and all.

use datafusion::error::DataFusionError;

/// A crate error, as DataFusion reports it.
pub(crate) fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}
