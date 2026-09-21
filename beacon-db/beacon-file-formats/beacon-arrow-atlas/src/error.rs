//! The seam between the crate's own errors and DataFusion's.
//!
//! Inside the crate an error is an `anyhow::Error`, with context added where
//! the context says something the cause does not: the collection, the
//! dataset, the column. At the DataFusion boundary it becomes one
//! `DataFusionError::External`, chain and all, so a query reports what went
//! wrong and where without a second layer of formatting.

use datafusion::error::DataFusionError;

/// A crate error, as DataFusion reports it.
pub(crate) fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}
