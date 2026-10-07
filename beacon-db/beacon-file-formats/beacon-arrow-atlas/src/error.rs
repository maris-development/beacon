//! The seam between the crate's own errors and DataFusion's.
//!
//! Inside the crate an error is an `anyhow::Error` with context added. At
//! the boundary it becomes one `DataFusionError::External` whose message holds
//! the whole chain, as DataFusion prints only the outer error.

use datafusion::error::DataFusionError;

/// A crate error, as DataFusion reports it.
pub(crate) fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(format!("{error:#}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_names_every_cause() {
        let error =
            anyhow::anyhow!("column 'x' holds Int32 and Utf8").context("reading the schema");

        let message = external(error).to_string();

        assert!(
            message.contains("reading the schema: column 'x' holds Int32 and Utf8"),
            "{message}"
        );
    }
}
