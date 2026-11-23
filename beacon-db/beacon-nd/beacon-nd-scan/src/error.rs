//! The map from dataset errors to DataFusion errors.

use beacon_nd_dataset::Error;
use datafusion::error::DataFusionError;

/// Map a dataset error to a DataFusion error. `Unsupported` becomes `NotImplemented`.
///
/// `Arrow` becomes `ArrowError`. Each other variant becomes `Execution` with its text.
// The scan tasks use this map; until they land, only the tests call it.
#[allow(dead_code)]
pub(crate) fn to_datafusion(error: Error) -> DataFusionError {
    match error {
        Error::Unsupported { .. } => DataFusionError::NotImplemented(error.to_string()),
        Error::Arrow(error) => DataFusionError::from(error),
        error => DataFusionError::Execution(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use beacon_nd_dataset::Error;
    use datafusion::error::DataFusionError;

    use super::to_datafusion;

    #[test]
    fn an_unsupported_file_is_not_implemented() {
        let error = to_datafusion(Error::Unsupported {
            dataset: "a.nd".into(),
            reason: "ragged".into(),
        });
        assert!(
            matches!(error, DataFusionError::NotImplemented(ref text) if text.contains("a.nd"))
        );
    }

    #[test]
    fn an_off_grid_file_is_an_execution_error_with_its_text() {
        let error = to_datafusion(Error::OffGrid {
            dataset: "a.nd".into(),
            grid: vec!["depth".into()],
            reason: "x".into(),
        });
        assert!(matches!(error, DataFusionError::Execution(ref text) if text.contains("depth")));
    }
}
