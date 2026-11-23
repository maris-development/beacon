//! The map from dataset errors to DataFusion errors.

use beacon_nd_dataset::Error;
use datafusion::error::DataFusionError;

/// Map a dataset error to a DataFusion error. `Unsupported` becomes `NotImplemented`.
///
/// `Arrow` becomes `ArrowError`. `Read` maps its inner error and adds the variable as context.
/// Each other variant becomes `Execution` with its text.
pub fn to_datafusion(error: Error) -> DataFusionError {
    match error {
        Error::Unsupported { .. } => DataFusionError::NotImplemented(error.to_string()),
        Error::Arrow(error) => DataFusionError::from(error),
        Error::Read { variable, error } => {
            to_datafusion(*error).context(format!("variable '{variable}'"))
        }
        error => DataFusionError::Execution(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use arrow::error::ArrowError;
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

    #[test]
    fn a_read_error_keeps_the_kind_of_its_inner_error_and_names_the_variable() {
        let error = to_datafusion(Error::Read {
            variable: "temp".into(),
            error: Box::new(Error::Unsupported {
                dataset: "a.nd".into(),
                reason: "a filter".into(),
            }),
        });
        assert!(error.to_string().contains("variable 'temp'"), "{error}");
        assert!(
            matches!(error.find_root(), DataFusionError::NotImplemented(_)),
            "{error}"
        );
    }

    #[test]
    fn an_arrow_error_stays_an_arrow_error() {
        let error = to_datafusion(Error::Arrow(ArrowError::ComputeError("overflow".into())));
        assert!(
            matches!(error, DataFusionError::ArrowError(ref inner, _)
                if matches!(**inner, ArrowError::ComputeError(ref text) if text == "overflow")),
            "{error}"
        );
    }
}
