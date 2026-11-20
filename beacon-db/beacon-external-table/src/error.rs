//! [`ExternalTableError`]: a scan error with the table, the file type and the file.

use std::{error::Error, fmt, sync::Arc};

use datafusion::common::DataFusionError;

/// The step of a scan that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPhase {
    /// The object store lookup or the file list.
    List,
    /// The format plan over the listed files.
    Plan,
    /// The open of one file.
    Open,
    /// The read of the batches of one file.
    Read,
}

impl fmt::Display for ScanPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::List => "list",
            Self::Plan => "plan",
            Self::Open => "open",
            Self::Read => "read",
        })
    }
}

/// A scan error of an external table.
///
/// The error is the payload of a `DataFusionError::External`. [`find`](Self::find)
/// gets it back from a query error.
#[derive(Debug)]
pub struct ExternalTableError {
    /// The table name of the `CREATE` statement.
    pub table: String,
    /// The lowercase file type of the table.
    pub file_type: String,
    /// The step that failed.
    pub phase: ScanPhase,
    /// The path of the failed file in its object store. `None` when no single file failed.
    pub object: Option<String>,
    /// The error of the format or the object store.
    pub source: DataFusionError,
}

impl ExternalTableError {
    /// The external table error in the source chain of `error`, if there is one.
    pub fn find(error: &DataFusionError) -> Option<&Self> {
        let mut current: Option<&(dyn Error + 'static)> = Some(error);
        while let Some(error) = current {
            if let Some(found) = error.downcast_ref::<Self>() {
                return Some(found);
            }
            current = error.source();
        }
        None
    }
}

impl fmt::Display for ExternalTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "external table '{}' ({}): ", self.table, self.file_type)?;
        match (self.phase, &self.object) {
            (ScanPhase::List, _) => write!(f, "cannot list the files")?,
            (ScanPhase::Plan, _) => write!(f, "cannot plan the scan")?,
            (ScanPhase::Open, Some(object)) => write!(f, "cannot open file '{object}'")?,
            (ScanPhase::Open, None) => write!(f, "cannot open the data")?,
            (ScanPhase::Read, Some(object)) => write!(f, "cannot read file '{object}'")?,
            (ScanPhase::Read, None) => write!(f, "cannot read the data")?,
        }
        write!(f, ": {}", self.source)
    }
}

impl Error for ExternalTableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// The table name and the file type that each scan error of a table carries.
#[derive(Debug, Clone)]
pub(crate) struct ErrorContext {
    table: Arc<str>,
    file_type: Arc<str>,
}

impl ErrorContext {
    pub(crate) fn new(table: &str, file_type: &str) -> Self {
        Self {
            table: table.into(),
            file_type: file_type.into(),
        }
    }

    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    pub(crate) fn file_type(&self) -> &str {
        &self.file_type
    }

    /// Wraps `source` in an [`ExternalTableError`] for `phase` and `object`.
    pub(crate) fn error(
        &self,
        phase: ScanPhase,
        object: Option<&str>,
        source: DataFusionError,
    ) -> DataFusionError {
        // A memory limit is no file fault, and a caller matches on this variant.
        if matches!(source, DataFusionError::ResourcesExhausted(_)) {
            return source;
        }
        DataFusionError::External(Box::new(ExternalTableError {
            table: self.table.to_string(),
            file_type: self.file_type.to_string(),
            phase,
            object: object.map(str::to_string),
            source,
        }))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::error::ArrowError,
        common::{DataFusionError, exec_datafusion_err},
    };

    use super::*;

    fn context() -> ErrorContext {
        ErrorContext::new("obs", "netcdf")
    }

    #[test]
    fn a_read_error_names_the_table_the_file_type_and_the_file() {
        let error = context().error(
            ScanPhase::Read,
            Some("cruises/a.nc"),
            exec_datafusion_err!("bad chunk"),
        );
        let message = error.to_string();
        assert!(
            message.contains(
                "external table 'obs' (netcdf): cannot read file 'cruises/a.nc': Execution error: bad chunk"
            ),
            "{message}"
        );
    }

    #[test]
    fn each_phase_has_its_own_text() {
        let text = |phase, object| {
            context()
                .error(phase, object, exec_datafusion_err!("x"))
                .to_string()
        };
        assert!(text(ScanPhase::List, None).contains("cannot list the files"));
        assert!(text(ScanPhase::Plan, None).contains("cannot plan the scan"));
        assert!(text(ScanPhase::Open, Some("a.nc")).contains("cannot open file 'a.nc'"));
        assert!(text(ScanPhase::Read, None).contains("cannot read the data"));
    }

    #[test]
    fn find_returns_the_fields_of_the_typed_error() {
        let error = context().error(
            ScanPhase::Open,
            Some("a.nc"),
            exec_datafusion_err!("no such file"),
        );
        let found = ExternalTableError::find(&error).expect("the typed error");
        assert_eq!(found.table, "obs");
        assert_eq!(found.file_type, "netcdf");
        assert_eq!(found.phase, ScanPhase::Open);
        assert_eq!(found.object.as_deref(), Some("a.nc"));
        assert!(found.source.to_string().contains("no such file"));
    }

    #[test]
    fn find_looks_through_a_context_and_an_arrow_error() {
        let error = context().error(ScanPhase::Read, Some("a.nc"), exec_datafusion_err!("x"));
        let in_context = error.context("while the query ran");
        assert!(ExternalTableError::find(&in_context).is_some());

        let in_arrow =
            DataFusionError::ArrowError(Box::new(ArrowError::ExternalError(Box::new(in_context))), None);
        assert!(ExternalTableError::find(&in_arrow).is_some());
    }

    #[test]
    fn find_returns_none_for_another_error() {
        assert!(ExternalTableError::find(&exec_datafusion_err!("x")).is_none());
    }

    #[test]
    fn a_memory_limit_error_stays_the_same_variant() {
        let error = context().error(
            ScanPhase::Read,
            Some("a.nc"),
            DataFusionError::ResourcesExhausted("no memory".to_string()),
        );
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    }
}
