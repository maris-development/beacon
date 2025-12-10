use std::sync::Arc;

use arrow_schema::{ArrowError, DataType};
use nd_arrow_array::error::NdArrayError;

use crate::util::SuperTypeError;

pub type BBFResult<T> = std::result::Result<T, BBFError>;

#[derive(Debug, thiserror::Error)]
pub enum BBFError {
    #[error("BBF writing error: {0}")]
    Writing(#[from] BBFWritingError),
    #[error("BBF reading error: {0}")]
    Reading(#[from] BBFReadingError),
    #[error("Shared error: {0}")]
    Shared(#[from] Arc<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum BBFWritingError {
    #[error("Failed to cast arrow array with data type {0} => {1} : {2}")]
    ArrowCastFailure(DataType, DataType, ArrowError),
    #[error("Failed to create NdArrowArray: {0}: {1}")]
    NdArrowArrayCreationFailure(NdArrayError, String),
    #[error("Unable to find super type for array group: {0}")]
    SuperTypeNotFound(SuperTypeError),
    #[error("Failed to build array group: {0}")]
    ArrayGroupBuildFailure(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to create temporary file: {0} for array partition: {1}")]
    TempFileCreationFailure(std::io::Error, String),
    #[error("Failed to write array group to partition: {0}")]
    ArrayGroupWriteFailure(ArrowError),
    #[error("Failed to finalize array partition: {0}")]
    ArrayPartitionFinalizeFailure(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum BBFReadingError {
    #[error("Failed reading array from array group: {0} with error: {1}")]
    ArrayGroupReadFailure(String, Box<dyn std::error::Error + Send + Sync>),
}
