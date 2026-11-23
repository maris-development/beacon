//! The errors of the dataset contract.

/// The result type of this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// An error of the dataset contract.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file does not fit the grid of the scan.
    #[error("{dataset}: the file is not on the grid {grid:?}: {reason}")]
    OffGrid {
        /// The name of the dataset.
        dataset: String,
        /// The dimension names of the grid.
        grid: Vec<String>,
        /// Why the file is not on the grid.
        reason: String,
    },
    /// The region does not lie inside the variable.
    #[error("variable '{variable}': invalid region: {reason}")]
    InvalidRegion {
        /// The name of the variable.
        variable: String,
        /// Why the region is not valid.
        reason: String,
    },
    /// The dataset does not obey the contract.
    #[error("{dataset}: invalid dataset: {reason}")]
    Invalid {
        /// The name of the dataset.
        dataset: String,
        /// Why the dataset is not valid.
        reason: String,
    },
    /// The file is valid, but the contract cannot model it.
    #[error("{dataset}: {reason}")]
    Unsupported {
        /// The name of the dataset.
        dataset: String,
        /// Why the contract cannot model the file.
        reason: String,
    },
    /// An error of `nd-arrow-array`.
    #[error(transparent)]
    Nd(#[from] nd_arrow_array::NdError),
    /// An error of Arrow.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// The read of one variable failed.
    #[error("variable '{variable}': {error}")]
    Read {
        /// The name of the variable.
        variable: String,
        /// The error of the read.
        error: Box<Error>,
    },
    /// An error of the format: I/O, decode.
    #[error(transparent)]
    Format(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Wrap an error of the format.
    pub fn format(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Format(error.into())
    }
}
