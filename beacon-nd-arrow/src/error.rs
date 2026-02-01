use arrow::error::ArrowError;

use crate::dimensions::Dimensions;

/// Errors produced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum NdArrayError {
    /// Broadcasting failed.
    #[error("Broadcasting error occurred: {0}")]
    BroadcastingError(#[from] BroadcastError),

    /// The flat storage length does not match the expected size derived from the dimensions.
    #[error("Array length: {0} and dimensions: {1:?} don't align.")]
    MisalignedArrayDimensions(usize, Dimensions),

    /// The Arrow column does not match the expected ND physical layout.
    #[error("Invalid ND column representation: {0}")]
    InvalidNdColumn(String),

    /// The ND record batch is structurally invalid.
    #[error("Invalid ND batch: {0}")]
    InvalidNdBatch(String),

    /// Dimensions are structurally invalid (e.g. empty or duplicate names).
    #[error("Invalid dimensions: {0}")]
    InvalidDimensions(String),

    /// Slice indices are invalid for the current dimensions.
    #[error("Invalid slice: {0}")]
    InvalidSlice(String),

    /// Wrapper around Arrow-level failures.
    #[error("Arrow Error: {0}")]
    Arrow(#[from] ArrowError),
}

/// Errors specific to broadcasting.
#[derive(Debug, thiserror::Error)]
pub enum BroadcastError {
    /// The input and target dimensions cannot be broadcast under the active mode.
    #[error("Incompatible shapes: {0:?} and {1:?}")]
    IncompatibleShapes(Dimensions, Dimensions),

    /// A set of dimensions cannot be broadcast to a common result.
    #[error("Cannot broadcast shapes: {0:?}")]
    NoBroadcastableShape(Vec<Dimensions>),
}
