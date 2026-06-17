//! Error types for the `beacon-nd-arrow` crate.
//!
//! [`NdArrowError`] is the crate's typed error for its "front-door" array
//! construction and validation paths (mirroring `beacon-nd-array`'s
//! `NdArrayError`).
//!
//! The async traits ([`NdArrowArray`](crate::array::NdArrowArray),
//! [`ArrayBackend`](crate::array::backend::ArrayBackend),
//! [`ArrowTypeConversion`](crate::array::compat_typings::ArrowTypeConversion))
//! keep returning [`anyhow::Result`]; the [`NdArrowError::Other`] bridge lets a
//! typed error convert into `anyhow::Error` (and back, via `?`) so the two
//! coexist and the trait methods can migrate incrementally.

/// Errors produced by `beacon-nd-arrow`'s array construction and shape logic.
#[derive(Debug, thiserror::Error)]
pub enum NdArrowError {
    /// The provided values could not be arranged into the requested shape.
    #[error("failed to create ndarray from values and dimensions: {0}")]
    InvalidShape(String),

    /// The number of shape axes does not match the number of named dimensions.
    #[error("shape length {shape_len} does not match dimensions length {dims_len}")]
    ShapeDimensionMismatch { shape_len: usize, dims_len: usize },

    /// The element count implied by the shape disagrees with the backend.
    #[error("total size implied by shape ({expected}) does not match backend length ({actual})")]
    SizeMismatch { expected: usize, actual: usize },

    /// A supplied validity mask does not have the same shape as its array.
    #[error("validity shape {validity_shape:?} must match array shape {array_shape:?}")]
    ValidityShapeMismatch {
        validity_shape: Vec<usize>,
        array_shape: Vec<usize>,
    },

    /// Any other error, including failures bubbled up from a trait method.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Result alias for fallible `beacon-nd-arrow` front-door operations.
pub type Result<T> = std::result::Result<T, NdArrowError>;
