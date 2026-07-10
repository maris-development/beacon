//! Error types for the `beacon-nd-array` crate.
//!
//! [`NdArrayError`] is the crate's typed error for its "front-door" validation
//! paths (array construction, shape/dimension checks, broadcasting).
//!
//! The [`ArrayBackend`](crate::array::backend::ArrayBackend) trait and the
//! dataset/Arrow conversion helpers keep returning [`anyhow::Result`]: that trait
//! is implemented by every reader crate (NetCDF, Atlas, TIFF, Zarr, …), so a
//! typed error there would cascade across the whole workspace. The
//! [`NdArrayError::Other`] bridge lets the two coexist — an `anyhow::Error` from
//! a backend call converts into [`NdArrayError`] via `?`, and an
//! [`NdArrayError`] converts back into `anyhow::Error` at any caller that still
//! returns `anyhow::Result`.

/// Errors produced by `beacon-nd-array`'s array construction and shape logic.
#[derive(Debug, thiserror::Error)]
pub enum NdArrayError {
    /// The provided values could not be arranged into the requested shape.
    #[error("failed to create ndarray from values and dimensions: {0}")]
    InvalidShape(String),

    /// The number of shape axes does not match the number of named dimensions.
    #[error("shape length {shape_len} does not match dimensions length {dims_len}")]
    ShapeDimensionMismatch { shape_len: usize, dims_len: usize },

    /// The element count implied by the shape disagrees with the backend.
    #[error("total size implied by shape ({expected}) does not match backend length ({actual})")]
    SizeMismatch { expected: usize, actual: usize },

    /// A set of source dimensions is not a subset of the broadcast target.
    #[error(
        "source dimensions {source_dims:?} are not a subset of target dimensions {target_dims:?}"
    )]
    BroadcastDimensions {
        source_dims: Vec<String>,
        target_dims: Vec<String>,
    },

    /// Any other error, including failures bubbled up from an
    /// [`ArrayBackend`](crate::array::backend::ArrayBackend) call.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Result alias for fallible `beacon-nd-array` front-door operations.
pub type Result<T> = std::result::Result<T, NdArrayError>;
