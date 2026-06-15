//! Error types for the `beacon-common` crate.
//!
//! [`CommonError`] is the crate's unified error type. It aggregates the failure
//! modes of the utility modules (CF time parsing, super-typing) so callers can
//! match on a single type and get a descriptive [`Display`](std::fmt::Display)
//! message for logging.
//!
//! Lower-level helpers that sit directly on DataFusion trait paths (e.g.
//! [`parse_listing_table_url`](crate::listing_url::parse_listing_table_url) and
//! the [`TableProvider`](datafusion::catalog::TableProvider) implementations)
//! intentionally keep returning [`datafusion::error::Result`] — converting those
//! would cascade across every dependent crate and fight the trait signatures.

use crate::super_typing::SuperTypeError;

/// Errors produced by `beacon-common`'s utility functions.
#[derive(Debug, thiserror::Error)]
pub enum CommonError {
    /// Failed to parse a CF-convention time `units` string (bad calendar,
    /// reference date, or time unit). The message carries the offending input.
    #[error("{0}")]
    CfTime(String),

    /// Failed to compute a common super type across the provided schemas.
    #[error(transparent)]
    SuperType(#[from] SuperTypeError),
}

/// Result alias for fallible `beacon-common` operations.
pub type Result<T> = std::result::Result<T, CommonError>;
