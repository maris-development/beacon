//! Error types for the `beacon-common` crate.
//!
//! [`CommonError`] is the crate's unified error type. It aggregates the failure
//! modes of the utility modules (CF time parsing) so callers can match on a
//! single type and get a descriptive [`Display`](std::fmt::Display) message for
//! logging.
//!
//! Some helpers sit on a DataFusion trait path. Two examples are
//! [`parse_listing_table_url`](crate::listing_url::parse_listing_table_url) and
//! each [`TableProvider`](datafusion::catalog::TableProvider) implementation.
//! They keep [`datafusion::error::Result`] on purpose. A change there reaches
//! every dependent crate, and it fights the trait signatures.

/// The errors of the utility functions in `beacon-common`.
#[derive(Debug, thiserror::Error)]
pub enum CommonError {
    /// A CF `units` string does not parse. The calendar, the reference date or the
    /// time unit is wrong. The message holds the input.
    #[error("{0}")]
    CfTime(String),
}

/// The result alias for a `beacon-common` operation that can fail.
pub type Result<T> = std::result::Result<T, CommonError>;
