//! Error types for the `beacon-config` crate.
//!
//! [`ConfigError`] is the crate's unified error type, covering the failure modes
//! of loading configuration from the environment and preparing Beacon's data
//! directories. Binaries surface these cleanly by calling [`init`](crate::init)
//! early in `main`.

use std::path::PathBuf;

/// Errors produced while loading Beacon's configuration or preparing its data
/// directories.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read or parse one or more configuration values from the
    /// environment (e.g. a non-numeric value for a numeric variable).
    #[error("failed to load configuration from environment: {0}")]
    EnvLoad(String),

    /// `BEACON_BASE_PATH` was set to a value that is not a valid URL base path.
    #[error("invalid BEACON_BASE_PATH: {0}")]
    InvalidBasePath(String),

    /// A required data directory could not be created.
    #[error("failed to create directory {}: {source}", .path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Result alias for fallible `beacon-config` operations.
pub type Result<T> = std::result::Result<T, ConfigError>;
