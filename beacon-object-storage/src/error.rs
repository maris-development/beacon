//! Error types for the Beacon object storage layer.

use std::env::VarError;

/// Errors produced while building or operating Beacon's object stores.
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    /// Failed to initialize a store or its filesystem watcher.
    #[error("Initialization error: {0}")]
    InitializationError(String),

    /// A required configuration value is missing.
    #[error("Missing configuration: {key}")]
    MissingConfig { key: &'static str },

    /// A configuration value is present but invalid.
    #[error("Invalid configuration for {key}: {message}")]
    InvalidConfig { key: &'static str, message: String },

    /// A required environment variable is missing or unreadable.
    #[error("Missing environment variable {var}: {source}")]
    MissingEnvVar {
        var: &'static str,
        #[source]
        source: VarError,
    },
}

/// Result alias for fallible object storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
