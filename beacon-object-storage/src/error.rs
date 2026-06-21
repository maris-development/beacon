//! Error types for the Beacon object storage layer.

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

    /// An error returned by the underlying `object_store` backend (S3 or local
    /// filesystem), e.g. while building a store or canonicalizing its root.
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

/// Result alias for fallible object storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
