//! The errors of the statistics store.

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A redb transaction, table or commit failed.
    #[error("statistics database: {0}")]
    Database(#[from] redb::Error),
    /// A stored key or value does not decode, or a value does not encode.
    #[error("statistics value: {0}")]
    Codec(String),
    /// An array could not be built or cast.
    #[error("statistics array: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A blocking store task panicked or was cancelled.
    #[error("statistics task: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// redb has one error type for each kind of operation; each converts into `redb::Error`.
macro_rules! from_redb {
    ($($source:ty),* $(,)?) => {
        $(impl From<$source> for StoreError {
            fn from(error: $source) -> Self {
                Self::Database(error.into())
            }
        })*
    };
}

from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError,
);

impl From<bincode::Error> for StoreError {
    fn from(error: bincode::Error) -> Self {
        Self::Codec(error.to_string())
    }
}

/// The result of a store operation.
pub type Result<T> = std::result::Result<T, StoreError>;
