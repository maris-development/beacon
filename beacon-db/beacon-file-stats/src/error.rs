//! The crate error type.

/// Every fallible operation in this crate returns this error.
#[derive(Debug, thiserror::Error)]
pub enum FileStatsError {
    #[error("registry: {0}")]
    Registry(String),

    #[error("segment format: {0}")]
    Format(String),

    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, FileStatsError>;

impl From<redb::Error> for FileStatsError {
    fn from(value: redb::Error) -> Self {
        Self::Registry(value.to_string())
    }
}

macro_rules! redb_error {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for FileStatsError {
                fn from(value: $ty) -> Self {
                    Self::Registry(value.to_string())
                }
            }
        )+
    };
}

redb_error!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
);
