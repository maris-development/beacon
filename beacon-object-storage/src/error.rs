#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Event error: {0}")]
    EventError(#[from] crate::event::EventError),
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
}
