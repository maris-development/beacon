#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Failed to read table configuration: {0}")]
    FailedToReadTableConfig(object_store::Error),
    #[error("Invalid table configuration: {0}")]
    InvalidTableConfig(serde_json::Error),
}
