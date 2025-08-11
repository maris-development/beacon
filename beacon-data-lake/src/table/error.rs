#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Invalid table configuration: {0}")]
    InvalidTableConfig(serde_json::Error),
}
