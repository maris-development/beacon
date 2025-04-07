#[derive(Debug, thiserror::Error)]
pub enum TableEngineError {
    #[error("Datafusion Error: {0}")]
    DatafusionError(#[from] datafusion::error::DataFusionError),
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Invalid Path.")]
    InvalidPath,
    #[error("Failed to create table provider: {0}")]
    TableProviderError(anyhow::Error),
}
