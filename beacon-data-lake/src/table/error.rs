#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Failed to read table configuration: {0}")]
    FailedToReadTableConfig(object_store::Error),
    #[error("Invalid table configuration: {0}")]
    InvalidTableConfig(serde_json::Error),
    #[error("Table error: {0}")]
    GenericTableError(String),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error(
        "Cannot delete table '{table_name}' because it is referenced by merged table '{merged_table}'"
    )]
    TableReferencedByMerged {
        table_name: String,
        merged_table: String,
    },
    #[error("Datafusion Error: {0}")]
    DatafusionError(#[from] datafusion::error::DataFusionError),
}
