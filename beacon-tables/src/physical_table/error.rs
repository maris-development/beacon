use super::table_engine::error::TableEngineError;

#[derive(Debug, thiserror::Error)]
pub enum PhysicalTableError {
    #[error("Table Creation Error: {0}")]
    TableCreationError(Box<PhysicalTableError>),
    #[error("Table Engine Error: {0}")]
    TableEngineError(#[from] TableEngineError),
}
