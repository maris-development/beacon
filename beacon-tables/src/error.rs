use crate::{logical_table::LogicalTableError, physical_table};

#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("Failed to read table directory: {0}")]
    TableConfigLoadReadError(std::io::Error),
    #[error("Failed to open table: {0} with error: {1}")]
    FailedToOpenTable(String, Box<Self>),
    #[error("Table {0} does not exist or cannot be discovered.")]
    TableDoesNotExist(String),
    #[error("Logical table error: {0}")]
    LogicalTableError(#[from] LogicalTableError),
    #[error("Failed to find table configuration file: {0} for table: {1}")]
    TableConfigFileError(std::io::Error, String),
    #[error("Failed to parse table configuration file: {0} for table: {1}")]
    TableConfigFileParseError(serde_json::Error, String),
    #[error("Failed to create table: {0}")]
    TableCreationError(Box<Self>),
    #[error("Failed to serialize table config: {0}")]
    TableConfigSerializationError(serde_json::Error),
    #[error("Failed to write table config to disk: {0}")]
    TableConfigWriteError(std::io::Error),
    #[error("Failed to delete table: {0}")]
    TableDeletionError(Box<Self>),
    #[error("Table IO error: {0}")]
    TableIOError(#[from] std::io::Error),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("Base Table Directory does not exist.")]
    BaseTableDirectoryDoesNotExist,
    #[error("Failed to create Base Table Directory: {0}")]
    FailedToCreateBaseTableDirectory(std::io::Error),
    #[error("Failed to create table directory: {0} for table: {1}")]
    FailedToCreateTableDirectory(std::io::Error, String),
    #[error("Physical table error: {0}")]
    PhysicalTableError(#[from] physical_table::error::PhysicalTableError),
    #[error("Invalid table name: {0}")]
    InvalidTableName(String),
    #[error("Failed to create table provider: {0}")]
    TableProviderCreationError(#[from] datafusion::error::DataFusionError),
    #[error("Table error: {0}")]
    TableError(String),
}
