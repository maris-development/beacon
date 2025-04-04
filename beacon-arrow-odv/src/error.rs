use arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum OdvError {
    #[error("Failed to open ODV file: {0}")]
    FileOpenError(#[from] std::io::Error),
    #[error("Failed to parse ODV header to Arrow schema: {0}")]
    ArrowSchemaError(Box<OdvError>),
    #[error("Failed to parse ODV header to Arrow field {field}: {inner}")]
    ArrowFieldError { inner: Box<OdvError>, field: String },
    #[error("Failed to read ODV columns: {0}")]
    ColumnReadError(ArrowError),
    #[error("Unsupported data type: {0:?}")]
    UnsupportedDataType(String),
    #[error("Failed to read metadata lines: {0}")]
    MetadataReadError(std::io::Error),
    #[error("Failed to read ODV column reader: {0}")]
    ColumnReaderCreationError(ArrowError),
    #[error("Failed to Create ODV Arrow RecordBatch: {0}")]
    RecordBatchCreationError(ArrowError),
    #[error("Failed to apply ODV schema projection: {0}")]
    SchemaProjectionError(ArrowError),
    #[error("Failed to decode ODV RecordBatch: {0}")]
    RecordBatchDecodeError(Box<OdvError>),
    #[error("QF field {0} is relative to a field that does not exist")]
    QualityControlFieldNotFound(String),
    #[error("Invalid QF field {0}")]
    InvalidQualityControlField(String),
}
