use arrow::error::ArrowError;
use ndarray::ShapeError;
use netcdf::{types::NcVariableType, AttributeValue};

use crate::encoders::EncoderError;

#[derive(Debug, thiserror::Error)]
pub enum ArrowNetCDFError {
    #[error("Internal NetCDF error: {0}")]
    NetCDFError(#[from] netcdf::Error),
    #[error("Failed to translate NetCDF to Arrow Schema: {0}")]
    ArrowSchemaError(Box<ArrowNetCDFError>),
    #[error("Unsupported data type: {0:?}")]
    UnsupportedNetCDFDataType(NcVariableType),
    #[error("Unsupported Attribute Value Type: {0:?}")]
    UnsupportedAttributeValueType(AttributeValue),
    #[error("Unable to transform NetCDF Attribute to NdArray: {0}")]
    AttributeShapeError(ShapeError),
    #[error("Failed to read variable data: {0}")]
    VariableReadError(Box<ArrowNetCDFError>),
    #[error("Failed to read & decode time variable: {0}")]
    TimeVariableReadError(Box<ArrowNetCDFError>),
    #[error("Failed to read record batch: {0}")]
    RecordBatchReadError(Box<ArrowNetCDFError>),
    #[error("Unable to project arrow schema: {0}")]
    ArrowSchemaProjectionError(ArrowError),
    #[error("Invalid field name in schema: {0}")]
    InvalidFieldName(String),
    #[error("Unable to broadcast current selection of arrays to a common shape: {0}")]
    UnableToBroadcast(String),
    #[error("Failed to create arrow record batch: {0}")]
    ArrowRecordBatchError(ArrowError),
    #[error("Encoder Error: {0}")]
    EncoderError(EncoderError),
    #[error("Unable to create encoder: {0}")]
    EncoderCreationError(Box<ArrowNetCDFError>),
    #[error("Unable to Create NetCDF writer: {0}")]
    NetCDFWriterError(Box<ArrowNetCDFError>),
    #[error("Failed to write record batch: {0}")]
    RecordBatchWriteError(Box<ArrowNetCDFError>),
    #[error("Failed to create spooled ipc temp file: {0}")]
    IpcBufferFileError(ArrowError),
    #[error("Ipc Buffer Failed to write: {0}")]
    IpcBufferWriteError(ArrowError),
    #[error("Ipc Buffer Failed to read: {0}")]
    IpcBufferReadError(ArrowError),
    #[error("Ipc Buffer Failed to close: {0}")]
    IpcBufferCloseError(ArrowError),
    #[error("Ipc Buffer Failed to Open for reading: {0}")]
    IpcBufferOpenError(ArrowError),
    #[error("Stream Error: {0}")]
    Stream(String),
    #[error("Reader Error: {0}")]
    Reader(String),
}
