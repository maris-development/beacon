//! Classification of `zarrs::array::DataType` (a struct in zarrs >= 0.23) into a
//! match-friendly enum so call sites can dispatch by built-in dtype.

use zarrs::array::DataType;
use zarrs::array::data_type::{
    BoolDataType, BytesDataType, Float32DataType, Float64DataType, Int8DataType, Int16DataType,
    Int32DataType, Int64DataType, StringDataType, UInt8DataType, UInt16DataType, UInt32DataType,
    UInt64DataType,
};

/// Built-in zarrs data types we have explicit Arrow mappings for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZarrDtypeKind {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Bytes,
    /// Any data type not covered above (custom / extension types).
    Other,
}

/// Discriminates a [`DataType`] into a [`ZarrDtypeKind`].
pub fn classify(data_type: &DataType) -> ZarrDtypeKind {
    if data_type.is::<BoolDataType>() {
        ZarrDtypeKind::Bool
    } else if data_type.is::<Int8DataType>() {
        ZarrDtypeKind::Int8
    } else if data_type.is::<Int16DataType>() {
        ZarrDtypeKind::Int16
    } else if data_type.is::<Int32DataType>() {
        ZarrDtypeKind::Int32
    } else if data_type.is::<Int64DataType>() {
        ZarrDtypeKind::Int64
    } else if data_type.is::<UInt8DataType>() {
        ZarrDtypeKind::UInt8
    } else if data_type.is::<UInt16DataType>() {
        ZarrDtypeKind::UInt16
    } else if data_type.is::<UInt32DataType>() {
        ZarrDtypeKind::UInt32
    } else if data_type.is::<UInt64DataType>() {
        ZarrDtypeKind::UInt64
    } else if data_type.is::<Float32DataType>() {
        ZarrDtypeKind::Float32
    } else if data_type.is::<Float64DataType>() {
        ZarrDtypeKind::Float64
    } else if data_type.is::<StringDataType>() {
        ZarrDtypeKind::String
    } else if data_type.is::<BytesDataType>() {
        ZarrDtypeKind::Bytes
    } else {
        ZarrDtypeKind::Other
    }
}

pub fn try_zarrs_dtype_to_arrow(
    data_type: &DataType,
) -> Result<arrow::datatypes::DataType, String> {
    match classify(data_type) {
        ZarrDtypeKind::Bool => Ok(arrow::datatypes::DataType::Boolean),
        ZarrDtypeKind::Int8 => Ok(arrow::datatypes::DataType::Int8),
        ZarrDtypeKind::Int16 => Ok(arrow::datatypes::DataType::Int16),
        ZarrDtypeKind::Int32 => Ok(arrow::datatypes::DataType::Int32),
        ZarrDtypeKind::Int64 => Ok(arrow::datatypes::DataType::Int64),
        ZarrDtypeKind::UInt8 => Ok(arrow::datatypes::DataType::UInt8),
        ZarrDtypeKind::UInt16 => Ok(arrow::datatypes::DataType::UInt16),
        ZarrDtypeKind::UInt32 => Ok(arrow::datatypes::DataType::UInt32),
        ZarrDtypeKind::UInt64 => Ok(arrow::datatypes::DataType::UInt64),
        ZarrDtypeKind::Float32 => Ok(arrow::datatypes::DataType::Float32),
        ZarrDtypeKind::Float64 => Ok(arrow::datatypes::DataType::Float64),
        ZarrDtypeKind::String => Ok(arrow::datatypes::DataType::Utf8),
        ZarrDtypeKind::Bytes => Ok(arrow::datatypes::DataType::Binary),
        ZarrDtypeKind::Other => Err(format!("Unsupported Zarrs data type: {:?}", data_type)),
    }
}
