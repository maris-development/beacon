pub fn try_zarrs_dtype_to_arrow(
    data_type: zarrs::array::DataType,
) -> Result<arrow::datatypes::DataType, String> {
    match data_type {
        zarrs::array::DataType::Bool => Ok(arrow::datatypes::DataType::Boolean),
        zarrs::array::DataType::Int8 => Ok(arrow::datatypes::DataType::Int8),
        zarrs::array::DataType::Int16 => Ok(arrow::datatypes::DataType::Int16),
        zarrs::array::DataType::Int32 => Ok(arrow::datatypes::DataType::Int32),
        zarrs::array::DataType::Int64 => Ok(arrow::datatypes::DataType::Int64),
        zarrs::array::DataType::UInt8 => Ok(arrow::datatypes::DataType::UInt8),
        zarrs::array::DataType::UInt16 => Ok(arrow::datatypes::DataType::UInt16),
        zarrs::array::DataType::UInt32 => Ok(arrow::datatypes::DataType::UInt32),
        zarrs::array::DataType::UInt64 => Ok(arrow::datatypes::DataType::UInt64),
        zarrs::array::DataType::Float32 => Ok(arrow::datatypes::DataType::Float32),
        zarrs::array::DataType::Float64 => Ok(arrow::datatypes::DataType::Float64),
        zarrs::array::DataType::String => Ok(arrow::datatypes::DataType::Utf8),
        zarrs::array::DataType::Bytes => Ok(arrow::datatypes::DataType::Binary),
        _ => Err(format!("Unsupported Zarrs data type: {:?}", data_type)),
    }
}
