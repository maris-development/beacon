pub const STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

/// Returns a recommended row count per Arrow IPC batch for the given data type.
///
/// These values are tuned for streaming throughput and memory usage:
/// smaller batches for variable-width types (strings/binary) and larger batches
/// for fixed-width numeric types. The values are heuristics, not guarantees;
/// callers may override them when dataset-specific behavior warrants it.
pub const fn arrow_chunk_size_by_type(data_type: &arrow::datatypes::DataType) -> usize {
    match data_type {
        arrow::datatypes::DataType::Utf8 => 16 * 1024, // 16K strings per batch
        arrow::datatypes::DataType::Binary => 16 * 1024, // 16K binary values per batch
        arrow::datatypes::DataType::LargeUtf8 => 8 * 1024, // 8K strings per batch
        arrow::datatypes::DataType::LargeBinary => 8 * 1024, // 8K binary values per batch

        arrow::datatypes::DataType::UInt8 | arrow::datatypes::DataType::Int8 => 512 * 1024, // 512K values per batch for 1 byte types
        arrow::datatypes::DataType::UInt16 | arrow::datatypes::DataType::Int16 => 256 * 1024, // 256K values per batch for 2 byte types
        arrow::datatypes::DataType::UInt32
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Float32 => 128 * 1024, // 128K values per batch for 4 byte types
        arrow::datatypes::DataType::UInt64
        | arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::Float64 => 64 * 1024, // 64K values per batch for 8 byte types

        // Timestamp types are always 64k values per batch, as they are 8 bytes each but often have good compression due to repeated values.
        arrow::datatypes::DataType::Timestamp(_, _) => 64 * 1024,
        arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64 => 64 * 1024, // Date types are also 8 bytes each and often compress well

        arrow::datatypes::DataType::Boolean => 512 * 1024, // 512K values per batch for boolean types (1 bit per value)
        _ => 4096,                                         // Default chunk size for other types
    }
}
