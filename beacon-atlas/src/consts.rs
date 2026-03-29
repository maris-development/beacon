pub const STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
pub(crate) const COLLECTION_METADATA_FILE: &str = "atlas.json";
pub(crate) const PARTITION_METADATA_FILE: &str = "atlas_partition.json";
pub(crate) const ENTRIES_FILE: &str = "entries.arrow";
pub(crate) const ENTRIES_COLUMN_NAME: &str = "__entry_key";
pub(crate) const ARRAY_FILE_NAME: &str = "array.arrow";
pub(crate) const LAYOUT_FILE_NAME: &str = "layout.arrow";
pub(crate) const STATISTICS_FILE_NAME: &str = "statistics.arrow";
pub(crate) const DEFAULT_IO_CACHE_BYTES: usize = 256 * 1024 * 1024; // 64 MiB
pub(crate) const DEFAULT_CHUNK_FETCH_CONCURRENCY: usize = 16;

/// Returns a recommended row count per Arrow IPC batch for the given data type.
///
/// These values are tuned for streaming throughput and memory usage:
/// smaller batches for variable-width types (strings/binary) and larger batches
/// for fixed-width numeric types. The values are heuristics, not guarantees;
/// callers may override them when dataset-specific behavior warrants it.
pub const fn arrow_chunk_size_by_type(data_type: &arrow::datatypes::DataType) -> usize {
    match data_type {
        arrow::datatypes::DataType::Utf8 => 32 * 1024, // 32K strings per batch
        arrow::datatypes::DataType::Binary => 32 * 1024, // 32K binary values per batch
        arrow::datatypes::DataType::LargeUtf8 => 16 * 1024, // 16K strings per batch
        arrow::datatypes::DataType::LargeBinary => 16 * 1024, // 16K binary values per batch

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
        _ => 64 * 1024,                                    // Default chunk size for other types
    }
}
