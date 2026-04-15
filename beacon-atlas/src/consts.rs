use beacon_nd_arrow::datatypes::NdArrayDataType;

pub const STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
pub const DATASET_PREFETCH_CONCURRENCY: usize = 16;
pub(crate) const COLLECTION_METADATA_FILE: &str = "atlas.json";
pub(crate) const PARTITION_METADATA_FILE: &str = "atlas_partition.json";
pub(crate) const ENTRIES_FILE: &str = "entries.arrow";
pub(crate) const ENTRIES_COLUMN_NAME: &str = "__entry_key";
pub(crate) const ARRAY_FILE_NAME: &str = "array";
pub(crate) const LAYOUT_FILE_NAME: &str = "layout.arrow";
pub(crate) const STATISTICS_FILE_NAME: &str = "statistics.arrow";
pub(crate) const DEFAULT_IO_CACHE_BYTES: usize = 512 * 1024 * 1024; // 64 MiB
pub(crate) const DEFAULT_CHUNK_FETCH_CONCURRENCY: usize = 16;

/// Returns a recommended row count per Arrow IPC batch for the given data type.
///
/// These values are tuned for streaming throughput and memory usage:
/// smaller batches for variable-width types (strings/binary) and larger batches
/// for fixed-width numeric types. The values are heuristics, not guarantees;
/// callers may override them when dataset-specific behavior warrants it.
pub const fn chunk_size_by_type(data_type: &NdArrayDataType) -> usize {
    match data_type {
        NdArrayDataType::Bool => 512 * 1024, // 256k rows per batch for booleans (1 bit per value)
        NdArrayDataType::I8 => 512 * 1024,   // 512k rows per batch for i8 (1 byte per value)
        NdArrayDataType::I16 => 512 * 1024,  // 512k rows per batch for i16 (2 bytes per value)
        NdArrayDataType::I32 => 256 * 1024,  // 256k rows per batch for i32 (4 bytes per value)
        NdArrayDataType::I64 => 128 * 1024,  // 128k rows per batch for i64 (8 bytes per value)
        NdArrayDataType::U8 => 512 * 1024,   // 512k rows per batch for u8 (1 byte per value)
        NdArrayDataType::U16 => 512 * 1024,  // 512k rows per batch for u16 (2 bytes per value)
        NdArrayDataType::U32 => 256 * 1024,  // 256k rows per batch for u32 (4 bytes per value)
        NdArrayDataType::U64 => 128 * 1024,  // 128k rows per batch for u64 (8 bytes per value)
        NdArrayDataType::F32 => 256 * 1024,  // 256k rows per batch for f32 (4 bytes per value)
        NdArrayDataType::F64 => 128 * 1024,  // 128k rows per batch for f64 (8 bytes per value)
        NdArrayDataType::Timestamp => 128 * 1024, // 128k rows per batch for timestamps (8 bytes per value)
        NdArrayDataType::Binary => 64 * 1024,     // 64k rows per batch for binary (variable width)
        NdArrayDataType::String => 64 * 1024,     // 64k rows per batch for strings (variable width)
    }
}
