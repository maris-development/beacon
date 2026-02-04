use arrow::ipc::CompressionType;
use once_cell::sync::Lazy;

pub mod array;
pub mod array_chunked;
pub mod array_encoding;
pub mod arrow_dataset;
pub mod arrow_object_store;
pub mod attribute;
pub mod column;
pub mod column_reader;
pub mod consts;
pub mod dataset;
pub mod layout;
pub mod partition;
pub mod pruning;
pub mod rle;
pub mod schema;
pub mod stream;
pub mod util;
pub mod variable;

pub static IPC_WRITE_OPTS: Lazy<arrow::ipc::writer::IpcWriteOptions> = Lazy::new(|| {
    arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap()
        .with_dictionary_handling(arrow::ipc::writer::DictionaryHandling::Delta)
});
