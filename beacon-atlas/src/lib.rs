use arrow::ipc::CompressionType;
use once_cell::sync::Lazy;

pub mod array;
pub mod array_file;
pub mod arrow_object_store;
pub mod collection;
pub mod column;
pub mod config;
pub mod consts;
pub mod datafusion;
pub mod nd_array;
pub mod partition;
pub mod prelude;
pub mod scalar;
pub mod schema;
pub mod typed_vec;
pub mod util;

pub static IPC_WRITE_OPTS: Lazy<arrow::ipc::writer::IpcWriteOptions> = Lazy::new(|| {
    arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap()
        .with_dictionary_handling(arrow::ipc::writer::DictionaryHandling::Delta)
});
