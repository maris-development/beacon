//! File format plugins: the FFI tables and the traits that plugin authors implement.

mod statistics;
mod stream;
mod types;

pub use statistics::{decode_statistics, encode_statistics};
pub use stream::{export_batch_stream, import_batch_stream};
pub use types::{
    ArgKind, ArgSpec, FFI_ArgSpec, FFI_ScanRequest, FFI_SortKey, FFI_WriteRequest, OutputKind,
    ScanRequest, SortKey, WriteRequest,
};

/// The ABI version of the file format FFI types.
///
/// Increase it on any change of a table layout, of an FFI enum value, or of the `beacon.nd`
/// encoding.
pub const FILE_FORMAT_FFI_VERSION: u64 = 1;

/// Returns [`FILE_FORMAT_FFI_VERSION`].
pub extern "C" fn file_format_ffi_version() -> u64 {
    FILE_FORMAT_FFI_VERSION
}

#[cfg(test)]
pub(crate) mod tests;
