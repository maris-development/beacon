//! File format plugins: the FFI tables and the traits that plugin authors implement.

mod export;
mod factory;
mod foreign;
mod reader;
mod statistics;
mod stream;
mod types;

pub use export::FFI_FileFormat;
pub use factory::{
    FFI_FileFormatFactory, FFI_OptionPair, FILE_FORMATS_SYMBOL, FileFormatsFn,
    ForeignFormatFactory, FormatFactory, export_file_formats, import_format_factory,
};
pub use foreign::ForeignFormatReader;
pub use reader::FormatReader;
pub use statistics::{decode_statistics, encode_statistics};
pub use stream::{export_batch_stream, export_batch_stream_entered, import_batch_stream};
pub use types::{
    ArgKind, ArgSpec, FFI_ArgSpec, FFI_ScanRequest, FFI_SortKey, FFI_WriteRequest, OutputKind,
    ScanRequest, SortKey, WriteRequest,
};

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod reader_tests;

#[cfg(test)]
pub(crate) mod factory_tests;
