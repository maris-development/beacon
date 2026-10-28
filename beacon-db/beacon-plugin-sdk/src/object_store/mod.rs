//! Carries an `object_store::ObjectStore` across the plugin boundary.
//!
//! The module re-exports the `object_store` crate as [`store`], so a plugin uses
//! the same crate version as Beacon.

pub use ::object_store as store;

mod bytes;
mod error;
mod export;
mod foreign;
mod multipart;
mod options;
mod stream;

pub use bytes::FFI_Bytes;
pub use error::{FFI_ErrorKind, FFI_ObjectStoreError, FFI_ObjectStoreResult};
pub use export::{FFI_GetResult, FFI_ObjectStore};
pub use foreign::ForeignObjectStore;
pub use multipart::{FFI_MultipartUpload, ForeignMultipartUpload};
pub use options::{
    FFI_Attribute, FFI_Attributes, FFI_CopyOptions, FFI_GetOptions, FFI_GetRange,
    FFI_GetResultMeta, FFI_ListResult, FFI_ObjectMeta, FFI_PutMode, FFI_PutMultipartOptions,
    FFI_PutOptions, FFI_PutResult, FFI_Range, FFI_RenameOptions, FFI_TagSet,
};
pub use stream::{FFI_BytesStream, FFI_MetaStream, FFI_PathStream, FFI_Stream};

#[cfg(test)]
mod tests;
