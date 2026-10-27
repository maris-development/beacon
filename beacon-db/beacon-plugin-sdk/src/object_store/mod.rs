//! Carries an `object_store::ObjectStore` across the plugin boundary.
//!
//! The module re-exports the `object_store` crate as [`store`], so a plugin uses
//! the same crate version as Beacon.

pub use ::object_store as store;

mod bytes;
mod error;
mod options;

pub use bytes::FFI_Bytes;
pub use error::{FFI_ErrorKind, FFI_ObjectStoreError, FFI_ObjectStoreResult};
pub use options::FFI_ObjectMeta;

/// The ABI version of the object store FFI types.
pub const OBJECT_STORE_FFI_VERSION: u64 = 1;

/// Returns [`OBJECT_STORE_FFI_VERSION`].
pub extern "C" fn object_store_ffi_version() -> u64 {
    OBJECT_STORE_FFI_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_ffi_version_is_one() {
        assert_eq!(object_store_ffi_version(), 1);
    }
}
