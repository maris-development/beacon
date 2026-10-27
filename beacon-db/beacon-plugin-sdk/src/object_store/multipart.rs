//! Multipart uploads that cross the plugin boundary.

use std::ffi::c_void;

use async_ffi::FfiFuture;
use stabby::vec::Vec as SVec;

use super::bytes::FFI_Bytes;
use super::error::FFI_ObjectStoreResult;
use super::options::FFI_PutResult;

/// An `object_store::MultipartUpload` as FFI data.
#[repr(C)]
pub struct FFI_MultipartUpload {
    /// Uploads one part.
    pub put_part: unsafe extern "C" fn(
        upload: &mut Self,
        payload: SVec<FFI_Bytes>,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,
    /// Completes the upload.
    pub complete:
        unsafe extern "C" fn(upload: &mut Self) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>>,
    /// Aborts the upload.
    pub abort: unsafe extern "C" fn(upload: &mut Self) -> FfiFuture<FFI_ObjectStoreResult<()>>,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(upload: &mut Self),
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
}

// Only the owner calls the functions, through `&mut Self`.
unsafe impl Send for FFI_MultipartUpload {}
