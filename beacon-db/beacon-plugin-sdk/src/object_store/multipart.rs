//! Multipart uploads that cross the plugin boundary.

use std::ffi::c_void;
use std::fmt;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ::bytes::Bytes;
use ::object_store::{Error, MultipartUpload, PutPayload, PutResult, Result, UploadPart};
use async_ffi::FfiFuture;
use async_trait::async_trait;
use futures::FutureExt;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::bytes::FFI_Bytes;
use super::error::{FFI_ObjectStoreResult, STORE};
use super::export::{drop_quietly, export_entered};
use super::options::FFI_PutResult;
use super::{OBJECT_STORE_FFI_VERSION, object_store_ffi_version};

/// An `object_store::MultipartUpload` as FFI data.
///
/// The exporter builds it with [`FFI_MultipartUpload::new`]. The receiver uses it through
/// [`ForeignMultipartUpload`]. `Drop` calls `release`. A release before a successful
/// `complete` or `abort` aborts the upload in the background on the exporter runtime.
///
/// Only the FFI functions read or change these fields.
#[repr(C)]
pub struct FFI_MultipartUpload {
    /// Returns [`OBJECT_STORE_FFI_VERSION`] of the exporter. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// Uploads one part. Several parts can be in flight at the same time.
    ///
    /// Fails while a `complete` or `abort` is pending.
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

struct UploadPrivateData {
    upload: Arc<Mutex<Box<dyn MultipartUpload>>>,
    handle: Handle,
    /// Set when `complete` or `abort` succeeds.
    finished: Arc<AtomicBool>,
}

impl FFI_MultipartUpload {
    /// Exports `upload`. Each poll runs inside `handle.enter()`, and a drop
    /// before the end aborts the upload on `handle`.
    pub fn new(upload: Box<dyn MultipartUpload>, handle: Handle) -> Self {
        let private_data = Box::new(UploadPrivateData {
            upload: Arc::new(Mutex::new(upload)),
            handle,
            finished: Arc::new(AtomicBool::new(false)),
        });
        Self {
            version: object_store_ffi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            put_part: put_part_fn,
            complete: complete_fn,
            abort: abort_fn,
            release: release_fn,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    fn data(&self) -> &UploadPrivateData {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const UploadPrivateData) }
    }
}

impl Drop for FFI_MultipartUpload {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

fn busy_error() -> Error {
    Error::Generic {
        store: STORE,
        source: "multipart upload is busy with complete or abort".into(),
    }
}

unsafe extern "C" fn put_part_fn(
    upload: &mut FFI_MultipartUpload,
    payload: SVec<FFI_Bytes>,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    let upload = &*upload;
    export_entered(
        || upload.data().handle.clone(),
        move || {
            let payload: PutPayload = payload.into_iter().map(Bytes::from).collect();
            // The store numbers the parts in call order, so `put_part` runs now.
            let Ok(mut inner) = upload.data().upload.try_lock() else {
                return ready(Err(busy_error())).boxed();
            };
            inner.put_part(payload)
        },
    )
}

unsafe extern "C" fn complete_fn(
    upload: &mut FFI_MultipartUpload,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>> {
    let upload = &*upload;
    export_entered(
        || upload.data().handle.clone(),
        || {
            let shared = Arc::clone(&upload.data().upload);
            let finished = Arc::clone(&upload.data().finished);
            async move {
                let mut inner = shared.lock().await;
                let result = inner.complete().await?;
                // Set under the lock, so a release task sees it.
                finished.store(true, Ordering::SeqCst);
                drop(inner);
                Ok(FFI_PutResult::from(&result))
            }
            .boxed()
        },
    )
}

unsafe extern "C" fn abort_fn(
    upload: &mut FFI_MultipartUpload,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    let upload = &*upload;
    export_entered(
        || upload.data().handle.clone(),
        || {
            let shared = Arc::clone(&upload.data().upload);
            let finished = Arc::clone(&upload.data().finished);
            async move {
                let mut inner = shared.lock().await;
                inner.abort().await?;
                // Set under the lock, so a release task sees it.
                finished.store(true, Ordering::SeqCst);
                drop(inner);
                Ok(())
            }
            .boxed()
        },
    )
}

unsafe extern "C" fn release_fn(upload: &mut FFI_MultipartUpload) {
    if upload.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(upload.private_data as *mut UploadPrivateData) };
    upload.private_data = std::ptr::null_mut();
    drop_quietly(move || {
        let handle = data.handle.clone();
        let _guard = handle.enter();
        if !data.finished.load(Ordering::SeqCst) {
            let shared = Arc::clone(&data.upload);
            let finished = Arc::clone(&data.finished);
            handle.spawn(async move {
                let mut upload = shared.lock().await;
                // A successful `complete` or `abort` sets `finished` before it frees the lock.
                if finished.load(Ordering::SeqCst) {
                    return;
                }
                if let Err(error) = upload.abort().await {
                    tracing::warn!("abort of a dropped multipart upload failed: {error}");
                }
            });
        }
        drop(data);
    });
}

/// An `object_store::MultipartUpload` over an [`FFI_MultipartUpload`] from another library.
///
/// A drop before a successful `complete` or `abort` makes the exporter abort the upload
/// in the background. The receiver does not wait for that abort.
pub struct ForeignMultipartUpload(FFI_MultipartUpload);

impl ForeignMultipartUpload {
    /// Fails with `NotSupported` when the table has another [`OBJECT_STORE_FFI_VERSION`].
    /// The refused table is released, so the exporter aborts the upload.
    pub fn try_new(upload: FFI_MultipartUpload) -> Result<Self> {
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (upload.version)() };
        if version != OBJECT_STORE_FFI_VERSION {
            return Err(Error::NotSupported {
                source: format!(
                    "multipart upload FFI version {version} is not supported (want {OBJECT_STORE_FFI_VERSION})"
                )
                .into(),
            });
        }
        Ok(Self(upload))
    }
}

impl fmt::Debug for ForeignMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ForeignMultipartUpload")
    }
}

// SAFETY for each call below: the table is live, and the function pointer matches it.
#[async_trait]
impl MultipartUpload for ForeignMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let payload: SVec<FFI_Bytes> = data.into_iter().map(FFI_Bytes::from).collect();
        let future = unsafe { (self.0.put_part)(&mut self.0, payload) };
        async move { future.await.into_result() }.boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let future = unsafe { (self.0.complete)(&mut self.0) };
        future.await.into_result().map(PutResult::from)
    }

    async fn abort(&mut self) -> Result<()> {
        let future = unsafe { (self.0.abort)(&mut self.0) };
        future.await.into_result()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::resume_unwind;

    use super::*;

    /// An upload that panics in each call.
    #[derive(Debug)]
    struct PanicUpload;

    #[async_trait]
    impl MultipartUpload for PanicUpload {
        fn put_part(&mut self, _: PutPayload) -> UploadPart {
            resume_unwind(Box::new("put_part"))
        }

        async fn complete(&mut self) -> Result<PutResult> {
            resume_unwind(Box::new("complete"))
        }

        async fn abort(&mut self) -> Result<()> {
            resume_unwind(Box::new("abort"))
        }
    }

    fn assert_panic_error(error: Error, text: &str) {
        assert!(matches!(error, Error::Generic { .. }), "{error:?}");
        let message = error.to_string();
        assert!(
            message.contains(&format!("panic in object store call: {text}")),
            "{message}"
        );
    }

    fn panicking_upload() -> ForeignMultipartUpload {
        let ffi = FFI_MultipartUpload::new(Box::new(PanicUpload), Handle::current());
        ForeignMultipartUpload::try_new(ffi).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panics_in_upload_calls_are_errors() {
        let mut upload = panicking_upload();
        let error = upload
            .put_part(PutPayload::from_static(b"x"))
            .await
            .unwrap_err();
        assert_panic_error(error, "put_part");
        assert_panic_error(upload.complete().await.unwrap_err(), "complete");
        assert_panic_error(upload.abort().await.unwrap_err(), "abort");
        // The drop spawns an abort that panics on the host runtime only.
        drop(upload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn release_twice_is_a_no_op() {
        let mut ffi = FFI_MultipartUpload::new(Box::new(PanicUpload), Handle::current());
        unsafe { (ffi.release)(&mut ffi) };
        assert!(ffi.private_data.is_null());
        drop(ffi);
    }
}
