//! A configured file format as an FFI table.

use std::ffi::c_void;
use std::future::{Future, ready};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_ffi::{FfiFuture, FutureExt as _};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion_ffi::arrow_wrappers::WrappedSchema;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use datafusion_ffi::util::{FFI_Option, FFI_Result};
use futures::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use super::file_format_ffi_version;
use super::reader::FormatReader;
use super::statistics::encode_statistics;
use super::stream::{export_batch_stream, import_batch_stream};
use super::types::{FFI_ScanRequest, FFI_SortKey, FFI_WriteRequest, ScanRequest, WriteRequest};
use crate::object_store::{FFI_ObjectMeta, FFI_ObjectStore};
use crate::panic_safe::panic_message;

/// A configured file format ([`FormatReader`]) as FFI data.
///
/// The exporter builds it with [`FFI_FileFormat::new`]. The receiver uses it through
/// [`super::ForeignFormatReader`]. Only the FFI functions read or change these fields.
#[repr(C)]
pub struct FFI_FileFormat {
    /// Returns [`super::FILE_FORMAT_FFI_VERSION`] of the exporter. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// [`FormatReader::infer_schema`].
    pub infer_schema: unsafe extern "C" fn(
        format: &Self,
        store: FFI_ObjectStore,
        object: FFI_ObjectMeta,
    ) -> FfiFuture<FFI_Result<WrappedSchema>>,
    /// [`FormatReader::infer_stats`]. The bytes are `datafusion_proto_common::Statistics`.
    pub infer_stats: unsafe extern "C" fn(
        format: &Self,
        store: FFI_ObjectStore,
        object: FFI_ObjectMeta,
        table_schema: WrappedSchema,
    ) -> FfiFuture<FFI_Result<FFI_Option<SVec<u8>>>>,
    /// [`FormatReader::open`].
    pub open: unsafe extern "C" fn(
        format: &Self,
        store: FFI_ObjectStore,
        object: FFI_ObjectMeta,
        request: FFI_ScanRequest,
    ) -> FfiFuture<FFI_Result<FFI_RecordBatchStream>>,
    /// [`FormatReader::write_ordering`].
    pub write_ordering:
        unsafe extern "C" fn(format: &Self, schema: WrappedSchema) -> FFI_Result<SVec<FFI_SortKey>>,
    /// [`FormatReader::write`].
    pub write: unsafe extern "C" fn(
        format: &Self,
        store: FFI_ObjectStore,
        request: FFI_WriteRequest,
        input: FFI_RecordBatchStream,
    ) -> FfiFuture<FFI_Result<u64>>,
    /// Makes a new table over the same format.
    pub clone: unsafe extern "C" fn(format: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(format: &mut Self),
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
}

// The format is `Send + Sync`, and the functions take `&Self`.
unsafe impl Send for FFI_FileFormat {}
unsafe impl Sync for FFI_FileFormat {}

struct FormatPrivateData {
    reader: Arc<dyn FormatReader>,
}

impl FFI_FileFormat {
    /// Exports `reader`.
    pub fn new(reader: Arc<dyn FormatReader>) -> Self {
        let private_data = Box::new(FormatPrivateData { reader });
        Self {
            version: file_format_ffi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            infer_schema: infer_schema_fn,
            infer_stats: infer_stats_fn,
            open: open_fn,
            write_ordering: write_ordering_fn,
            write: write_fn,
            clone: clone_fn,
            release: release_fn,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    fn reader(&self) -> Arc<dyn FormatReader> {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        let data = unsafe { &*(self.private_data as *const FormatPrivateData) };
        Arc::clone(&data.reader)
    }

    /// Returns the inner reader. Only valid when this library made the table.
    pub(crate) fn local_reader(&self) -> Arc<dyn FormatReader> {
        self.reader()
    }
}

impl Clone for FFI_FileFormat {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_FileFormat {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

/// The error text of a panic in a file format call.
pub(crate) fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    format!("panic in file format call: {}", panic_message(payload))
}

/// Runs `work` and swallows a panic in it.
pub(crate) fn drop_quietly(work: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(work)) {
        // The payload drop can panic too.
        std::mem::forget(payload);
    }
}

/// An exported future: a panic in a poll or in the drop is caught.
struct SafeFuture<T, U> {
    /// `None` after the end.
    inner: Option<BoxFuture<'static, Result<T>>>,
    map: fn(T) -> U,
}

impl<T, U> Future for SafeFuture<T, U> {
    type Output = FFI_Result<U>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(FFI_Result::Err(SString::from(
                "file format future polled after its end",
            )));
        };
        let map = this.map;
        let polled = catch_unwind(AssertUnwindSafe(|| match inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(FFI_Result::Ok(map(value))),
            Poll::Ready(Err(error)) => {
                Poll::Ready(FFI_Result::Err(SString::from(error.to_string().as_str())))
            }
        }));
        let output = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => output,
            Err(payload) => FFI_Result::Err(SString::from(panic_text(payload.as_ref()).as_str())),
        };
        let inner = this.inner.take();
        drop_quietly(move || drop(inner));
        Poll::Ready(output)
    }
}

impl<T, U> Drop for SafeFuture<T, U> {
    fn drop(&mut self) {
        let inner = self.inner.take();
        drop_quietly(move || drop(inner));
    }
}

/// Exports the future that `make` builds. A panic in `make` gives a ready error future.
fn export_future<T, U, F>(make: F, map: fn(T) -> U) -> FfiFuture<FFI_Result<U>>
where
    T: Send + 'static,
    U: Send + 'static,
    F: FnOnce() -> BoxFuture<'static, Result<T>>,
{
    match catch_unwind(AssertUnwindSafe(make)) {
        Ok(inner) => SafeFuture {
            inner: Some(inner),
            map,
        }
        .into_ffi(),
        Err(payload) => ready(FFI_Result::Err(SString::from(
            panic_text(payload.as_ref()).as_str(),
        )))
        .into_ffi(),
    }
}

fn error_future<U: Send + 'static>(error: DataFusionError) -> FfiFuture<FFI_Result<U>> {
    ready(FFI_Result::Err(SString::from(error.to_string().as_str()))).into_ffi()
}

/// Imports the store and the object of one call.
fn call_inputs(
    store: FFI_ObjectStore,
    object: FFI_ObjectMeta,
) -> Result<(Arc<dyn ObjectStore>, ObjectMeta)> {
    let store = Arc::<dyn ObjectStore>::try_from(store)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let object =
        ObjectMeta::try_from(object).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok((store, object))
}

unsafe extern "C" fn infer_schema_fn(
    format: &FFI_FileFormat,
    store: FFI_ObjectStore,
    object: FFI_ObjectMeta,
) -> FfiFuture<FFI_Result<WrappedSchema>> {
    let (store, object) = match call_inputs(store, object) {
        Ok(inputs) => inputs,
        Err(error) => return error_future(error),
    };
    export_future(
        || {
            let reader = format.reader();
            Box::pin(async move { reader.infer_schema(store, &object).await })
        },
        WrappedSchema::from,
    )
}

unsafe extern "C" fn infer_stats_fn(
    format: &FFI_FileFormat,
    store: FFI_ObjectStore,
    object: FFI_ObjectMeta,
    table_schema: WrappedSchema,
) -> FfiFuture<FFI_Result<FFI_Option<SVec<u8>>>> {
    let (store, object) = match call_inputs(store, object) {
        Ok(inputs) => inputs,
        Err(error) => return error_future(error),
    };
    let table_schema = SchemaRef::from(table_schema);
    export_future(
        || {
            let reader = format.reader();
            Box::pin(async move { reader.infer_stats(store, &object, table_schema).await })
        },
        |statistics| statistics.as_ref().map(encode_statistics).into(),
    )
}

unsafe extern "C" fn open_fn(
    format: &FFI_FileFormat,
    store: FFI_ObjectStore,
    object: FFI_ObjectMeta,
    request: FFI_ScanRequest,
) -> FfiFuture<FFI_Result<FFI_RecordBatchStream>> {
    let (store, object) = match call_inputs(store, object) {
        Ok(inputs) => inputs,
        Err(error) => return error_future(error),
    };
    export_future(
        || {
            let reader = format.reader();
            let request = ScanRequest::from(request);
            Box::pin(async move { reader.open(store, &object, request).await })
        },
        export_batch_stream,
    )
}

unsafe extern "C" fn write_ordering_fn(
    format: &FFI_FileFormat,
    schema: WrappedSchema,
) -> FFI_Result<SVec<FFI_SortKey>> {
    let called = catch_unwind(AssertUnwindSafe(|| {
        format.reader().write_ordering(SchemaRef::from(schema))
    }));
    match called {
        Ok(Ok(keys)) => {
            let mut out = SVec::new();
            for key in &keys {
                out.push(FFI_SortKey::from(key));
            }
            FFI_Result::Ok(out)
        }
        Ok(Err(error)) => FFI_Result::Err(SString::from(error.to_string().as_str())),
        Err(payload) => FFI_Result::Err(SString::from(panic_text(payload.as_ref()).as_str())),
    }
}

unsafe extern "C" fn write_fn(
    format: &FFI_FileFormat,
    store: FFI_ObjectStore,
    request: FFI_WriteRequest,
    input: FFI_RecordBatchStream,
) -> FfiFuture<FFI_Result<u64>> {
    let store = match Arc::<dyn ObjectStore>::try_from(store) {
        Ok(store) => store,
        Err(error) => return error_future(DataFusionError::External(Box::new(error))),
    };
    export_future(
        || {
            let reader = format.reader();
            let request = WriteRequest::from(request);
            let input = import_batch_stream(input);
            Box::pin(async move { reader.write(store, request, input).await })
        },
        |rows| rows,
    )
}

unsafe extern "C" fn clone_fn(format: &FFI_FileFormat) -> FFI_FileFormat {
    let cloned = catch_unwind(AssertUnwindSafe(|| FFI_FileFormat::new(format.reader())));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(format: &mut FFI_FileFormat) {
    if format.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(format.private_data as *mut FormatPrivateData) };
    format.private_data = std::ptr::null_mut();
    drop_quietly(move || drop(data));
}
