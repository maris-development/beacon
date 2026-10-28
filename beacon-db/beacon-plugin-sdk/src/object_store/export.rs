//! The exporter side: an `Arc<dyn ObjectStore>` as an [`FFI_ObjectStore`] table.

use std::ffi::c_void;
use std::future::{Future, ready};
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ::bytes::Bytes;
use ::object_store::{Error, ObjectMeta, ObjectStore, PutPayload, Result};
use async_ffi::{FfiFuture, FutureExt as _};
use datafusion_ffi::ffi_option::FFI_Option;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, stream};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use super::bytes::FFI_Bytes;
use super::error::{FFI_ObjectStoreResult, STORE, panic_error};
use super::multipart::FFI_MultipartUpload;
use super::options::{
    FFI_CopyOptions, FFI_GetOptions, FFI_GetResultMeta, FFI_ListResult, FFI_ObjectMeta,
    FFI_PutMultipartOptions, FFI_PutOptions, FFI_PutResult, FFI_Range, FFI_RenameOptions,
    parse_path,
};
use super::stream::{FFI_BytesStream, FFI_MetaStream, FFI_PathStream, FFI_Stream};
use crate::sdk_abi_version;

/// The result of a get as FFI data.
#[repr(C)]
pub struct FFI_GetResult {
    /// The metadata, range and attributes.
    pub meta: FFI_GetResultMeta,
    /// The payload chunks. A local file crosses as a stream too.
    pub payload: FFI_BytesStream,
}

/// An `Arc<dyn ObjectStore>` as an FFI table.
///
/// The exporter builds it with [`FFI_ObjectStore::new`]. The receiver turns it into an
/// `Arc<dyn ObjectStore>` with `TryFrom`. `Clone` calls `clone`, and `Drop` calls `release`.
/// The first five fields stay at the same place in every version, so a receiver can refuse
/// and release a table of another version.
///
/// Only the FFI functions read or change these fields.
#[repr(C)]
pub struct FFI_ObjectStore {
    /// Returns the ABI version of the table. See [`crate::BEACON_PLUGIN_SDK_ABI`].
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table. See [`datafusion_ffi::get_library_marker_id`].
    pub library_marker_id: extern "C" fn() -> usize,
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
    /// Makes a new table that shares the inner store.
    pub clone: unsafe extern "C" fn(store: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(store: &mut Self),
    /// Returns the `Display` text of the inner store.
    pub display: unsafe extern "C" fn(store: &Self) -> SString,
    /// See `ObjectStore::get_opts`.
    pub get_opts: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        options: FFI_GetOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<FFI_GetResult>>,
    /// See `ObjectStore::get_ranges`.
    pub get_ranges: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        ranges: SVec<FFI_Range>,
    ) -> FfiFuture<FFI_ObjectStoreResult<SVec<FFI_Bytes>>>,
    /// See `ObjectStore::put_opts`. The payload has one item per chunk.
    pub put_opts: unsafe extern "C" fn(
        store: &Self,
        location: SString,
        payload: SVec<FFI_Bytes>,
        options: FFI_PutOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>>,
    /// See `ObjectStore::put_multipart_opts`.
    pub put_multipart_opts:
        unsafe extern "C" fn(
            store: &Self,
            location: SString,
            options: FFI_PutMultipartOptions,
        ) -> FfiFuture<FFI_ObjectStoreResult<FFI_MultipartUpload>>,
    /// See `ObjectStore::delete_stream`. The result gives the deleted paths.
    pub delete_stream:
        unsafe extern "C" fn(store: &Self, locations: FFI_PathStream) -> FFI_PathStream,
    /// See `ObjectStore::list`.
    pub list: unsafe extern "C" fn(store: &Self, prefix: FFI_Option<SString>) -> FFI_MetaStream,
    /// See `ObjectStore::list_with_offset`.
    pub list_with_offset: unsafe extern "C" fn(
        store: &Self,
        prefix: FFI_Option<SString>,
        offset: SString,
    ) -> FFI_MetaStream,
    /// See `ObjectStore::list_with_delimiter`.
    pub list_with_delimiter:
        unsafe extern "C" fn(
            store: &Self,
            prefix: FFI_Option<SString>,
        ) -> FfiFuture<FFI_ObjectStoreResult<FFI_ListResult>>,
    /// See `ObjectStore::copy_opts`.
    pub copy_opts: unsafe extern "C" fn(
        store: &Self,
        from: SString,
        to: SString,
        options: FFI_CopyOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,
    /// See `ObjectStore::rename_opts`.
    pub rename_opts: unsafe extern "C" fn(
        store: &Self,
        from: SString,
        to: SString,
        options: FFI_RenameOptions,
    ) -> FfiFuture<FFI_ObjectStoreResult<()>>,
}

// The exporter state is an `Arc<dyn ObjectStore>` and a `Handle`; both are `Send + Sync`.
unsafe impl Send for FFI_ObjectStore {}
unsafe impl Sync for FFI_ObjectStore {}

struct StorePrivateData {
    store: Arc<dyn ObjectStore>,
    handle: Handle,
}

impl FFI_ObjectStore {
    /// Exports `store`. Each poll of an exported future or stream runs inside `handle.enter()`.
    pub fn new(store: Arc<dyn ObjectStore>, handle: Handle) -> Self {
        let private_data = Box::new(StorePrivateData { store, handle });
        Self {
            version: sdk_abi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn,
            release: release_fn,
            display: display_fn,
            get_opts: get_opts_fn,
            get_ranges: get_ranges_fn,
            put_opts: put_opts_fn,
            put_multipart_opts: put_multipart_opts_fn,
            delete_stream: delete_stream_fn,
            list: list_fn,
            list_with_offset: list_with_offset_fn,
            list_with_delimiter: list_with_delimiter_fn,
            copy_opts: copy_opts_fn,
            rename_opts: rename_opts_fn,
        }
    }

    fn data(&self) -> &StorePrivateData {
        // SAFETY: `new` or `clone_fn` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const StorePrivateData) }
    }

    /// Returns the inner store. Only valid when this library made the table.
    pub(crate) fn local_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.data().store)
    }
}

impl Clone for FFI_ObjectStore {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_ObjectStore {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

unsafe extern "C" fn clone_fn(store: &FFI_ObjectStore) -> FFI_ObjectStore {
    let cloned = catch_unwind(AssertUnwindSafe(|| {
        let data = store.data();
        FFI_ObjectStore::new(Arc::clone(&data.store), data.handle.clone())
    }));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(store: &mut FFI_ObjectStore) {
    if store.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(store.private_data as *mut StorePrivateData) };
    store.private_data = std::ptr::null_mut();
    drop_quietly(move || {
        let handle = data.handle.clone();
        let _guard = handle.enter();
        drop(data);
    });
}

unsafe extern "C" fn display_fn(store: &FFI_ObjectStore) -> SString {
    catch_unwind(AssertUnwindSafe(|| {
        SString::from(store.data().store.to_string().as_str())
    }))
    .unwrap_or_else(|payload| panic_error(payload).message)
}

/// Runs `drop_work` and swallows a panic in it.
pub(super) fn drop_quietly(drop_work: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(drop_work)) {
        // The payload drop can panic too.
        std::mem::forget(payload);
    }
}

/// An exported future: each poll and the drop run inside the handle, and a panic is an error.
struct EnteredFuture<T> {
    /// `None` after the end.
    inner: Option<BoxFuture<'static, Result<T>>>,
    handle: Handle,
}

impl<T> Future for EnteredFuture<T> {
    type Output = FFI_ObjectStoreResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            let error = Error::Generic {
                store: STORE,
                source: "object store future polled after its end".into(),
            };
            return Poll::Ready(FFI_ObjectStoreResult::Err(error.into()));
        };
        let _guard = this.handle.enter();
        // The result conversion formats the error source, so it runs under the guard too.
        let polled = catch_unwind(AssertUnwindSafe(|| match inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(FFI_ObjectStoreResult::from(result)),
        }));
        let output = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => output,
            Err(payload) => FFI_ObjectStoreResult::Err(panic_error(payload)),
        };
        let inner = this.inner.take();
        drop_quietly(move || drop(inner));
        Poll::Ready(output)
    }
}

impl<T> Drop for EnteredFuture<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _guard = self.handle.enter();
            drop_quietly(move || drop(inner));
        }
    }
}

/// Exports the future that `make` builds, with the handle from `handle`.
///
/// `make` runs inside the handle. A panic in either closure gives a ready error future.
pub(super) fn export_entered<T, H, F>(handle: H, make: F) -> FfiFuture<FFI_ObjectStoreResult<T>>
where
    T: Send + 'static,
    H: FnOnce() -> Handle,
    F: FnOnce() -> BoxFuture<'static, Result<T>>,
{
    let made = catch_unwind(AssertUnwindSafe(|| {
        let handle = handle();
        let inner = {
            let _guard = handle.enter();
            make()
        };
        EnteredFuture {
            inner: Some(inner),
            handle,
        }
        .into_ffi()
    }));
    made.unwrap_or_else(|payload| {
        ready(FFI_ObjectStoreResult::Err(panic_error(payload))).into_ffi()
    })
}

/// Exports the future that `make` builds from the inner store and the handle.
fn export_future<T, F>(store: &FFI_ObjectStore, make: F) -> FfiFuture<FFI_ObjectStoreResult<T>>
where
    T: Send + 'static,
    F: FnOnce(Arc<dyn ObjectStore>, Handle) -> BoxFuture<'static, Result<T>>,
{
    export_entered(
        || store.data().handle.clone(),
        || {
            let data = store.data();
            make(Arc::clone(&data.store), data.handle.clone())
        },
    )
}

/// Exports the listing that `make` builds from the inner store.
fn export_list<F>(store: &FFI_ObjectStore, make: F) -> FFI_MetaStream
where
    F: FnOnce(&dyn ObjectStore) -> Result<BoxStream<'static, Result<ObjectMeta>>>,
{
    let made = catch_unwind(AssertUnwindSafe(|| {
        let data = store.data();
        // A store can spawn a task when it starts a listing.
        let _guard = data.handle.enter();
        let listing = make(data.store.as_ref()).unwrap_or_else(|error| error_stream(error));
        FFI_MetaStream::new(listing, data.handle.clone(), |meta| {
            FFI_ObjectMeta::try_from(&meta)
        })
    }));
    made.unwrap_or_else(|payload| {
        let error = Error::from(panic_error(payload));
        FFI_Stream::new_unentered(error_stream::<FFI_ObjectMeta>(error), Ok)
    })
}

fn error_stream<T: Send + 'static>(error: Error) -> BoxStream<'static, Result<T>> {
    stream::once(ready(Err(error))).boxed()
}

fn parse_prefix(prefix: FFI_Option<SString>) -> Result<Option<::object_store::path::Path>> {
    prefix
        .into_option()
        .map(|prefix| parse_path(prefix.as_str()))
        .transpose()
}

unsafe extern "C" fn get_opts_fn(
    store: &FFI_ObjectStore,
    location: SString,
    options: FFI_GetOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_GetResult>> {
    export_future(store, move |inner, handle| {
        async move {
            let location = parse_path(location.as_str())?;
            let result = inner.get_opts(&location, options.into()).await?;
            let meta = FFI_GetResultMeta::try_from(&result)?;
            // `into_stream` reads a local file with `spawn_blocking`, so it needs the handle.
            let payload = FFI_BytesStream::new(result.into_stream(), handle, |chunk| {
                Ok(FFI_Bytes::from(chunk))
            });
            Ok(FFI_GetResult { meta, payload })
        }
        .boxed()
    })
}

unsafe extern "C" fn get_ranges_fn(
    store: &FFI_ObjectStore,
    location: SString,
    ranges: SVec<FFI_Range>,
) -> FfiFuture<FFI_ObjectStoreResult<SVec<FFI_Bytes>>> {
    export_future(store, move |inner, _| {
        async move {
            let location = parse_path(location.as_str())?;
            let ranges: Vec<Range<u64>> = ranges.iter().map(|range| (*range).into()).collect();
            let chunks = inner.get_ranges(&location, &ranges).await?;
            Ok(chunks.into_iter().map(FFI_Bytes::from).collect())
        }
        .boxed()
    })
}

unsafe extern "C" fn put_opts_fn(
    store: &FFI_ObjectStore,
    location: SString,
    payload: SVec<FFI_Bytes>,
    options: FFI_PutOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_PutResult>> {
    export_future(store, move |inner, _| {
        async move {
            let location = parse_path(location.as_str())?;
            let payload: PutPayload = payload.into_iter().map(Bytes::from).collect();
            let result = inner.put_opts(&location, payload, options.into()).await?;
            Ok(FFI_PutResult::from(&result))
        }
        .boxed()
    })
}

unsafe extern "C" fn put_multipart_opts_fn(
    store: &FFI_ObjectStore,
    location: SString,
    options: FFI_PutMultipartOptions,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_MultipartUpload>> {
    export_future(store, move |inner, handle| {
        async move {
            let location = parse_path(location.as_str())?;
            let upload = inner.put_multipart_opts(&location, options.into()).await?;
            Ok(FFI_MultipartUpload::new(upload, handle))
        }
        .boxed()
    })
}

unsafe extern "C" fn delete_stream_fn(
    store: &FFI_ObjectStore,
    locations: FFI_PathStream,
) -> FFI_PathStream {
    let made = catch_unwind(AssertUnwindSafe(|| {
        let data = store.data();
        // A store can spawn a task when it starts the deletes.
        let _guard = data.handle.enter();
        let locations = locations
            .map(|location| location.and_then(|location| parse_path(location.as_str())))
            .boxed();
        let deleted = data.store.delete_stream(locations);
        FFI_PathStream::new(deleted, data.handle.clone(), |path| {
            Ok(SString::from(path.as_ref()))
        })
    }));
    made.unwrap_or_else(|payload| {
        let error = Error::from(panic_error(payload));
        FFI_Stream::new_unentered(error_stream::<SString>(error), Ok)
    })
}

unsafe extern "C" fn list_fn(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
) -> FFI_MetaStream {
    export_list(store, move |inner| {
        let prefix = parse_prefix(prefix)?;
        Ok(inner.list(prefix.as_ref()))
    })
}

unsafe extern "C" fn list_with_offset_fn(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
    offset: SString,
) -> FFI_MetaStream {
    export_list(store, move |inner| {
        let prefix = parse_prefix(prefix)?;
        let offset = parse_path(offset.as_str())?;
        Ok(inner.list_with_offset(prefix.as_ref(), &offset))
    })
}

unsafe extern "C" fn list_with_delimiter_fn(
    store: &FFI_ObjectStore,
    prefix: FFI_Option<SString>,
) -> FfiFuture<FFI_ObjectStoreResult<FFI_ListResult>> {
    export_future(store, move |inner, _| {
        async move {
            let prefix = parse_prefix(prefix)?;
            let listed = inner.list_with_delimiter(prefix.as_ref()).await?;
            FFI_ListResult::try_from(&listed)
        }
        .boxed()
    })
}

unsafe extern "C" fn copy_opts_fn(
    store: &FFI_ObjectStore,
    from: SString,
    to: SString,
    options: FFI_CopyOptions,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    export_future(store, move |inner, _| {
        async move {
            let from = parse_path(from.as_str())?;
            let to = parse_path(to.as_str())?;
            inner.copy_opts(&from, &to, options.try_into()?).await
        }
        .boxed()
    })
}

unsafe extern "C" fn rename_opts_fn(
    store: &FFI_ObjectStore,
    from: SString,
    to: SString,
    options: FFI_RenameOptions,
) -> FfiFuture<FFI_ObjectStoreResult<()>> {
    export_future(store, move |inner, _| {
        async move {
            let from = parse_path(from.as_str())?;
            let to = parse_path(to.as_str())?;
            inner.rename_opts(&from, &to, options.try_into()?).await
        }
        .boxed()
    })
}
