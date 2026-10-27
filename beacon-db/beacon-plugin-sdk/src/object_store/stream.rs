//! Poll-based streams that cross the plugin boundary.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

use ::object_store::{Error, Result};
use async_ffi::{ContextExt, FfiContext, FfiPoll};
use datafusion_ffi::ffi_option::FFI_Option;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use stabby::string::String as SString;
use tokio::runtime::Handle;

use super::bytes::FFI_Bytes;
use super::error::{FFI_ObjectStoreResult, panic_error};
use super::options::FFI_ObjectMeta;

/// A stream of `object_store::Result<T>` items as FFI data.
///
/// The exporter builds it with [`FFI_Stream::new`] or [`FFI_Stream::new_unentered`].
/// The receiver polls it as a [`Stream`]. `Drop` calls `release` one time.
/// After the end or after a panic, each poll gives `None`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Stream<T> {
    /// Polls the exported stream for the next item.
    pub poll_next: unsafe extern "C" fn(
        stream: &Self,
        cx: &mut FfiContext,
    ) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<T>>>,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(stream: &mut Self),
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
}

/// A stream of object payload chunks.
#[allow(non_camel_case_types)]
pub type FFI_BytesStream = FFI_Stream<FFI_Bytes>;
/// A stream of object metadata.
#[allow(non_camel_case_types)]
pub type FFI_MetaStream = FFI_Stream<FFI_ObjectMeta>;
/// A stream of object paths.
#[allow(non_camel_case_types)]
pub type FFI_PathStream = FFI_Stream<SString>;

// The exported inner stream is `Send`, and only the owner polls it.
unsafe impl<T: Send> Send for FFI_Stream<T> {}

struct StreamPrivateData<T> {
    /// `None` after the end or after a panic.
    inner: Option<BoxStream<'static, Result<T>>>,
    handle: Option<Handle>,
}

impl<T: 'static> FFI_Stream<T> {
    /// Exports `stream`. Each poll runs inside `handle.enter()`.
    ///
    /// `map` converts each item. A failed conversion gives an `Err` item, and the stream continues.
    pub fn new<S, U, F>(stream: S, handle: Handle, map: F) -> Self
    where
        S: Stream<Item = Result<U>> + Send + 'static,
        F: FnMut(U) -> Result<T> + Send + 'static,
    {
        Self::export(stream, Some(handle), map)
    }

    /// Exports `stream` without a runtime handle. Each poll runs without `enter`.
    ///
    /// A receiver uses it to send a stream back to the host, because a receiver holds no handle.
    pub fn new_unentered<S, U, F>(stream: S, map: F) -> Self
    where
        S: Stream<Item = Result<U>> + Send + 'static,
        F: FnMut(U) -> Result<T> + Send + 'static,
    {
        Self::export(stream, None, map)
    }

    fn export<S, U, F>(stream: S, handle: Option<Handle>, mut map: F) -> Self
    where
        S: Stream<Item = Result<U>> + Send + 'static,
        F: FnMut(U) -> Result<T> + Send + 'static,
    {
        let inner = stream.map(move |item| item.and_then(&mut map)).boxed();
        let private_data = Box::new(StreamPrivateData {
            inner: Some(inner),
            handle,
        });
        Self {
            poll_next: poll_next_fn::<T>,
            release: release_fn::<T>,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

unsafe extern "C" fn poll_next_fn<T>(
    stream: &FFI_Stream<T>,
    cx: &mut FfiContext,
) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<T>>> {
    // SAFETY: the exporter set `private_data`, and only the owner polls, so the access is unique.
    let private_data = unsafe { &mut *(stream.private_data as *mut StreamPrivateData<T>) };
    let Some(inner) = private_data.inner.as_mut() else {
        return FfiPoll::Ready(FFI_Option::None);
    };
    let _guard = private_data.handle.as_ref().map(Handle::enter);
    let polled = catch_unwind(AssertUnwindSafe(|| {
        cx.with_context(|std_cx| inner.poll_next_unpin(std_cx))
    }));
    match polled {
        Ok(Poll::Pending) => FfiPoll::Pending,
        Ok(Poll::Ready(Some(item))) => FfiPoll::Ready(FFI_Option::Some(item.into())),
        Ok(Poll::Ready(None)) => {
            drop_inner(private_data);
            FfiPoll::Ready(FFI_Option::None)
        }
        Err(payload) => {
            drop_inner(private_data);
            FfiPoll::Ready(FFI_Option::Some(FFI_ObjectStoreResult::Err(panic_error(
                payload,
            ))))
        }
    }
}

/// Drops the inner stream now, so the host frees its resources early.
fn drop_inner<T>(private_data: &mut StreamPrivateData<T>) {
    let inner = private_data.inner.take();
    let _ = catch_unwind(AssertUnwindSafe(move || drop(inner)));
}

unsafe extern "C" fn release_fn<T>(stream: &mut FFI_Stream<T>) {
    if stream.private_data.is_null() {
        return;
    }
    // SAFETY: `export` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let private_data = unsafe { Box::from_raw(stream.private_data as *mut StreamPrivateData<T>) };
    stream.private_data = std::ptr::null_mut();
    let _ = catch_unwind(AssertUnwindSafe(move || drop(private_data)));
}

impl<T> Stream for FFI_Stream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // A null `private_data` means that the exporter state is released.
        if this.private_data.is_null() {
            return Poll::Ready(None);
        }
        // SAFETY: `private_data` is live, and `poll_next` matches it.
        let polled = cx.with_ffi_context(|ffi_cx| unsafe { (this.poll_next)(this, ffi_cx) });
        match polled {
            FfiPoll::Ready(item) => Poll::Ready(
                Option::<FFI_ObjectStoreResult<T>>::from(item)
                    .map(FFI_ObjectStoreResult::into_result),
            ),
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => {
                this.release_now();
                let error = panic_error(Box::new("the other side of the stream panicked"));
                Poll::Ready(Some(Err(Error::from(error))))
            }
        }
    }
}

impl<T> FFI_Stream<T> {
    fn release_now(&mut self) {
        if !self.private_data.is_null() {
            // SAFETY: `private_data` is live, and the null check stops a second release.
            unsafe { (self.release)(self) };
            self.private_data = std::ptr::null_mut();
        }
    }
}

impl<T> Drop for FFI_Stream<T> {
    fn drop(&mut self) {
        self.release_now();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use ::object_store::Error;
    use futures::{StreamExt, stream};
    use tokio::runtime::Handle;

    use super::*;

    fn ok_item(value: u32) -> ::object_store::Result<u32> {
        Ok(value)
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stream_round_trip() {
        let inner = stream::iter([1u32, 2, 3]).map(Ok);
        let ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        let items: Vec<u32> = ffi.map(|item| item.unwrap()).collect().await;
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn stream_error_keeps_kind() {
        let inner = stream::iter([Err(Error::NotFound {
            path: "a/b".to_string(),
            source: "gone".into(),
        })]);
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        let item = ffi.next().await.unwrap();
        assert!(matches!(item, Err(Error::NotFound { ref path, .. }) if path == "a/b"));
        assert!(ffi.next().await.is_none());
    }

    #[tokio::test]
    async fn item_conversion_error_is_error_item() {
        let inner = stream::iter([1u32, 2]).map(Ok);
        let map = |value: u32| {
            if value == 1 {
                Err(Error::NotSupported {
                    source: "odd".into(),
                })
            } else {
                Ok(value)
            }
        };
        let ffi = FFI_Stream::<u32>::new(inner, Handle::current(), map);
        let items: Vec<_> = ffi.collect().await;
        assert!(matches!(items[0], Err(Error::NotSupported { .. })));
        assert_eq!(items[1].as_ref().unwrap(), &2);
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn polled_after_end_stays_none() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = polls.clone();
        let mut values = vec![2u32, 1];
        let inner = stream::poll_fn(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(values.pop().map(Ok))
        });
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        assert_eq!(ffi.next().await.unwrap().unwrap(), 1);
        assert_eq!(ffi.next().await.unwrap().unwrap(), 2);
        assert!(ffi.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 3);
        assert!(ffi.next().await.is_none());
        assert!(ffi.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn end_releases_inner_before_drop() {
        let drops = Arc::new(AtomicUsize::new(0));
        let guard = DropCounter(drops.clone());
        let inner = stream::iter([1u32]).map(move |value| {
            let _ = &guard;
            Ok(value)
        });
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        assert_eq!(ffi.next().await.unwrap().unwrap(), 1);
        assert!(ffi.next().await.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(ffi);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drop_mid_stream_releases_inner() {
        let drops = Arc::new(AtomicUsize::new(0));
        let guard = DropCounter(drops.clone());
        let inner = stream::iter(0u32..1000).map(move |value| {
            let _ = &guard;
            Ok(value)
        });
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        assert_eq!(ffi.next().await.unwrap().unwrap(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(ffi);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn panic_in_inner_stream_is_error() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = polls.clone();
        // `resume_unwind` skips the panic hook, so the test output stays clean.
        let inner = stream::poll_fn(move |_| -> Poll<Option<::object_store::Result<u32>>> {
            counter.fetch_add(1, Ordering::SeqCst);
            std::panic::resume_unwind(Box::new("boom"))
        });
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), ok_item);
        let item = ffi.next().await.unwrap();
        match item {
            Err(error @ Error::Generic { .. }) => {
                assert!(
                    error
                        .to_string()
                        .contains("panic in object store call: boom")
                );
            }
            other => panic!("unexpected item {other:?}"),
        }
        assert!(ffi.next().await.is_none());
        assert!(ffi.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn panic_in_item_conversion_is_error() {
        let inner = stream::iter([1u32]).map(Ok);
        let map = |_: u32| -> ::object_store::Result<u32> {
            std::panic::resume_unwind(Box::new("bad item"))
        };
        let mut ffi = FFI_Stream::<u32>::new(inner, Handle::current(), map);
        let item = ffi.next().await.unwrap();
        assert!(matches!(item, Err(Error::Generic { .. })));
        assert!(ffi.next().await.is_none());
    }

    unsafe extern "C" fn panicked_poll(
        _: &FFI_Stream<u32>,
        _: &mut FfiContext,
    ) -> FfiPoll<FFI_Option<FFI_ObjectStoreResult<u32>>> {
        FfiPoll::Panicked
    }

    unsafe extern "C" fn count_release(stream: &mut FFI_Stream<u32>) {
        let count = unsafe { Arc::from_raw(stream.private_data as *const AtomicUsize) };
        count.fetch_add(1, Ordering::SeqCst);
        stream.private_data = std::ptr::null_mut();
    }

    #[tokio::test]
    async fn foreign_panicked_poll_is_error() {
        let releases = Arc::new(AtomicUsize::new(0));
        let mut ffi = FFI_Stream::<u32> {
            poll_next: panicked_poll,
            release: count_release,
            private_data: Arc::into_raw(releases.clone()) as *mut c_void,
        };
        let item = ffi.next().await.unwrap();
        match item {
            Err(error @ Error::Generic { .. }) => {
                assert!(error.to_string().contains("panic in object store call:"));
            }
            other => panic!("unexpected item {other:?}"),
        }
        assert!(ffi.next().await.is_none());
        drop(ffi);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn poll_enters_handle() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        let inner = stream::once(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok(7u32)
        });
        let ffi = FFI_Stream::<u32>::new(inner, runtime.handle().clone(), ok_item);
        let items = std::thread::spawn(move || {
            futures::executor::block_on(ffi.map(|item| item.unwrap()).collect::<Vec<_>>())
        })
        .join()
        .unwrap();
        assert_eq!(items, vec![7]);
    }

    #[test]
    fn new_unentered_round_trip() {
        let inner = stream::iter([4u32, 5]).map(Ok);
        let ffi = FFI_Stream::<u32>::new_unentered(inner, ok_item);
        let items: Vec<u32> = futures::executor::block_on(ffi.map(|item| item.unwrap()).collect());
        assert_eq!(items, vec![4, 5]);
    }

    #[test]
    fn stream_aliases_are_send() {
        fn assert_send<S: Send>() {}
        assert_send::<FFI_BytesStream>();
        assert_send::<FFI_MetaStream>();
        assert_send::<FFI_PathStream>();
    }
}
