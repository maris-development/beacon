//! Round trips of `FFI_ObjectStore` through the forced foreign path.

use std::fmt;
use std::mem::discriminant;
use std::sync::Arc;

use ::bytes::Bytes;
use ::object_store::memory::InMemory;
use ::object_store::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ::object_store::{
    Attributes, CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, RenameTargetMode,
    Result, UpdateVersion, UploadPart,
};
use async_trait::async_trait;
use datafusion_ffi::get_library_marker_id;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use tokio::runtime::Handle;

use super::*;

extern "C" fn foreign_marker_id() -> usize {
    get_library_marker_id() + 1
}

extern "C" fn other_version() -> u64 {
    2
}

/// Exports `store` and imports it through the foreign path.
pub(crate) fn foreign(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    let mut ffi = FFI_ObjectStore::new(store, Handle::current());
    ffi.library_marker_id = foreign_marker_id;
    Arc::<dyn ObjectStore>::try_from(ffi).expect("import the store")
}

const DATA: &[u8] = b"0123456789";

async fn seeded() -> Arc<dyn ObjectStore> {
    let store = InMemory::new();
    for (path, data) in [
        ("a/1.nc", DATA),
        ("a/b/2.nc", b"two".as_slice()),
        ("a/c/3.nc", b"three".as_slice()),
        ("d/4.nc", b"four".as_slice()),
        ("empty.nc", b"".as_slice()),
    ] {
        store
            .put(&Path::from(path), PutPayload::from_static(data))
            .await
            .unwrap();
    }
    Arc::new(store)
}

async fn bytes_and_meta(
    result: GetResult,
) -> (Bytes, ObjectMeta, std::ops::Range<u64>, Attributes) {
    let meta = result.meta.clone();
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    (result.bytes().await.unwrap(), meta, range, attributes)
}

async fn sorted_list(stream: BoxStream<'static, Result<ObjectMeta>>) -> Vec<ObjectMeta> {
    let mut list: Vec<ObjectMeta> = stream.try_collect().await.unwrap();
    list.sort_by(|a, b| a.location.cmp(&b.location));
    list
}

fn assert_list_result_eq(left: ListResult, right: ListResult) {
    assert_eq!(left.common_prefixes, right.common_prefixes);
    assert_eq!(left.objects, right.objects);
}

#[test]
fn object_store_ffi_version_is_one() {
    assert_eq!(object_store_ffi_version(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_marker_returns_original_arc() {
    let store = seeded().await;
    let ffi = FFI_ObjectStore::new(store.clone(), Handle::current());
    let back = Arc::<dyn ObjectStore>::try_from(ffi).unwrap();
    assert!(Arc::ptr_eq(&store, &back));
    // The import frees the table, so only `store` and `back` remain.
    assert_eq!(Arc::strong_count(&store), 2);
    drop(back);
    assert_eq!(Arc::strong_count(&store), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_version_is_refused() {
    let store = seeded().await;
    let mut ffi = FFI_ObjectStore::new(store.clone(), Handle::current());
    ffi.library_marker_id = foreign_marker_id;
    ffi.version = other_version;
    let error = Arc::<dyn ObjectStore>::try_from(ffi).unwrap_err();
    assert_eq!(Arc::strong_count(&store), 1);
    assert!(matches!(error, Error::NotSupported { .. }));
    assert!(
        error
            .to_string()
            .contains("object store FFI version 2 is not supported (want 1)")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_matches_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/1.nc");
    let direct = bytes_and_meta(store.get(&path).await.unwrap()).await;
    let crossed = bytes_and_meta(remote.get(&path).await.unwrap()).await;
    assert_eq!(crossed, direct);
    assert_eq!(&crossed.0[..], DATA);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_each_range_kind() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/1.nc");
    for range in [
        GetRange::Bounded(2..5),
        GetRange::Offset(3),
        GetRange::Suffix(2),
    ] {
        let options = || GetOptions {
            range: Some(range.clone()),
            ..Default::default()
        };
        let direct = bytes_and_meta(store.get_opts(&path, options()).await.unwrap()).await;
        let crossed = bytes_and_meta(remote.get_opts(&path, options()).await.unwrap()).await;
        assert_eq!(crossed, direct, "range {range:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn head_matches_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/b/2.nc");
    assert_eq!(
        remote.head(&path).await.unwrap(),
        store.head(&path).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn get_ranges_matches_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/1.nc");
    let ranges = [0..2, 4..6];
    let direct = store.get_ranges(&path, &ranges).await.unwrap();
    let crossed = remote.get_ranges(&path, &ranges).await.unwrap();
    assert_eq!(crossed, direct);
    assert_eq!(
        crossed,
        vec![Bytes::from_static(b"01"), Bytes::from_static(b"45")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_get_errors() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/1.nc");
    let e_tag = store.head(&path).await.unwrap().e_tag;
    assert!(e_tag.is_some());

    let options = GetOptions {
        if_none_match: e_tag,
        ..Default::default()
    };
    let error = remote.get_opts(&path, options).await.unwrap_err();
    assert!(matches!(error, Error::NotModified { .. }), "{error:?}");

    let options = GetOptions {
        if_match: Some("\"nope\"".to_string()),
        ..Default::default()
    };
    let error = remote.get_opts(&path, options).await.unwrap_err();
    assert!(matches!(error, Error::Precondition { .. }), "{error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_missing_is_not_found() {
    let remote = foreign(seeded().await);
    let error = remote.get(&Path::from("missing.nc")).await.unwrap_err();
    assert!(
        matches!(&error, Error::NotFound { path, .. } if path == "missing.nc"),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_object_get() {
    let remote = foreign(seeded().await);
    let result = remote.get(&Path::from("empty.nc")).await.unwrap();
    assert_eq!(result.meta.size, 0);
    assert_eq!(result.range, 0..0);
    assert!(result.bytes().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn range_past_end_matches_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("a/1.nc");
    let options = || GetOptions {
        range: Some(GetRange::Bounded(20..30)),
        ..Default::default()
    };
    let direct = store.get_opts(&path, options()).await.unwrap_err();
    let crossed = remote.get_opts(&path, options()).await.unwrap_err();
    assert_eq!(discriminant(&crossed), discriminant(&direct), "{crossed:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_and_offset_match_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let prefix = Path::from("a");
    assert_eq!(
        sorted_list(remote.list(None)).await,
        sorted_list(store.list(None)).await
    );
    assert_eq!(
        sorted_list(remote.list(Some(&prefix))).await,
        sorted_list(store.list(Some(&prefix))).await
    );
    let offset = Path::from("a/b/2.nc");
    let crossed = sorted_list(remote.list_with_offset(Some(&prefix), &offset)).await;
    let direct = sorted_list(store.list_with_offset(Some(&prefix), &offset)).await;
    assert_eq!(crossed, direct);
    assert_eq!(crossed.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_with_delimiter_matches_direct() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    assert_list_result_eq(
        remote.list_with_delimiter(None).await.unwrap(),
        store.list_with_delimiter(None).await.unwrap(),
    );
    let prefix = Path::from("a");
    let crossed = remote.list_with_delimiter(Some(&prefix)).await.unwrap();
    assert_eq!(crossed.common_prefixes.len(), 2);
    assert_list_result_eq(
        crossed,
        store.list_with_delimiter(Some(&prefix)).await.unwrap(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn display_shows_inner_display() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    assert_eq!(remote.to_string(), store.to_string());
    assert_eq!(
        format!("{remote:?}"),
        format!("ForeignObjectStore({store})")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_gets_share_one_store() {
    let store = InMemory::new();
    for i in 0..16 {
        let data = Bytes::from(format!("object {i}"));
        store
            .put(&Path::from(format!("p/{i}")), data.into())
            .await
            .unwrap();
    }
    let remote = foreign(Arc::new(store));
    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let remote = remote.clone();
            tokio::spawn(async move {
                let path = Path::from(format!("p/{i}"));
                let bytes = remote.get(&path).await.unwrap().bytes().await.unwrap();
                (i, bytes)
            })
        })
        .collect();
    for task in tasks {
        let (i, bytes) = task.await.unwrap();
        assert_eq!(bytes, Bytes::from(format!("object {i}")));
    }
}

/// A store whose `get_opts` yields one known chunk.
///
/// `put_opts` records the pointer of the first payload chunk. Its uploads count `abort` calls.
#[derive(Debug, Default)]
struct ChunkStore {
    chunk: Bytes,
    put_pointer: Mutex<Option<usize>>,
    aborts: Arc<AtomicUsize>,
    completes: Arc<AtomicUsize>,
    /// Each part waits until this many parts are in flight.
    parts_in_flight: usize,
    started_parts: Arc<AtomicUsize>,
    /// Makes `complete` of each upload fail.
    fail_complete: bool,
    upload_drops: Arc<AtomicUsize>,
}

impl ChunkStore {
    fn with_chunk(chunk: Bytes) -> Self {
        Self {
            chunk,
            ..Default::default()
        }
    }
}

/// An upload that counts its calls.
#[derive(Debug)]
struct CountingUpload {
    aborts: Arc<AtomicUsize>,
    completes: Arc<AtomicUsize>,
    parts_in_flight: usize,
    started_parts: Arc<AtomicUsize>,
    fail_complete: bool,
    drops: Arc<AtomicUsize>,
}

impl Drop for CountingUpload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, _: PutPayload) -> UploadPart {
        let started = Arc::clone(&self.started_parts);
        let wanted = self.parts_in_flight;
        async move {
            started.fetch_add(1, Ordering::SeqCst);
            while started.load(Ordering::SeqCst) < wanted {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.completes.fetch_add(1, Ordering::SeqCst);
        if self.fail_complete {
            return Err(Error::Generic {
                store: "ChunkStore",
                source: "complete fails".into(),
            });
        }
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl fmt::Display for ChunkStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkStore")
    }
}

fn not_implemented(operation: &str) -> Error {
    Error::NotImplemented {
        operation: operation.to_string(),
        implementer: "ChunkStore".to_string(),
    }
}

#[async_trait]
impl ObjectStore for ChunkStore {
    async fn put_opts(&self, _: &Path, payload: PutPayload, _: PutOptions) -> Result<PutResult> {
        let first = payload.iter().next().map(|chunk| chunk.as_ptr() as usize);
        *self.put_pointer.lock().unwrap() = first;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(CountingUpload {
            aborts: Arc::clone(&self.aborts),
            completes: Arc::clone(&self.completes),
            parts_in_flight: self.parts_in_flight,
            started_parts: Arc::clone(&self.started_parts),
            fail_complete: self.fail_complete,
            drops: Arc::clone(&self.upload_drops),
        }))
    }

    async fn get_opts(&self, location: &Path, _: GetOptions) -> Result<GetResult> {
        let size = self.chunk.len() as u64;
        let chunk = self.chunk.clone();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(chunk) }).boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
            attributes: Attributes::new(),
        })
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        stream::once(async { Err(not_implemented("delete_stream")) }).boxed()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        stream::once(async { Err(not_implemented("list")) }).boxed()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(not_implemented("list_with_delimiter"))
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(not_implemented("copy_opts"))
    }

    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        Err(not_implemented("rename_opts"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn get_chunk_keeps_pointer() {
    let chunk = Bytes::from(vec![7u8; 64]);
    let pointer = chunk.as_ptr();
    let remote = foreign(Arc::new(ChunkStore::with_chunk(chunk)));
    let result = remote.get(&Path::from("x.nc")).await.unwrap();
    let GetResultPayload::Stream(mut chunks) = result.payload else {
        panic!("expected a stream payload");
    };
    let received = chunks.next().await.unwrap().unwrap();
    assert_eq!(received.as_ptr(), pointer);
    assert_eq!(received.len(), 64);
    assert!(chunks.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn put_then_get() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("new/x.nc");
    let payload = PutPayload::from_iter([Bytes::from_static(b"abc"), Bytes::from_static(b"def")]);
    let put = remote.put(&path, payload).await.unwrap();
    let direct = store.head(&path).await.unwrap();
    assert_eq!(put.e_tag, direct.e_tag);
    let crossed = remote.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&crossed[..], b"abcdef");
}

#[tokio::test(flavor = "multi_thread")]
async fn put_create_mode_on_existing_is_already_exists() {
    let remote = foreign(seeded().await);
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let error = remote
        .put_opts(
            &Path::from("a/1.nc"),
            PutPayload::from_static(b"x"),
            options,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AlreadyExists { .. }), "{error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn put_update_mode_with_stale_etag_is_precondition() {
    let remote = foreign(seeded().await);
    let options = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some("stale".to_string()),
            version: None,
        }),
        ..Default::default()
    };
    let error = remote
        .put_opts(
            &Path::from("a/1.nc"),
            PutPayload::from_static(b"x"),
            options,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Precondition { .. }), "{error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn put_payload_keeps_pointer() {
    let store = Arc::new(ChunkStore::default());
    let remote = foreign(store.clone());
    let chunk = Bytes::from(vec![3u8; 64]);
    let pointer = chunk.as_ptr() as usize;
    remote
        .put(&Path::from("x.nc"), PutPayload::from(chunk))
        .await
        .unwrap();
    assert_eq!(*store.put_pointer.lock().unwrap(), Some(pointer));
}

#[tokio::test(flavor = "multi_thread")]
async fn multipart_put_complete_read_back() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let path = Path::from("big.nc");
    let parts = [
        Bytes::from(vec![1u8; 5 * 1024 * 1024]),
        Bytes::from(vec![2u8; 5 * 1024 * 1024]),
        Bytes::from_static(b"3"),
    ];
    let mut upload = remote.put_multipart(&path).await.unwrap();
    // All parts start before any of them is awaited.
    let futures: Vec<_> = parts
        .iter()
        .map(|part| upload.put_part(PutPayload::from(part.clone())))
        .collect();
    futures::future::try_join_all(futures).await.unwrap();
    upload.complete().await.unwrap();
    let back = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(back, parts.concat());
}

#[tokio::test(flavor = "multi_thread")]
async fn multipart_parts_run_concurrently() {
    let store = Arc::new(ChunkStore {
        parts_in_flight: 3,
        ..Default::default()
    });
    let remote = foreign(store.clone());
    let mut upload = remote.put_multipart(&Path::from("x.nc")).await.unwrap();
    // Each part waits until all 3 are in flight, so serial parts would hang.
    let futures: Vec<_> = (0..3)
        .map(|_| upload.put_part(PutPayload::from_static(b"p")))
        .collect();
    let joined = futures::future::try_join_all(futures);
    tokio::time::timeout(Duration::from_secs(5), joined)
        .await
        .expect("parts run concurrently")
        .unwrap();
    upload.complete().await.unwrap();
    assert_eq!(store.completes.load(Ordering::SeqCst), 1);
}

async fn wait_for_count(count: &AtomicUsize, wanted: usize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(5);
    while count.load(Ordering::SeqCst) < wanted && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    count.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_multipart_aborts() {
    let store = Arc::new(ChunkStore::default());
    let remote = foreign(store.clone());
    let mut upload = remote.put_multipart(&Path::from("x.nc")).await.unwrap();
    upload
        .put_part(PutPayload::from_static(b"part"))
        .await
        .unwrap();
    drop(upload);
    assert_eq!(wait_for_count(&store.aborts, 1).await, 1);
    // No second abort comes later.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_or_aborted_multipart_does_not_abort_on_drop() {
    let store = Arc::new(ChunkStore::default());
    let remote = foreign(store.clone());

    let mut upload = remote.put_multipart(&Path::from("x.nc")).await.unwrap();
    upload.complete().await.unwrap();
    drop(upload);

    let mut upload = remote.put_multipart(&Path::from("y.nc")).await.unwrap();
    upload.abort().await.unwrap();
    drop(upload);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(store.completes.load(Ordering::SeqCst), 1);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_complete_aborts_on_drop() {
    let store = Arc::new(ChunkStore {
        fail_complete: true,
        ..Default::default()
    });
    let remote = foreign(store.clone());
    let mut upload = remote.put_multipart(&Path::from("x.nc")).await.unwrap();
    let error = upload.complete().await.unwrap_err();
    assert!(error.to_string().contains("complete fails"), "{error}");
    drop(upload);
    assert_eq!(wait_for_count(&store.aborts, 1).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_multipart_frees_host_upload() {
    let store = Arc::new(ChunkStore::default());
    let remote = foreign(store.clone());
    let upload = remote.put_multipart(&Path::from("x.nc")).await.unwrap();
    assert_eq!(store.upload_drops.load(Ordering::SeqCst), 0);
    drop(upload);
    assert_eq!(wait_for_count(&store.upload_drops, 1).await, 1);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
}

/// Adds one to its counter when it drops.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Returns a stream of many items that holds a [`DropCounter`] on `drops`.
fn counted_stream<T: Send + 'static>(
    drops: &Arc<AtomicUsize>,
    mut item: impl FnMut() -> T + Send + 'static,
) -> BoxStream<'static, T> {
    let guard = DropCounter(Arc::clone(drops));
    stream::repeat_with(move || {
        let _guard = &guard;
        item()
    })
    .take(1000)
    .boxed()
}

/// A store whose `get_opts` payload and `list` streams count their drops.
#[derive(Debug, Default)]
struct DropStore {
    get_drops: Arc<AtomicUsize>,
    list_drops: Arc<AtomicUsize>,
}

impl fmt::Display for DropStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DropStore")
    }
}

#[async_trait]
impl ObjectStore for DropStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(not_implemented("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(&self, location: &Path, _: GetOptions) -> Result<GetResult> {
        let payload = counted_stream(&self.get_drops, || Ok(Bytes::from_static(b"chunk")));
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size: 5000,
                e_tag: None,
                version: None,
            },
            range: 0..5000,
            attributes: Attributes::new(),
        })
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        stream::once(async { Err(not_implemented("delete_stream")) }).boxed()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        counted_stream(&self.list_drops, || {
            Ok(ObjectMeta {
                location: Path::from("x.nc"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            })
        })
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(not_implemented("list_with_delimiter"))
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(not_implemented("copy_opts"))
    }

    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        Err(not_implemented("rename_opts"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_get_stream_frees_host_stream() {
    let store = Arc::new(DropStore::default());
    let remote = foreign(store.clone());
    let result = remote.get(&Path::from("x.nc")).await.unwrap();
    let GetResultPayload::Stream(mut chunks) = result.payload else {
        panic!("expected a stream payload");
    };
    chunks.next().await.unwrap().unwrap();
    assert_eq!(store.get_drops.load(Ordering::SeqCst), 0);
    drop(chunks);
    assert_eq!(wait_for_count(&store.get_drops, 1).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_list_stream_frees_host_stream() {
    let store = Arc::new(DropStore::default());
    let remote = foreign(store.clone());
    let mut list = remote.list(None);
    list.next().await.unwrap().unwrap();
    assert_eq!(store.list_drops.load(Ordering::SeqCst), 0);
    drop(list);
    assert_eq!(wait_for_count(&store.list_drops, 1).await, 1);
}

/// Records whether its drop runs inside a tokio runtime.
struct RuntimeProbe(Arc<Mutex<Option<bool>>>);

impl Drop for RuntimeProbe {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = Some(Handle::try_current().is_ok());
    }
}

/// A store whose `get_opts` never completes and holds a [`RuntimeProbe`].
#[derive(Debug, Default)]
struct PendingStore {
    dropped_in_runtime: Arc<Mutex<Option<bool>>>,
}

impl fmt::Display for PendingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PendingStore")
    }
}

#[async_trait]
impl ObjectStore for PendingStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(not_implemented("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(&self, _: &Path, _: GetOptions) -> Result<GetResult> {
        let _probe = RuntimeProbe(Arc::clone(&self.dropped_in_runtime));
        std::future::pending().await
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        stream::once(async { Err(not_implemented("delete_stream")) }).boxed()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        stream::once(async { Err(not_implemented("list")) }).boxed()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(not_implemented("list_with_delimiter"))
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(not_implemented("copy_opts"))
    }

    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        Err(not_implemented("rename_opts"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_pending_get_drops_inner_future_in_handle() {
    let store = Arc::new(PendingStore::default());
    let remote = foreign(store.clone());
    let path = Path::from("x.nc");
    let mut get = remote.get(&path).boxed();
    // One poll reaches the pending inner future.
    assert!((&mut get).now_or_never().is_none());
    assert!(store.dropped_in_runtime.lock().unwrap().is_none());
    // The drop runs on a thread outside any runtime.
    std::thread::scope(|scope| {
        scope.spawn(move || {
            assert!(Handle::try_current().is_err());
            drop(get);
        });
    });
    assert_eq!(*store.dropped_in_runtime.lock().unwrap(), Some(true));
}

/// A store that panics in each call.
#[derive(Debug)]
struct PanicStore;

impl fmt::Display for PanicStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanicStore")
    }
}

#[async_trait]
impl ObjectStore for PanicStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        panic!("put_opts")
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        panic!("put_multipart_opts")
    }

    async fn get_opts(&self, _: &Path, _: GetOptions) -> Result<GetResult> {
        panic!("get_opts")
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        panic!("delete_stream")
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        panic!("list")
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        panic!("list_with_delimiter")
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        panic!("copy_opts")
    }

    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        panic!("rename_opts")
    }
}

fn assert_panic_error(error: Error, text: &str) {
    let Error::Generic { source, .. } = &error else {
        panic!("expected a Generic error, got {error:?}");
    };
    let message = source.to_string();
    assert!(
        message.starts_with("panic in object store call:"),
        "{message}"
    );
    assert!(message.contains(text), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn panicking_store_gives_errors() {
    let remote = foreign(Arc::new(PanicStore));
    let path = Path::from("x.nc");
    assert_panic_error(remote.get(&path).await.unwrap_err(), "get_opts");
    // `head` goes through `get_opts`.
    assert_panic_error(remote.head(&path).await.unwrap_err(), "get_opts");
    let put = remote.put(&path, PutPayload::from_static(b"x")).await;
    assert_panic_error(put.unwrap_err(), "put_opts");
    let listed: Vec<Result<ObjectMeta>> = remote.list(None).collect().await;
    assert_eq!(listed.len(), 1);
    let Some(Err(error)) = listed.into_iter().next() else {
        panic!("expected one error item");
    };
    assert_panic_error(error, "list");
    let locations = stream::iter([Ok(path.clone())]).boxed();
    let deleted: Vec<Result<Path>> = remote.delete_stream(locations).collect().await;
    assert_eq!(deleted.len(), 1);
    let Some(Err(error)) = deleted.into_iter().next() else {
        panic!("expected one error item");
    };
    assert_panic_error(error, "delete_stream");
    let listed = remote.list_with_delimiter(None).await;
    assert_panic_error(listed.unwrap_err(), "list_with_delimiter");
    let other = Path::from("y.nc");
    assert_panic_error(remote.copy(&path, &other).await.unwrap_err(), "copy_opts");
    assert_panic_error(
        remote.rename(&path, &other).await.unwrap_err(),
        "rename_opts",
    );
    let Err(error) = remote.put_multipart(&path).await else {
        panic!("expected an error");
    };
    assert_panic_error(error, "put_multipart_opts");
    // The store still works after the panics.
    assert_panic_error(remote.get(&path).await.unwrap_err(), "get_opts");
}

/// An `InMemory` store whose `get_opts` needs the tokio timer.
#[derive(Debug, Default)]
struct SleepStore(InMemory);

impl fmt::Display for SleepStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SleepStore")
    }
}

#[async_trait]
impl ObjectStore for SleepStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.0.rename_opts(from, to, options).await
    }
}

#[test]
fn block_on_outside_runtime() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let path = Path::from("a/1.nc");
    let store = SleepStore::default();
    runtime
        .block_on(store.put(&path, PutPayload::from_static(DATA)))
        .unwrap();
    let remote = {
        let _guard = runtime.enter();
        foreign(Arc::new(store))
    };
    let bytes = std::thread::spawn(move || {
        assert!(Handle::try_current().is_err());
        futures::executor::block_on(async {
            remote.get(&path).await.unwrap().bytes().await.unwrap()
        })
    })
    .join()
    .unwrap();
    assert_eq!(&bytes[..], DATA);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_file_system_get() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("x.nc"), DATA).unwrap();
    let local = ::object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(local);
    let path = Path::from("x.nc");
    // The host store gives a `File` payload; it crosses as a stream.
    let direct = store.get(&path).await.unwrap();
    assert!(matches!(direct.payload, GetResultPayload::File(..)));
    let remote = foreign(store);
    let result = remote.get(&path).await.unwrap();
    assert!(matches!(result.payload, GetResultPayload::Stream(..)));
    assert_eq!(&result.bytes().await.unwrap()[..], DATA);
    let options = GetOptions {
        range: Some(GetRange::Bounded(2..5)),
        ..Default::default()
    };
    let ranged = remote.get_opts(&path, options).await.unwrap();
    assert_eq!(&ranged.bytes().await.unwrap()[..], b"234");
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_deletes_all() {
    let paths = ["a/1.nc", "a/b/2.nc", "d/4.nc"].map(Path::from);
    let input = || {
        let mut items: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();
        items.insert(
            1,
            Err(Error::NotFound {
                path: "gone.nc".to_string(),
                source: "missing".into(),
            }),
        );
        stream::iter(items).boxed()
    };

    let direct_store = seeded().await;
    let direct: Vec<Result<Path>> = direct_store.delete_stream(input()).collect().await;

    let store = seeded().await;
    let remote = foreign(store.clone());
    let crossed: Vec<Result<Path>> = remote.delete_stream(input()).collect().await;

    assert_eq!(crossed.len(), direct.len());
    for (crossed, direct) in crossed.iter().zip(&direct) {
        match (crossed, direct) {
            (Ok(crossed), Ok(direct)) => assert_eq!(crossed, direct),
            (Err(crossed), Err(direct)) => {
                assert_eq!(discriminant(crossed), discriminant(direct), "{crossed:?}")
            }
            other => panic!("items differ: {other:?}"),
        }
    }
    assert_eq!(crossed.iter().filter(|item| item.is_err()).count(), 1);
    for path in &paths {
        let error = store.get(path).await.unwrap_err();
        assert!(matches!(error, Error::NotFound { .. }), "{error:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_and_rename_modes() {
    let store = seeded().await;
    let remote = foreign(store.clone());
    let from = Path::from("a/1.nc");
    let existing = Path::from("d/4.nc");

    let copy_create = CopyOptions {
        mode: CopyMode::Create,
        ..Default::default()
    };
    let error = remote
        .copy_opts(&from, &existing, copy_create)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AlreadyExists { .. }), "{error:?}");

    let rename_create = RenameOptions {
        target_mode: RenameTargetMode::Create,
        ..Default::default()
    };
    let error = remote
        .rename_opts(&from, &existing, rename_create)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AlreadyExists { .. }), "{error:?}");

    let copied = Path::from("copy.nc");
    remote.copy(&from, &copied).await.unwrap();
    let renamed = Path::from("renamed.nc");
    remote.rename(&copied, &renamed).await.unwrap();
    let back = store.get(&renamed).await.unwrap().bytes().await.unwrap();
    assert_eq!(&back[..], DATA);
    assert!(matches!(
        store.head(&copied).await.unwrap_err(),
        Error::NotFound { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn unusual_path_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let remote = foreign(store.clone());
    let path = Path::from("dir a/100%/ü.nc");
    remote
        .put(&path, PutPayload::from_static(b"odd"))
        .await
        .unwrap();
    assert_eq!(store.head(&path).await.unwrap().location, path);
    assert_eq!(remote.head(&path).await.unwrap().location, path);
    let listed = sorted_list(remote.list(Some(&Path::from("dir a")))).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].location, path);
}
