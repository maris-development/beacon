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
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_version_is_refused() {
    let mut ffi = FFI_ObjectStore::new(seeded().await, Handle::current());
    ffi.library_marker_id = foreign_marker_id;
    ffi.version = other_version;
    let error = Arc::<dyn ObjectStore>::try_from(ffi).unwrap_err();
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
    let deadline = Instant::now() + Duration::from_secs(1);
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
