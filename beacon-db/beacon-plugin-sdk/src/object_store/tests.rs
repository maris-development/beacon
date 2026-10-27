//! Round trips of `FFI_ObjectStore` through the forced foreign path.

use std::fmt;
use std::mem::discriminant;
use std::sync::Arc;

use ::bytes::Bytes;
use ::object_store::memory::InMemory;
use ::object_store::path::Path;
use ::object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result,
};
use async_trait::async_trait;
use datafusion_ffi::get_library_marker_id;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
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
async fn write_operations_are_not_implemented() {
    let remote = foreign(seeded().await);
    let path = Path::from("new.nc");
    let error = remote.put(&path, PutPayload::from_static(b"x")).await;
    assert!(matches!(error, Err(Error::NotImplemented { .. })));
    let error = remote.copy(&Path::from("a/1.nc"), &path).await;
    assert!(matches!(error, Err(Error::NotImplemented { .. })));
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
#[derive(Debug)]
struct ChunkStore {
    chunk: Bytes,
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
    let remote = foreign(Arc::new(ChunkStore { chunk }));
    let result = remote.get(&Path::from("x.nc")).await.unwrap();
    let GetResultPayload::Stream(mut chunks) = result.payload else {
        panic!("expected a stream payload");
    };
    let received = chunks.next().await.unwrap().unwrap();
    assert_eq!(received.as_ptr(), pointer);
    assert_eq!(received.len(), 64);
    assert!(chunks.next().await.is_none());
}
