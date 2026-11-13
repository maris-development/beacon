//! The `ObjectStore` wrapper.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
};

use crate::config::StoreCacheConfig;
use crate::handle::CacheHandle;
use crate::index::{Cursor, range_start};
use crate::state::{CacheBypass, CacheState};
use crate::upload::CachedUpload;

/// Objects that one lock of the index returns to a listing.
const LIST_CHUNK: usize = 1024;

/// An `ObjectStore` that answers `list` and `head` calls from memory.
///
/// The first listing reads the full source. Later listings read the index.
/// Writes through this store update the index at once. A background sync
/// finds changes that a different program made.
#[derive(Debug, Clone)]
pub struct CachedObjectStore(pub(crate) Arc<CacheState>);

impl CachedObjectStore {
    /// Put a cache in front of `inner`. `config.enable` has no effect here.
    pub fn new(inner: Arc<dyn ObjectStore>, config: &StoreCacheConfig) -> Self {
        Self(Arc::new(CacheState::new(inner, config.clone())))
    }

    /// Put a cache in front of `inner` when `config.enable` is set.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        config: &StoreCacheConfig,
    ) -> (Arc<dyn ObjectStore>, Option<CacheHandle>) {
        if !config.enable {
            return (inner, None);
        }
        let cached = Self::new(inner, config);
        let handle = cached.handle();
        (Arc::new(cached), Some(handle))
    }

    /// Handle that reads the status of this cache and starts a sync.
    pub fn handle(&self) -> CacheHandle {
        CacheHandle(Arc::downgrade(&self.0))
    }

    fn list_from(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let state = Arc::clone(&self.0);
        let key = prefix.map(|path| path.as_ref().to_string()).unwrap_or_default();
        let prefix = prefix.cloned();
        let offset = offset.cloned();
        let offset_key = offset.as_ref().map(|path| path.as_ref().to_string());
        let source = move |state: &CacheState| match &offset {
            Some(offset) => state.inner.list_with_offset(prefix.as_ref(), offset),
            None => state.inner.list(prefix.as_ref()),
        };
        if state.bypasses(&key) {
            CacheState::count(&state.counters.bypassed);
            return source(&state);
        }
        stream::once(async move {
            if state.ensure_synced().await {
                indexed(state, key, offset_key)
            } else {
                source(&state)
            }
        })
        .flatten()
        .boxed()
    }
}

/// Stream the objects below `prefix` from the index, in path order.
fn indexed(
    state: Arc<CacheState>,
    prefix: String,
    offset: Option<String>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    let start = range_start(&prefix);
    let cursor = Cursor::begin(&start, offset.as_deref());
    stream::try_unfold(cursor, move |cursor| {
        let step = match cursor {
            Cursor::Done => Ok(None),
            // A clear removes the objects, so the listing cannot continue.
            cursor if state.index.is_synced() => {
                Ok(Some(state.index.chunk(&start, &cursor, LIST_CHUNK)))
            }
            _ => Err(Error::Generic {
                store: "CachedObjectStore",
                source: "the cache dropped its index during the listing".into(),
            }),
        };
        std::future::ready(step)
    })
    .map_ok(|objects| stream::iter(objects.into_iter().map(Ok)))
    .try_flatten()
    .boxed()
}

/// True for a metadata request that the index can answer.
fn is_plain_head(options: &GetOptions) -> bool {
    options.head
        && options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.range.is_none()
        && options.version.is_none()
        && options.extensions.get::<CacheBypass>().is_none()
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachedObjectStore({})", self.0.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let result = self.0.inner.put_opts(location, payload, opts).await;
        // A failed condition means the index was stale for this path.
        if matches!(
            result,
            Ok(_) | Err(Error::AlreadyExists { .. } | Error::Precondition { .. })
        ) {
            self.0.refresh(location).await;
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.0.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CachedUpload::new(
            upload,
            Arc::clone(&self.0),
            location.clone(),
        )))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let state = &self.0;
        let key = location.as_ref();
        if is_plain_head(&options) && !state.bypasses(key) && state.is_fresh() {
            CacheState::count(&state.counters.hits);
            return match state.index.get(key) {
                Some(meta) => Ok(GetResult {
                    payload: GetResultPayload::Stream(stream::empty().boxed()),
                    range: 0..meta.size,
                    meta,
                    attributes: Attributes::default(),
                }),
                None => Err(Error::NotFound {
                    path: key.to_string(),
                    source: "the object is not in the store cache".into(),
                }),
            };
        }
        CacheState::count(&state.counters.bypassed);
        let tracked = options.version.is_none();
        let seq = state.next_seq();
        let result = state.inner.get_opts(location, options).await;
        if tracked {
            match &result {
                Ok(found) => state.observe(key, &found.meta, seq),
                Err(Error::NotFound { .. }) => state.upsert(key, None, seq),
                Err(_) => {}
            }
        }
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.0.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let state = Arc::clone(&self.0);
        self.0
            .inner
            .delete_stream(locations)
            .inspect_ok(move |location| state.tombstone(location))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_from(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_from(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let state = &self.0;
        let key = prefix.map(|path| path.as_ref().to_string()).unwrap_or_default();
        if state.bypasses(&key) {
            CacheState::count(&state.counters.bypassed);
            return state.inner.list_with_delimiter(prefix).await;
        }
        if !state.ensure_synced().await {
            return state.inner.list_with_delimiter(prefix).await;
        }
        let (folders, objects) = state.index.delimited(&key);
        let common_prefixes = folders
            .iter()
            .map(Path::parse)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let result = self.0.inner.copy_opts(from, to, options).await;
        if matches!(result, Ok(()) | Err(Error::AlreadyExists { .. })) {
            self.0.refresh(to).await;
        }
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let result = self.0.inner.rename_opts(from, to, options).await;
        match &result {
            Ok(()) => {
                self.0.tombstone(from);
                self.0.refresh(to).await;
            }
            Err(Error::AlreadyExists { .. }) => self.0.refresh(to).await,
            Err(_) => {}
        }
        result
    }
}
