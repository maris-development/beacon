//! A store wrapper that asks for large listing pages.
//!
//! [`ObjectStore::list`] sends no `max-keys`, so a server applies its own
//! default of 1000 keys per page. A recursive listing of a large bucket is
//! therefore a long chain of small, strictly sequential requests: each page
//! needs the continuation token of the one before it, so nothing overlaps.
//!
//! Measured against a SeaweedFS bucket holding 2 853 217 objects, the walk took
//! 2854 requests. The same walk with `max-keys=65535` took 44. On a co-located
//! deployment the saving is the per-request round trip, which is the whole cost
//! of the walk: the listing is pure wait, not work.
//!
//! [`BigPageList`] wraps a store that implements [`PaginatedListStore`] and
//! re-expresses `list` over it with an explicit page size. Every other method
//! delegates untouched.
//!
//! # Page size
//!
//! [`DEFAULT_MAX_KEYS`] is 65535. AWS caps `max-keys` at 1000 and ignores
//! anything larger, so this is a no-op there rather than an error. SeaweedFS
//! parses the parameter as a `uint16` and honours the whole range. A server
//! that returns fewer keys than asked is already the normal case, and the
//! continuation token handles it.

use std::fmt;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

/// Keys requested per listing page. See the module docs for why this value.
pub const DEFAULT_MAX_KEYS: usize = 65535;

/// A store whose `list` asks for [`DEFAULT_MAX_KEYS`] keys per page.
#[derive(Debug, Clone)]
pub struct BigPageList<T> {
    inner: Arc<T>,
    max_keys: usize,
}

impl<T> BigPageList<T> {
    /// Wrap `inner`, requesting [`DEFAULT_MAX_KEYS`] keys per page.
    pub fn new(inner: T) -> Self {
        Self::with_max_keys(inner, DEFAULT_MAX_KEYS)
    }

    /// The same, with the page size named.
    pub fn with_max_keys(inner: T, max_keys: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            max_keys: max_keys.max(1),
        }
    }
}

impl<T: fmt::Debug> fmt::Display for BigPageList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BigPageList(max_keys={}, {:?})", self.max_keys, self.inner)
    }
}

#[async_trait::async_trait]
impl<T> ObjectStore for BigPageList<T>
where
    T: ObjectStore + PaginatedListStore + 'static,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    /// The recursive listing, page by page, with an explicit `max-keys`.
    ///
    /// The stream stays lazy: a page is fetched only when the consumer has
    /// drained the one before it, so a caller that stops early (a `LIMIT`, a
    /// prefix match) stops paying. That mirrors the stream `list` returns.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let max_keys = self.max_keys;

        futures::stream::try_unfold(Some(None::<String>), move |state| {
            let inner = Arc::clone(&inner);
            let prefix = prefix.clone();
            async move {
                // `None` state means the previous page was the last one.
                let Some(token) = state else {
                    return Ok::<_, object_store::Error>(None);
                };
                let opts = PaginatedListOptions {
                    max_keys: Some(max_keys),
                    page_token: token,
                    ..Default::default()
                };
                let page = inner
                    .list_paginated(prefix.as_ref().map(|p| p.as_ref()), opts)
                    .await?;
                let next = page.page_token.map(Some);
                Ok(Some((
                    futures::stream::iter(page.result.objects.into_iter().map(Ok)),
                    next,
                )))
            }
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
