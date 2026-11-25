//! A local store wrapper that keeps file handles open and caches blocks.

mod blocks;
mod handles;
mod options;
mod read;
mod version;

use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, UploadPart,
};

pub use options::CachedLocalOptions;
use read::Reader;

/// The counters of a [`CachedLocalFileSystem`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// The open attempts of the handle cache.
    pub opens: u64,
    /// The blocks that the block cache answered.
    pub block_hits: u64,
    /// The blocks loaded from disk into the block cache.
    pub block_misses: u64,
    /// The positional reads from disk.
    pub disk_reads: u64,
    /// The bytes of the positional reads from disk.
    pub disk_bytes: u64,
}

/// A `LocalFileSystem` that keeps file handles open and reads with positional reads.
pub struct CachedLocalFileSystem {
    inner: LocalFileSystem,
    opts: CachedLocalOptions,
    reader: Arc<Reader>,
}

impl CachedLocalFileSystem {
    /// Wraps `inner` with the given options.
    pub fn new(inner: LocalFileSystem, opts: CachedLocalOptions) -> Self {
        let reader = Arc::new(Reader::new(&opts));
        Self {
            inner,
            opts,
            reader,
        }
    }

    /// Returns the current counters.
    pub fn stats(&self) -> CacheStats {
        let (disk_reads, disk_bytes) = self.reader.disk_counters();
        let (block_hits, block_misses) = self.reader.block_counters();
        CacheStats {
            opens: self.reader.handles.opens(),
            block_hits,
            block_misses,
            disk_reads,
            disk_bytes,
        }
    }

    fn evictor(&self) -> Evictor {
        Evictor {
            inner: self.inner.clone(),
            reader: self.reader.clone(),
        }
    }

    /// Drops the cached handle and all cached blocks of `location`.
    async fn evict(&self, location: &Path) {
        self.evictor().evict(location).await
    }

    /// Hides `.nfs` files of `entries` and evicts each listed file whose cached version differs.
    fn check_listing(
        &self,
        entries: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let evictor = self.evictor();
        entries
            .try_filter(|meta| std::future::ready(!is_nfs(&meta.location)))
            .and_then(move |meta| {
                let evictor = evictor.clone();
                // Only in-memory work: a cache lookup and, for a changed file, an eviction.
                async move {
                    evictor.evict_if_changed(&meta).await;
                    Ok(meta)
                }
            })
            .boxed()
    }
}

/// True for an NFS silly-rename file, which a listing hides.
fn is_nfs(location: &Path) -> bool {
    location
        .filename()
        .is_some_and(|name| name.starts_with(".nfs"))
}

/// The parts of the store that an eviction needs, for streams and uploads that outlive a call.
#[derive(Clone)]
struct Evictor {
    inner: LocalFileSystem,
    reader: Arc<Reader>,
}

impl Evictor {
    /// Drops the cached handle and, through its change callback, all cached blocks of
    /// `location`.
    async fn evict(&self, location: &Path) {
        // An invalid path has no cached handle; the inner store reports the error.
        if let Ok(path) = self.inner.path_to_filesystem(location) {
            self.reader.handles.invalidate(&path).await;
        }
    }

    /// Evicts the file of `meta` when its cached handle has another size or mtime.
    async fn evict_if_changed(&self, meta: &ObjectMeta) {
        let Ok(path) = self.inner.path_to_filesystem(&meta.location) else {
            return;
        };
        if let Some(file) = self.reader.handles.peek(&path).await
            && (file.version.size != meta.size || file.version.last_modified != meta.last_modified)
        {
            self.reader.handles.invalidate(&path).await;
        }
    }
}

/// A multipart upload that evicts its location when it completes or aborts.
struct EvictingUpload {
    upload: Box<dyn MultipartUpload>,
    evictor: Evictor,
    location: Path,
}

impl fmt::Debug for EvictingUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvictingUpload")
            .field("upload", &self.upload)
            .field("location", &self.location)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for EvictingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.evictor.evict(&self.location).await;
        let result = self.upload.complete().await;
        self.evictor.evict(&self.location).await;
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self.upload.abort().await;
        self.evictor.evict(&self.location).await;
        result
    }
}

impl fmt::Display for CachedLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedLocalFileSystem({})", self.inner)
    }
}

impl fmt::Debug for CachedLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedLocalFileSystem")
            .field("inner", &self.inner)
            .field("opts", &self.opts)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for CachedLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.evict(location).await;
        let result = self.inner.put_opts(location, payload, opts).await;
        self.evict(location).await;
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.evict(location).await;
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.evict(location).await;
        Ok(Box::new(EvictingUpload {
            upload: result?,
            evictor: self.evictor(),
            location: location.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let path = self.inner.path_to_filesystem(location)?;
        self.reader.get_opts(path, location.clone(), options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let path = self.inner.path_to_filesystem(location)?;
        self.reader.get_ranges(&path, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // The inner stream answers each location once and in order, so a queue pairs them.
        let pending = Arc::new(parking_lot::Mutex::new(VecDeque::<Option<Path>>::new()));
        let before = (self.evictor(), pending.clone());
        let locations = locations
            .then(move |location| {
                let (evictor, pending) = before.clone();
                async move {
                    if let Ok(path) = &location {
                        evictor.evict(path).await;
                    }
                    pending.lock().push_back(location.as_ref().ok().cloned());
                    location
                }
            })
            .boxed();
        let evictor = self.evictor();
        self.inner
            .delete_stream(locations)
            .then(move |result| {
                let evictor = evictor.clone();
                let location = pending.lock().pop_front().flatten();
                async move {
                    if let Some(path) = location {
                        evictor.evict(&path).await;
                    }
                    result
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.check_listing(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.check_listing(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects.retain(|meta| !is_nfs(&meta.location));
        result.common_prefixes.retain(|p| !is_nfs(p));
        let evictor = self.evictor();
        for meta in &result.objects {
            evictor.evict_if_changed(meta).await;
        }
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.evict(to).await;
        let result = self.inner.copy_opts(from, to, options).await;
        self.evict(to).await;
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.evict(from).await;
        self.evict(to).await;
        let result = self.inner.rename_opts(from, to, options).await;
        self.evict(from).await;
        self.evict(to).await;
        result
    }
}
