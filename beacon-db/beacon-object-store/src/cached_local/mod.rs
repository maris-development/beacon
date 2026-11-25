//! A local store wrapper that keeps file handles open and caches blocks.

mod blocks;
mod handles;
mod options;
mod read;
mod version;

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
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
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
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
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
