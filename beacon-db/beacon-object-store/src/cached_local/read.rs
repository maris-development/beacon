//! Positional reads of open files and the read function of the store.

use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, coalesce_ranges, path::Path as ObjectPath,
};

use super::CachedLocalOptions;
use super::handles::{HandleCache, OpenFile, STORE};

/// Reads into `buf` at `offset` until `buf` is full or the file ends. Returns the bytes read.
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let at = offset + total as u64;
        #[cfg(unix)]
        let result = std::os::unix::fs::FileExt::read_at(file, &mut buf[total..], at);
        #[cfg(windows)]
        let result = std::os::windows::fs::FileExt::seek_read(file, &mut buf[total..], at);
        match result {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

fn generic(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: source.into(),
    }
}

/// True for an error that a new handle of the same path can fix.
fn is_stale(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::StaleNetworkFileHandle
    )
}

fn read_error(path: &Path, e: io::Error) -> object_store::Error {
    generic(io::Error::new(
        e.kind(),
        format!("Unable to read data from file {}: {e}", path.display()),
    ))
}

/// Returns the stock error for a non-empty `range` that does not fit in `size` bytes.
fn check_eof(path: &Path, size: u64, range: &Range<u64>) -> object_store::Result<()> {
    // Errors for a start past the end, as the stock store does.
    GetRange::Bounded(range.clone())
        .as_range(size)
        .map_err(generic)?;
    if range.end > size {
        return Err(generic(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "Out of range of file {}, expected: {}, actual: {}",
                path.display(),
                size - range.start,
                range.end - range.start
            ),
        )));
    }
    Ok(())
}

/// True when `range` reaches past `size`, so that a newer file version can change the result.
fn needs_fresh(range: Option<&GetRange>, size: u64) -> bool {
    match range {
        Some(GetRange::Bounded(r)) => r.end > size,
        Some(GetRange::Offset(o)) => *o >= size,
        Some(GetRange::Suffix(_)) | None => false,
    }
}

/// The open handles and the read counters of the store.
pub(crate) struct Reader {
    pub handles: HandleCache,
    stream_chunk: u64,
    coalesce_gap: u64,
    disk_reads: AtomicU64,
    disk_bytes: AtomicU64,
}

impl Reader {
    pub fn new(opts: &CachedLocalOptions) -> Self {
        Self {
            handles: HandleCache::new(opts, None),
            stream_chunk: opts.stream_chunk,
            coalesce_gap: opts.coalesce_gap,
            disk_reads: AtomicU64::new(0),
            disk_bytes: AtomicU64::new(0),
        }
    }

    /// The number of positional reads and the bytes they read.
    pub fn disk_counters(&self) -> (u64, u64) {
        (
            self.disk_reads.load(Ordering::Relaxed),
            self.disk_bytes.load(Ordering::Relaxed),
        )
    }

    /// The `get_opts` of the store for the file at `path`.
    pub async fn get_opts(
        self: &Arc<Self>,
        path: PathBuf,
        location: ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut file = self.handles.get(&path).await?;
        if needs_fresh(options.range.as_ref(), file.version.size) {
            file = self.handles.get_fresh(&path).await?;
        }
        let meta = file.version.object_meta(location);
        options.check_preconditions(&meta)?;
        let range = match &options.range {
            Some(r) => r.as_range(meta.size).map_err(generic)?,
            None => 0..meta.size,
        };
        let payload = if options.head {
            futures::stream::empty().boxed()
        } else {
            self.stream(path, file, range.clone())
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    /// The `get_ranges` of the store for the file at `path`. All ranges read one file version.
    pub async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let mut file = self.handles.get(path).await?;
        let end = ranges.iter().filter(|r| !r.is_empty()).map(|r| r.end).max();
        if end.is_some_and(|end| end > file.version.size) {
            file = self.handles.get_fresh(path).await?;
        }
        coalesce_ranges(ranges, |r| self.read(path, &file, r), self.coalesce_gap).await
    }

    /// Streams `range` of `file` in pieces of at most `stream_chunk` bytes.
    fn stream(
        self: &Arc<Self>,
        path: PathBuf,
        file: Arc<OpenFile>,
        range: Range<u64>,
    ) -> BoxStream<'static, object_store::Result<Bytes>> {
        let reader = self.clone();
        let end = range.end;
        futures::stream::try_unfold(range.start, move |pos| {
            let reader = reader.clone();
            let path = path.clone();
            let file = file.clone();
            async move {
                if pos >= end {
                    return Ok(None);
                }
                let next = end.min(pos.saturating_add(reader.stream_chunk));
                let bytes = reader.read(&path, &file, pos..next).await?;
                Ok(Some((bytes, next)))
            }
        })
        .boxed()
    }

    /// Reads `range` of `file`, the open file at `path`, with the stock errors past its end.
    ///
    /// A read that fails because the handle went stale retries once on a new handle of the
    /// same file version.
    pub async fn read(
        &self,
        path: &Path,
        file: &Arc<OpenFile>,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        check_eof(path, file.version.size, &range)?;
        match self.read_at(file.clone(), range.clone()).await? {
            Ok(bytes) => Ok(bytes),
            Err(e) if is_stale(&e) => {
                self.handles.invalidate(path).await;
                let fresh = self.handles.get_fresh(path).await?;
                // A new version must not mix with bytes this call already returned.
                if fresh.version != file.version {
                    return Err(read_error(path, e));
                }
                self.read_at(fresh, range)
                    .await?
                    .map_err(|e| read_error(path, e))
            }
            Err(e) => Err(read_error(path, e)),
        }
    }

    /// One positional read of the whole `range` on a blocking thread.
    async fn read_at(
        &self,
        file: Arc<OpenFile>,
        range: Range<u64>,
    ) -> object_store::Result<io::Result<Bytes>> {
        let len = (range.end - range.start) as usize;
        let read = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; len];
            let n = read_exact_at(&file.file, &mut buf, range.start)?;
            buf.truncate(n);
            Ok::<_, io::Error>(buf)
        })
        .await
        .map_err(generic)?;
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
        Ok(read.and_then(|buf| {
            self.disk_bytes
                .fetch_add(buf.len() as u64, Ordering::Relaxed);
            if buf.len() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("expected {len} bytes, read {}", buf.len()),
                ));
            }
            Ok(Bytes::from(buf))
        }))
    }
}
