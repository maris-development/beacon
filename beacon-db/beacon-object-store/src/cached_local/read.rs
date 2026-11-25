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

fn read_error(path: &Path, e: io::Error) -> object_store::Error {
    generic(io::Error::new(
        e.kind(),
        format!("Unable to read data from file {}: {e}", path.display()),
    ))
}

/// An error of the read function.
enum ReadError {
    /// A positional read error that a new handle of the same path can fix.
    Stale(io::Error),
    /// Any other error, in its final form.
    Store(object_store::Error),
}

impl ReadError {
    fn into_store(self, path: &Path) -> object_store::Error {
        match self {
            ReadError::Stale(e) => read_error(path, e),
            ReadError::Store(e) => e,
        }
    }
}

impl From<object_store::Error> for ReadError {
    fn from(e: object_store::Error) -> Self {
        ReadError::Store(e)
    }
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
    /// Test seam: each positional read takes the next entry; `true` fails it as not found.
    #[cfg(test)]
    fail_reads: parking_lot::Mutex<std::collections::VecDeque<bool>>,
}

impl Reader {
    pub fn new(opts: &CachedLocalOptions) -> Self {
        Self {
            handles: HandleCache::new(opts, None),
            stream_chunk: opts.stream_chunk,
            coalesce_gap: opts.coalesce_gap,
            disk_reads: AtomicU64::new(0),
            disk_bytes: AtomicU64::new(0),
            #[cfg(test)]
            fail_reads: Default::default(),
        }
    }

    /// The number of positional reads and the bytes they read.
    pub fn disk_counters(&self) -> (u64, u64) {
        (
            self.disk_reads.load(Ordering::Relaxed),
            self.disk_bytes.load(Ordering::Relaxed),
        )
    }

    /// Drops the handle of `path` and returns a new one.
    async fn reopen(&self, path: &Path) -> object_store::Result<Arc<OpenFile>> {
        self.handles.invalidate(path).await;
        self.handles.get_fresh(path).await
    }

    /// The `get_opts` of the store for the file at `path`.
    ///
    /// The first piece is read before the result is returned, so that a stale handle can
    /// restart the call on a new handle with the metadata of that handle.
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
        let mut restarted = false;
        loop {
            let meta = file.version.object_meta(location.clone());
            options.check_preconditions(&meta)?;
            let range = match &options.range {
                Some(r) => r.as_range(meta.size).map_err(generic)?,
                None => 0..meta.size,
            };
            let first = range.start..range.end.min(range.start.saturating_add(self.stream_chunk));
            let payload = if options.head || first.is_empty() {
                futures::stream::empty().boxed()
            } else {
                match self.read(&path, &file, first.clone()).await {
                    Ok(bytes) => {
                        let rest = self.stream(path.clone(), file.clone(), first.end..range.end);
                        futures::stream::once(async { Ok(bytes) })
                            .chain(rest)
                            .boxed()
                    }
                    Err(ReadError::Stale(_)) if !restarted => {
                        restarted = true;
                        file = self.reopen(&path).await?;
                        continue;
                    }
                    Err(e) => return Err(e.into_store(&path)),
                }
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(payload),
                meta,
                range,
                attributes: Default::default(),
            });
        }
    }

    /// The `get_ranges` of the store for the file at `path`. All ranges read one handle.
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
        match self.read_ranges(path, &file, ranges).await {
            Err(ReadError::Stale(_)) => {
                let fresh = self.reopen(path).await?;
                self.read_ranges(path, &fresh, ranges)
                    .await
                    .map_err(|e| e.into_store(path))
            }
            result => result.map_err(|e| e.into_store(path)),
        }
    }

    /// Reads all `ranges` of `file` with coalesced reads.
    async fn read_ranges(
        &self,
        path: &Path,
        file: &Arc<OpenFile>,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, ReadError> {
        coalesce_ranges(ranges, |r| self.read(path, file, r), self.coalesce_gap).await
    }

    /// Streams `range` of `file` in pieces of at most `stream_chunk` bytes.
    ///
    /// A stale handle moves to a new handle only when the file version is the same.
    fn stream(
        self: &Arc<Self>,
        path: PathBuf,
        file: Arc<OpenFile>,
        range: Range<u64>,
    ) -> BoxStream<'static, object_store::Result<Bytes>> {
        let reader = self.clone();
        let end = range.end;
        futures::stream::try_unfold((range.start, file), move |(pos, file)| {
            let reader = reader.clone();
            let path = path.clone();
            async move {
                if pos >= end {
                    return Ok(None);
                }
                let next = end.min(pos.saturating_add(reader.stream_chunk));
                let (bytes, file) = match reader.read(&path, &file, pos..next).await {
                    Ok(bytes) => (bytes, file),
                    Err(ReadError::Stale(e)) => {
                        let fresh = reader.reopen(&path).await?;
                        // The returned metadata describes this version only.
                        if fresh.version != file.version {
                            return Err(read_error(&path, e));
                        }
                        let bytes = reader
                            .read(&path, &fresh, pos..next)
                            .await
                            .map_err(|e| e.into_store(&path))?;
                        (bytes, fresh)
                    }
                    Err(e) => return Err(e.into_store(&path)),
                };
                Ok(Some((bytes, (next, file))))
            }
        })
        .boxed()
    }

    /// Reads `range` of `file`, the open file at `path`, with the stock errors past its end.
    async fn read(
        &self,
        path: &Path,
        file: &Arc<OpenFile>,
        range: Range<u64>,
    ) -> Result<Bytes, ReadError> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        check_eof(path, file.version.size, &range)?;
        match self.read_at(file.clone(), range).await? {
            Ok(bytes) => Ok(bytes),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::StaleNetworkFileHandle
                ) =>
            {
                Err(ReadError::Stale(e))
            }
            Err(e) => Err(ReadError::Store(read_error(path, e))),
        }
    }

    /// One positional read of the whole `range` on a blocking thread.
    async fn read_at(
        &self,
        file: Arc<OpenFile>,
        range: Range<u64>,
    ) -> object_store::Result<io::Result<Bytes>> {
        #[cfg(test)]
        if self.fail_reads.lock().pop_front() == Some(true) {
            return Ok(Err(io::Error::new(io::ErrorKind::NotFound, "injected")));
        }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A reader with the given stream piece size and no coalescing, and a file of `len` ones.
    fn setup(stream_chunk: u64, len: usize) -> (tempfile::TempDir, PathBuf, Arc<Reader>) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, vec![1u8; len]).unwrap();
        let opts = CachedLocalOptions {
            revalidate_after: Duration::from_secs(3600),
            coalesce_gap: 0,
            stream_chunk,
            ..Default::default()
        };
        (dir, p, Arc::new(Reader::new(&opts)))
    }

    /// Writes `len` twos to a temporary file and renames it over `path`.
    fn replace(path: &Path, len: usize) {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, vec![2u8; len]).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    fn fail(reader: &Reader, script: &[bool]) {
        reader.fail_reads.lock().extend(script.iter().copied());
    }

    async fn get(reader: &Arc<Reader>, path: &Path, range: Option<Range<u64>>) -> GetResult {
        let opts = GetOptions::new().with_range(range);
        reader
            .get_opts(path.to_path_buf(), ObjectPath::from("a.bin"), opts)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn get_ranges_restarts_on_new_file() {
        let (_dir, p, reader) = setup(8 << 20, 1000);
        reader
            .get_ranges(&p, std::slice::from_ref(&(0..10)))
            .await
            .unwrap();
        // The cached handle keeps the old file readable after the rename.
        replace(&p, 2000);
        // The first fetch reads the old file, the second fails: the whole call restarts.
        fail(&reader, &[false, true]);
        let got = reader.get_ranges(&p, &[0..100, 500..600]).await.unwrap();
        for b in &got {
            assert_eq!(b.len(), 100);
            assert!(b.iter().all(|&x| x == 2), "mixed bytes");
        }
        assert_eq!(reader.handles.opens(), 2);
    }

    #[tokio::test]
    async fn get_range_restarts_on_new_file() {
        let (_dir, p, reader) = setup(8 << 20, 1000);
        reader
            .get_ranges(&p, std::slice::from_ref(&(0..10)))
            .await
            .unwrap();
        replace(&p, 2000);
        fail(&reader, &[true]);
        let r = get(&reader, &p, Some(0..100)).await;
        assert_eq!(r.meta.size, 2000);
        let bytes = r.bytes().await.unwrap();
        assert_eq!(bytes.len(), 100);
        assert!(bytes.iter().all(|&x| x == 2), "mixed bytes");
        fail(&reader, &[true]);
        replace(&p, 3000);
        let r = get(&reader, &p, None).await;
        assert_eq!(r.meta.size, 3000);
        let bytes = r.bytes().await.unwrap();
        assert_eq!(bytes.len(), 3000);
        assert!(bytes.iter().all(|&x| x == 2), "mixed bytes");
    }

    #[tokio::test]
    async fn second_failure_is_generic() {
        let (_dir, p, reader) = setup(8 << 20, 1000);
        fail(&reader, &[true, true]);
        let err = reader
            .get_ranges(&p, std::slice::from_ref(&(0..10)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, object_store::Error::Generic { store: STORE, .. }),
            "{err}"
        );
        fail(&reader, &[true, true]);
        let opts = GetOptions::new().with_range(Some(0..10));
        let err = reader
            .get_opts(p.clone(), ObjectPath::from("a.bin"), opts)
            .await
            .unwrap_err();
        assert!(
            matches!(err, object_store::Error::Generic { store: STORE, .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn stream_retries_later_piece_on_same_version() {
        let (_dir, p, reader) = setup(100, 300);
        fail(&reader, &[false, true]);
        let bytes = get(&reader, &p, None).await.bytes().await.unwrap();
        assert_eq!(&bytes[..], &[1u8; 300][..]);
    }

    #[tokio::test]
    async fn stream_fails_later_piece_on_new_version() {
        let (_dir, p, reader) = setup(100, 300);
        fail(&reader, &[false, true]);
        let r = get(&reader, &p, None).await;
        replace(&p, 300);
        // Same size, so make sure the mtime differs.
        let f = std::fs::File::options().write(true).open(&p).unwrap();
        f.set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .unwrap();
        drop(f);
        let err = r.bytes().await.unwrap_err();
        assert!(
            matches!(err, object_store::Error::Generic { store: STORE, .. }),
            "{err}"
        );
    }
}
