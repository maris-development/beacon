//! The cache of aligned file blocks.

use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use moka::future::Cache;

use super::handles::OpenFile;
use super::version::FileVersion;

/// The key of one cached block.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlockKey {
    /// The filesystem path of the file.
    pub file: Arc<PathBuf>,
    /// The version of the file that the block was read from.
    pub version: FileVersion,
    /// The epoch of the handle that read the block.
    pub epoch: u64,
    /// The block index: block `i` covers `i * block_size .. (i + 1) * block_size`.
    pub index: u64,
}

/// A bounded cache of aligned file blocks.
///
/// Block `i` covers `i * block_size .. (i + 1) * block_size` and is shorter at EOF.
pub(crate) struct BlockCache {
    cache: Cache<BlockKey, Bytes>,
    block_size: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    /// Creates a cache of blocks of `block_size` bytes that holds at most `max_bytes`.
    pub fn new(block_size: u64, max_bytes: u64) -> Self {
        assert!(block_size > 0, "block_size must not be 0");
        Self {
            cache: Cache::builder()
                .weigher(|_, v: &Bytes| v.len().try_into().unwrap_or(u32::MAX))
                .max_capacity(max_bytes)
                .support_invalidation_closures()
                .build(),
            block_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// The block size in bytes.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// The blocks found in the cache and the blocks loaded from disk.
    pub fn counters(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// The bytes in the cache. Pending evictions can make it larger than the limit.
    #[cfg(test)]
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Drops all blocks of `path`, of all versions and epochs.
    pub fn invalidate_path(&self, path: &Path) {
        let path = path.to_path_buf();
        self.cache
            .invalidate_entries_if(move |k, _| k.file.as_path() == path)
            .expect("the block cache supports invalidation closures");
    }

    /// Reads `range` of `file`, the open file at `path`, through the cache.
    ///
    /// `fetch` reads one byte range of `file` from disk and fails on a short read. Each run
    /// of adjacent missing blocks takes one `fetch`. The caller checks `range` against the
    /// file size.
    pub async fn read<F, Fut>(
        &self,
        path: &Arc<PathBuf>,
        file: &Arc<OpenFile>,
        range: Range<u64>,
        fetch: F,
    ) -> Result<Bytes, Arc<io::Error>>
    where
        F: Fn(Range<u64>) -> Fut,
        Fut: Future<Output = io::Result<Bytes>>,
    {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let bs = self.block_size;
        let first = range.start / bs;
        let count = ((range.end - 1) / bs - first + 1) as usize;
        let key = |i: usize| BlockKey {
            file: path.clone(),
            version: file.version,
            epoch: file.epoch,
            index: first + i as u64,
        };
        let mut blocks = Vec::with_capacity(count);
        for i in 0..count {
            blocks.push(self.cache.get(&key(i)).await);
        }
        let mut i = 0;
        while i < count {
            if blocks[i].is_some() {
                self.hits.fetch_add(1, Ordering::Relaxed);
                i += 1;
                continue;
            }
            let end = (i..count).find(|&k| blocks[k].is_some()).unwrap_or(count);
            let run_start = key(i).index * bs;
            let run_end = (key(end - 1).index * bs + bs).min(file.version.size);
            // Every block of the run that this call loads shares the one read.
            let run = fetch(run_start..run_end)
                .map(|r| r.map_err(Arc::new))
                .shared();
            let several = end - i > 1;
            for (k, block) in blocks.iter_mut().enumerate().take(end).skip(i) {
                let run = run.clone();
                let offset = (k - i) as u64 * bs;
                let entry = self
                    .cache
                    .entry(key(k))
                    .or_try_insert_with(async move {
                        let run = run.await?;
                        Ok(slice_block(&run, offset, bs, several))
                    })
                    .await
                    .map_err(|e: Arc<Arc<io::Error>>| (*e).clone())?;
                let counter = if entry.is_fresh() {
                    &self.misses
                } else {
                    &self.hits
                };
                counter.fetch_add(1, Ordering::Relaxed);
                *block = Some(entry.into_value());
            }
            i = end;
        }
        let blocks: Vec<Bytes> = blocks.into_iter().flatten().collect();
        copy_out(
            &blocks,
            (range.start - first * bs) as usize,
            range_len(&range),
        )
    }
}

fn range_len(range: &Range<u64>) -> usize {
    (range.end - range.start) as usize
}

/// The block at `offset` of a run read. A run of several blocks is copied, so that each
/// cached block holds only its own bytes.
fn slice_block(run: &Bytes, offset: u64, bs: u64, several: bool) -> Bytes {
    let start = (offset as usize).min(run.len());
    let end = ((offset + bs) as usize).min(run.len());
    if several {
        Bytes::copy_from_slice(&run[start..end])
    } else {
        run.slice(start..end)
    }
}

/// Copies `len` bytes from `offset` of the first block on into one buffer.
fn copy_out(blocks: &[Bytes], offset: usize, len: usize) -> Result<Bytes, Arc<io::Error>> {
    let available: usize = blocks.iter().map(Bytes::len).sum();
    if offset + len > available {
        return Err(Arc::new(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected {} bytes, read {available}", offset + len),
        )));
    }
    if blocks[0].len() >= offset + len {
        return Ok(blocks[0].slice(offset..offset + len));
    }
    let mut out = BytesMut::with_capacity(len);
    let mut skip = offset;
    for block in blocks {
        let part = &block[skip.min(block.len())..];
        skip = skip.saturating_sub(block.len());
        let take = part.len().min(len - out.len());
        out.extend_from_slice(&part[..take]);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::super::CachedLocalOptions;
    use super::super::handles::HandleCache;
    use super::*;

    #[tokio::test]
    async fn cache_stays_within_limit() {
        const B: u64 = 64 << 10;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, vec![3u8; 8 << 20]).unwrap();
        let handles = HandleCache::new(&CachedLocalOptions::default(), None);
        let file = handles.get(&p).await.unwrap();
        let path = Arc::new(p);
        let blocks = BlockCache::new(B, 1 << 20);
        for i in 0..(8 << 20) / B {
            let f = file.clone();
            let got = blocks
                .read(&path, &file, i * B..i * B + 100, |r| {
                    let f = f.clone();
                    async move {
                        let mut buf = vec![0u8; (r.end - r.start) as usize];
                        super::super::read::read_exact_at(&f.file, &mut buf, r.start)?;
                        Ok(Bytes::from(buf))
                    }
                })
                .await
                .unwrap();
            assert_eq!(got.len(), 100);
        }
        blocks.cache.run_pending_tasks().await;
        assert!(
            blocks.weighted_size() <= 1 << 20,
            "{}",
            blocks.weighted_size()
        );
        assert_eq!(blocks.counters(), (0, 128));
    }

    #[test]
    fn copy_out_spans_blocks() {
        let blocks = [
            Bytes::from_static(b"abcd"),
            Bytes::from_static(b"efgh"),
            Bytes::from_static(b"ij"),
        ];
        assert_eq!(&copy_out(&blocks, 1, 2).unwrap()[..], b"bc");
        assert_eq!(&copy_out(&blocks, 3, 6).unwrap()[..], b"defghi");
        assert_eq!(&copy_out(&blocks, 0, 10).unwrap()[..], b"abcdefghij");
        assert!(copy_out(&blocks, 3, 8).is_err());
    }
}
