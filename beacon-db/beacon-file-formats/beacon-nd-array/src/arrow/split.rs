//! A contiguous slice of the chunk list a dataset reads in.
//!
//! A reader cuts a dataset into a chunk list before it reads anything, and it
//! reads that list in order. The list is a pure function of the dataset layout,
//! the column projection and the batch size, so two readers that agree on those
//! three build the same list, in the same order.
//!
//! That is what makes one file readable by several partitions at once. Each
//! partition builds the whole list and reads one slice of it. A [`ChunkSplit`]
//! names the slice.
//!
//! # Where the slice comes from
//!
//! DataFusion splits a file by **byte range**, the same way it splits a Parquet
//! file (see [`FileGroupPartitioner`]). A byte range of a NetCDF or HDF5 file is
//! not itself a readable file, so the range is not read as bytes. It is read as
//! a *fraction*: [`ChunkSplit::ordinals`] maps the range onto the chunk list.
//!
//! The map is monotone, it sends byte 0 to chunk 0, and it sends the last byte
//! to the last chunk. Byte ranges that tile the file therefore give chunk ranges
//! that tile the list. No chunk is read twice, and no chunk is missed. This
//! holds for any file size and any chunk count, including a file split into more
//! parts than it has chunks (the surplus parts read nothing).
//!
//! [`FileGroupPartitioner`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_groups/struct.FileGroupPartitioner.html

use std::ops::Range;

/// One partition's share of a dataset's chunk list.
///
/// Build one with [`ChunkSplit::from_byte_range`], and resolve it against a
/// chunk count with [`ChunkSplit::ordinals`]. A `None` split reads the whole
/// list; see the module docs for why the two steps are separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSplit {
    /// First byte of the file this split owns.
    start: u64,
    /// One past the last byte this split owns.
    end: u64,
    /// Size of the whole file, in bytes. Never zero.
    file_size: u64,
}

impl ChunkSplit {
    /// The split that covers `range` of a file of `file_size` bytes.
    ///
    /// Returns `None` when there is nothing to split: an empty file, a file of
    /// unknown size, or a range that already covers the file. A caller that gets
    /// `None` reads the whole chunk list.
    ///
    /// An **empty** range is not the same as no range. It yields a split that
    /// owns no chunks, so a partition that was handed nothing reads nothing.
    pub fn from_byte_range(range: Range<u64>, file_size: u64) -> Option<Self> {
        if file_size == 0 {
            return None;
        }
        if range.start == 0 && range.end >= file_size {
            return None;
        }

        let start = range.start.min(file_size);
        let end = range.end.min(file_size).max(start);
        Some(Self {
            start,
            end,
            file_size,
        })
    }

    /// The chunks this split owns, out of a list of `chunk_count` chunks.
    ///
    /// The result is a half-open range into the chunk list. It is empty when the
    /// split covers less than one chunk.
    pub fn ordinals(&self, chunk_count: usize) -> Range<usize> {
        // `file_size` is never zero, so the division is safe. `u128` keeps the
        // product exact for any file size and any chunk count.
        let scale = |offset: u64| -> usize {
            ((offset as u128 * chunk_count as u128) / self.file_size as u128) as usize
        };

        let start = scale(self.start).min(chunk_count);
        let end = scale(self.end).max(start).min(chunk_count);
        start..end
    }

    /// True when this split owns the first byte of the file.
    ///
    /// A count that belongs to the file as a whole rather than to one chunk is
    /// added by this split alone. The total over every split of a file then
    /// equals the count an unsplit read reports.
    ///
    /// The test is on bytes, not on chunks, so exactly one split of a file leads
    /// even when the file holds no chunks at all.
    pub fn leads(&self) -> bool {
        self.start == 0
    }
}

/// The chunks `split` owns, out of a list of `count` chunks.
///
/// A `None` split owns the whole list. This is the one place a reader turns a
/// split into indices, so every reader slices its list the same way.
pub(crate) fn split_ordinals(split: Option<ChunkSplit>, count: usize) -> Range<usize> {
    match split {
        Some(split) => split.ordinals(count),
        None => 0..count,
    }
}

/// True when `split` owns the first byte of the file. A `None` split owns it,
/// because it owns the whole file.
pub(crate) fn split_leads(split: Option<ChunkSplit>) -> bool {
    match split {
        Some(split) => split.leads(),
        None => true,
    }
}

/// Byte ranges that tile a file, the way DataFusion's file-group partitioner
/// produces them for `parts` partitions.
#[cfg(test)]
pub(crate) fn byte_tiling(file_size: u64, parts: u64) -> Vec<Range<u64>> {
    let step = file_size.div_ceil(parts);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < file_size {
        let end = (start + step).min(file_size);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

/// The same tiling, resolved to splits. A single part gets `None`: it reads the
/// whole file, so there is nothing to split.
///
/// Every test that reads a dataset in parts goes through this, so they all split
/// the way a real scan does. The file size is arbitrary. The map reads a
/// fraction of the file, not a layout.
#[cfg(test)]
pub(crate) fn byte_tiling_splits(parts: u64) -> Vec<Option<ChunkSplit>> {
    const FILE_SIZE: u64 = 1_000;

    byte_tiling(FILE_SIZE, parts)
        .into_iter()
        .map(|range| ChunkSplit::from_byte_range(range, FILE_SIZE))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole file, and a file of unknown size, need no split.
    #[test]
    fn a_range_that_covers_the_file_is_not_a_split() {
        assert_eq!(ChunkSplit::from_byte_range(0..100, 100), None);
        // A partitioner that rounds the last range up still covers the file.
        assert_eq!(ChunkSplit::from_byte_range(0..128, 100), None);
        assert_eq!(ChunkSplit::from_byte_range(0..0, 0), None);
    }

    /// An empty range owns nothing. It must not fall back to the whole list.
    #[test]
    fn an_empty_range_owns_no_chunks() {
        let split = ChunkSplit::from_byte_range(60..60, 100).expect("a split");
        assert!(split.ordinals(10).is_empty());
        assert!(!split.leads());
    }

    /// The splits of a file tile its chunk list: every chunk once, in order.
    ///
    /// This is the property the whole design rests on. It must hold for a chunk
    /// count that divides the split count, one that does not, and one that is
    /// smaller than the split count.
    #[test]
    fn the_splits_of_a_file_tile_its_chunk_list() {
        for file_size in [1_u64, 7, 100, 4096, 1 << 30, u32::MAX as u64 + 3] {
            for parts in 1_u64..=8 {
                for chunk_count in [0_usize, 1, 2, 3, 7, 8, 64, 1_000] {
                    let mut covered = Vec::new();
                    let mut leaders = 0;

                    for range in byte_tiling(file_size, parts) {
                        let split = ChunkSplit::from_byte_range(range, file_size);
                        let owned = split_ordinals(split, chunk_count);
                        if split_leads(split) {
                            leaders += 1;
                        }
                        covered.extend(owned);
                    }

                    let expected: Vec<usize> = (0..chunk_count).collect();
                    assert_eq!(
                        covered, expected,
                        "file_size={file_size} parts={parts} chunks={chunk_count}"
                    );
                    assert_eq!(
                        leaders, 1,
                        "exactly one split leads: file_size={file_size} parts={parts} chunks={chunk_count}"
                    );
                }
            }
        }
    }

    /// Equal byte ranges give equal chunk counts, whatever the compression did
    /// to the bytes in between. The map reads a fraction, not a layout.
    #[test]
    fn equal_byte_ranges_give_equal_chunk_counts() {
        let file_size = 1_000;
        let chunk_count = 100;
        for range in byte_tiling(file_size, 4) {
            let split = ChunkSplit::from_byte_range(range, file_size).expect("a split");
            assert_eq!(split.ordinals(chunk_count).len(), 25);
        }
    }

    /// A range past the end of the file is clamped, not wrapped.
    #[test]
    fn a_range_past_the_end_is_clamped() {
        let split = ChunkSplit::from_byte_range(90..10_000, 100).expect("a split");
        assert_eq!(split.ordinals(10), 9..10);
    }
}
