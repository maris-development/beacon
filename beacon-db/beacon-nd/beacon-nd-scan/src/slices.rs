//! File slices: how one file is read by several partitions.
//!
//! A slice is the same file with a part of its chunks. Every slice builds the
//! same grid and takes its own range of the chunk numbers, so the slices of a
//! file read each chunk one time.

use std::num::NonZeroU32;
use std::ops::Range;

use datafusion::datasource::{listing::PartitionedFile, physical_plan::FileGroup};

/// Slice `index` of `count` of one file. It travels in the extensions of the entry.
///
/// The count is at least 1, and the index is below the count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NdSlice {
    index: u32,
    count: NonZeroU32,
}

impl NdSlice {
    /// The whole file: slice 0 of 1.
    const WHOLE: Self = Self {
        index: 0,
        count: NonZeroU32::MIN,
    };

    /// Slice `index` of `count`. `None` when `count` is 0 or `index` is not below `count`.
    pub(crate) fn new(index: u32, count: u32) -> Option<Self> {
        let count = NonZeroU32::new(count)?;
        (index < count.get()).then_some(Self { index, count })
    }

    /// The slice of `file`. An entry with no slice is the whole file.
    pub(crate) fn of(file: &PartitionedFile) -> Self {
        file.extension::<Self>().copied().unwrap_or(Self::WHOLE)
    }

    /// The number of this slice, from 0.
    pub(crate) fn index(self) -> u32 {
        self.index
    }

    /// How many slices the file has.
    pub(crate) fn count(self) -> u32 {
        self.count.get()
    }

    /// The chunks of this slice, out of `chunks` in the file.
    pub(crate) fn range(self, chunks: usize) -> Range<usize> {
        let (index, count) = (self.index as usize, self.count() as usize);
        (index * chunks / count)..((index + 1) * chunks / count)
    }
}

/// How many entries the scan aims at for each partition.
///
/// Sibling partitions share their unopened entries. More entries than
/// partitions let a partition that ends early take more work.
const ENTRIES_PER_PARTITION: usize = 4;

/// Cut the unsliced entries into slices, each file in proportion to its size.
///
/// The aim is `ENTRIES_PER_PARTITION * target` entries of equal size. A file
/// smaller than one such entry stays whole, and no file gets more slices than the aim.
pub(crate) fn slice_files(entries: Vec<PartitionedFile>, target: usize) -> Vec<PartitionedFile> {
    let unsliced = || {
        entries
            .iter()
            .filter(|entry| entry.extension::<NdSlice>().is_none())
    };
    let files = unsliced().count();
    if files == 0 {
        return entries;
    }
    let aim = ENTRIES_PER_PARTITION.saturating_mul(target).max(1);
    let total: u128 = unsliced()
        .map(|entry| u128::from(entry.object_meta.size))
        .sum();

    let mut sliced = Vec::with_capacity(entries.len().max(aim));
    for entry in entries {
        if entry.extension::<NdSlice>().is_some() {
            sliced.push(entry);
            continue;
        }
        let count = slice_count(entry.object_meta.size, total, files, aim);
        if count == 1 {
            sliced.push(entry);
            continue;
        }
        // Each index is below the count, so each slice is valid.
        for slice in (0..count).filter_map(|index| NdSlice::new(index, count)) {
            let mut slice = entry.clone().with_extension(slice);
            // The statistics describe the whole file, not one slice of it.
            slice.statistics = None;
            sliced.push(slice);
        }
    }
    sliced
}

/// The slices of one file of `size` bytes, out of `files` files of `total` bytes.
fn slice_count(size: u64, total: u128, files: usize, aim: usize) -> u32 {
    // The share of the file in the aim, to the nearest whole number.
    let count = (u128::from(size) * aim as u128 + total / 2)
        .checked_div(total)
        // No size is known, so each file gets the same count.
        .unwrap_or(aim.div_ceil(files) as u128);
    count.clamp(1, aim as u128).min(u128::from(u32::MAX)) as u32
}

/// Sort `entries` by weight, the largest entry first. Equal entries keep their order.
///
/// Sibling partitions take their work from the front of one queue. The large
/// entries start first, and the small entries fill the gaps at the end.
pub(crate) fn largest_first(mut entries: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
    entries.sort_by_key(|entry| std::cmp::Reverse(weight(entry)));
    entries
}

/// Spread `entries` over exactly `target` groups, the largest entry first.
pub(crate) fn spread_files(entries: Vec<PartitionedFile>, target: usize) -> Vec<FileGroup> {
    let entries = largest_first(entries);

    let mut groups: Vec<(u64, Vec<PartitionedFile>)> = vec![(0, Vec::new()); target.max(1)];
    for entry in entries {
        let lightest = groups
            .iter_mut()
            .min_by_key(|(size, _)| *size)
            .expect("at least one group");
        lightest.0 += weight(&entry).max(1);
        lightest.1.push(entry);
    }
    groups
        .into_iter()
        .map(|(_, files)| FileGroup::new(files))
        .collect()
}

/// The share of its file that one entry reads.
fn weight(entry: &PartitionedFile) -> u64 {
    entry.object_meta.size / u64::from(NdSlice::of(entry).count())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn file(path: &str, size: u64) -> PartitionedFile {
        PartitionedFile::new(path, size)
    }

    #[test]
    fn the_ranges_of_the_slices_cover_each_chunk_once() {
        for (chunks, count) in [(10usize, 4u32), (3, 8), (0, 2), (7, 1)] {
            let mut seen = Vec::new();
            for index in 0..count {
                seen.extend(NdSlice::new(index, count).unwrap().range(chunks));
            }
            assert_eq!(
                seen,
                (0..chunks).collect::<Vec<_>>(),
                "{chunks} chunks, {count} slices"
            );
        }
    }

    #[test]
    fn a_file_with_no_slice_is_the_whole_file() {
        let slice = NdSlice::of(&file("a.nc", 10));
        assert_eq!(slice, NdSlice::new(0, 1).unwrap());
        assert_eq!(slice.range(5), 0..5);
    }

    #[test]
    fn a_slice_with_no_count_or_an_index_past_the_count_is_not_made() {
        assert_eq!(NdSlice::new(0, 0), None);
        assert_eq!(NdSlice::new(4, 4), None);
        assert!(NdSlice::new(3, 4).is_some());
    }

    /// The slice count of each path, in the order of the first entry of the path.
    fn counts(entries: &[PartitionedFile]) -> Vec<(String, u32)> {
        let mut counts: Vec<(String, u32)> = Vec::new();
        for entry in entries {
            let path = entry.object_meta.location.to_string();
            if !counts.iter().any(|(seen, _)| *seen == path) {
                counts.push((path, NdSlice::of(entry).count()));
            }
        }
        counts
    }

    #[test]
    fn files_of_equal_size_share_the_aim() {
        let sliced = slice_files(vec![file("a.nc", 100), file("b.nc", 100)], 2);

        // The aim is 4 * 2 = 8 entries: 4 slices for each file.
        assert_eq!(sliced.len(), 8);
        for entry in &sliced {
            assert_eq!(NdSlice::of(entry).count(), 4);
            assert!(entry.statistics.is_none());
            assert!(entry.range.is_none());
        }
        let of_a: Vec<u32> = sliced
            .iter()
            .filter(|entry| entry.object_meta.location.as_ref() == "a.nc")
            .map(|entry| NdSlice::of(entry).index())
            .collect();
        assert_eq!(of_a, vec![0, 1, 2, 3]);
    }

    #[test]
    fn a_lone_file_gets_the_full_aim() {
        let sliced = slice_files(vec![file("a.nc", 100)], 8);
        assert_eq!(sliced.len(), 32);
        assert_eq!(counts(&sliced), vec![("a.nc".to_string(), 32)]);
    }

    #[test]
    fn enough_files_of_equal_size_are_not_sliced() {
        let entries: Vec<PartitionedFile> = (0..32).map(|i| file(&format!("{i}.nc"), 10)).collect();
        let kept = slice_files(entries, 4);
        assert_eq!(kept.len(), 32);
        assert!(
            kept.iter()
                .all(|entry| entry.extension::<NdSlice>().is_none())
        );
    }

    #[test]
    fn a_large_file_among_many_small_files_is_sliced() {
        let mut entries = vec![file("big.nc", 50_000)];
        entries.extend((0..40).map(|i| file(&format!("s{i}.nc"), 100)));

        let sliced = slice_files(entries, 8);

        // The aim is 32 entries of 1687.5 bytes: round(50000 / 1687.5) = 30.
        let counts = counts(&sliced);
        assert_eq!(counts[0], ("big.nc".to_string(), 30));
        assert!(counts[1..].iter().all(|(_, count)| *count == 1));
        assert_eq!(sliced.len(), 30 + 40);
        assert!(
            sliced
                .iter()
                .filter(|entry| entry.object_meta.location.as_ref() != "big.nc")
                .all(|entry| entry.extension::<NdSlice>().is_none())
        );
    }

    #[test]
    fn files_of_unknown_size_get_the_same_count() {
        let sliced = slice_files(vec![file("a.nc", 0), file("b.nc", 0), file("c.nc", 0)], 2);

        // The aim is 8 entries: ceil(8 / 3) = 3 slices for each file.
        assert_eq!(sliced.len(), 9);
        assert!(sliced.iter().all(|entry| NdSlice::of(entry).count() == 3));
    }

    #[test]
    fn an_empty_file_among_sized_files_stays_whole() {
        let sliced = slice_files(vec![file("a.nc", 100), file("empty.nc", 0)], 2);
        assert_eq!(
            counts(&sliced),
            vec![("a.nc".to_string(), 8), ("empty.nc".to_string(), 1)]
        );
    }

    #[test]
    fn a_huge_size_does_not_overflow() {
        let sliced = slice_files(vec![file("a.nc", u64::MAX), file("b.nc", u64::MAX)], 2);
        assert_eq!(sliced.len(), 8);
    }

    #[test]
    fn a_slice_is_not_sliced_again() {
        let once = slice_files(vec![file("a.nc", 100)], 4);
        let twice = slice_files(once.clone(), 16);
        assert_eq!(twice.len(), once.len());
    }

    #[test]
    fn no_entry_gives_no_slice() {
        assert!(slice_files(vec![], 8).is_empty());
    }

    #[test]
    fn the_largest_entry_comes_first_and_the_slices_keep_their_order() {
        let entries = slice_files(
            vec![file("s1.nc", 100), file("big.nc", 1500), file("s2.nc", 100)],
            1,
        );

        let order: Vec<(String, u32)> = largest_first(entries)
            .iter()
            .map(|entry| {
                (
                    entry.object_meta.location.to_string(),
                    NdSlice::of(entry).index(),
                )
            })
            .collect();

        // The aim is 4 entries of 425 bytes: big.nc gets 4 slices of 375 bytes.
        let expected = [
            ("big.nc", 0),
            ("big.nc", 1),
            ("big.nc", 2),
            ("big.nc", 3),
            ("s1.nc", 0),
            ("s2.nc", 0),
        ]
        .map(|(path, index)| (path.to_string(), index));
        assert_eq!(order, expected);
    }

    #[test]
    fn the_spread_gives_exactly_the_target_groups() {
        let groups = spread_files(
            vec![file("a.nc", 30), file("b.nc", 20), file("c.nc", 10)],
            5,
        );

        assert_eq!(groups.len(), 5);
        let paths: HashSet<String> = groups
            .iter()
            .flat_map(|group| group.files())
            .map(|entry| entry.object_meta.location.to_string())
            .collect();
        assert_eq!(paths.len(), 3);
        // Three files on five groups: one file in each of three groups.
        assert_eq!(
            groups
                .iter()
                .filter(|group| group.files().len() == 1)
                .count(),
            3
        );
    }

    #[test]
    fn the_spread_balances_by_size() {
        let groups = spread_files(
            vec![
                file("big.nc", 100),
                file("s1.nc", 10),
                file("s2.nc", 10),
                file("s3.nc", 10),
            ],
            2,
        );

        let sizes: Vec<u64> = groups
            .iter()
            .map(|group| {
                group
                    .files()
                    .iter()
                    .map(|entry| entry.object_meta.size)
                    .sum()
            })
            .collect();
        assert_eq!(sizes.iter().max(), Some(&100));
        assert_eq!(sizes.iter().min(), Some(&30));
    }

    #[test]
    fn no_entry_spreads_to_empty_groups() {
        let groups = spread_files(vec![], 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.files().is_empty()));
    }
}
