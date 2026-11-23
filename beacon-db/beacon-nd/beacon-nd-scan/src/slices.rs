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

/// Cut the unsliced entries into slices, when there are fewer of them than `target`.
pub(crate) fn slice_files(entries: Vec<PartitionedFile>, target: usize) -> Vec<PartitionedFile> {
    let files = entries
        .iter()
        .filter(|entry| entry.extension::<NdSlice>().is_none())
        .count();
    if files == 0 || files >= target {
        return entries;
    }

    let count = target.div_ceil(files) as u32;
    let mut sliced = Vec::with_capacity(entries.len() * count as usize);
    for entry in entries {
        if entry.extension::<NdSlice>().is_some() {
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

/// Spread `entries` over exactly `target` groups, the largest entry first.
pub(crate) fn spread_files(mut entries: Vec<PartitionedFile>, target: usize) -> Vec<FileGroup> {
    entries.sort_by_key(|entry| std::cmp::Reverse(weight(entry)));

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

    #[test]
    fn fewer_files_than_partitions_are_cut_into_slices() {
        let sliced = slice_files(vec![file("a.nc", 100), file("b.nc", 100)], 8);

        // K = ceil(8 / 2) = 4 slices for each file.
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
    fn enough_files_are_not_sliced() {
        let entries: Vec<PartitionedFile> = (0..8).map(|i| file(&format!("{i}.nc"), 10)).collect();
        let kept = slice_files(entries, 4);
        assert_eq!(kept.len(), 8);
        assert!(
            kept.iter()
                .all(|entry| entry.extension::<NdSlice>().is_none())
        );
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
