//! Dealing a scan's file slices to partitions.
//!
//! DataFusion's [`FileGroupPartitioner`] cuts the files of a scan into byte
//! ranges and hands each partition one contiguous run of them. That is the right
//! shape for a format whose cost is uniform across a file: one partition reads
//! one region, and the reads stay sequential.
//!
//! It is the wrong shape when the cost is not uniform, because a partition's run
//! is one region of the file and skew follows regions. The clearest case is a
//! predicate on the outermost dimension. An nd chunk list is C-ordered, so a
//! grid on `(time, lat, lon)` lists every chunk of the first time step before
//! any of the second. `WHERE time > …` then prunes a prefix of the list: with
//! contiguous runs the early partitions have nothing to read and the late ones
//! have everything, and the scan takes as long as its slowest partition.
//!
//! [`interleaved_file_groups`] cuts each file that is worth cutting into several
//! times as many slices as there are partitions, then deals them round-robin, so
//! each partition holds slices spread across every region of every file. A
//! prefix that prunes away now costs every partition the same.
//!
//! It also differs from DataFusion's partitioner in where it applies the size
//! test: per file rather than on the scan total. A share pays to open one file,
//! so one file is what has to be large enough to earn the share.
//!
//! # What it costs
//!
//! A partition holds several slices instead of one, so its opener runs several
//! times: once per slice, each opening the same file. A reader cache with
//! single-flight admission makes the extra opens cache hits, but each slice
//! still pays for its own stream setup, which for the flat read path includes
//! computing the predicate masks again.
//!
//! Reads also stop being one sequential run per partition. That matters more on
//! a local disk than on object storage, where a chunk is a range request either
//! way.
//!
//! So this is a trade, and [`SLICES_PER_PARTITION`] is where it is priced.

use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner};

/// How many slices of a scan each partition is dealt.
///
/// One slice per partition is DataFusion's own behaviour: contiguous runs, no
/// interleaving, and the skew described in the module docs. Raising it spreads
/// each partition over more of the file and costs one more opener run per
/// partition per file.
///
/// Four is a deliberate middle: it splits the common outer-dimension prefix
/// across every partition while keeping each partition's reads to a handful of
/// runs rather than one per chunk.
pub const SLICES_PER_PARTITION: usize = 4;

/// Cut every file over `min_split_size` into
/// `target_partitions * SLICES_PER_PARTITION` slices, and deal the result
/// round-robin.
///
/// The size test is **per file**, not on the scan total. A file is what a share
/// pays to open, so a file is what has to be worth splitting: a thousand small
/// files are not one large one, however they add up. DataFusion's own
/// partitioner tests the total, which would slice every file of a large
/// collection into shares that each re-open it for a fraction of its rows.
///
/// A file at or under the size is passed through whole. It still lands in some
/// partition, alongside the slices of its larger neighbours.
///
/// Returns `None` when no file is worth splitting, which leaves the scan's
/// existing grouping alone — the listing has already spread its files over
/// `target_partitions`, and that is the right answer when none of them can be
/// divided.
///
/// `preserve_order` falls back to DataFusion's contiguous deal. A partition that
/// must emit rows in order cannot hold slices that skip over one another, since
/// the rows between them belong to a different partition.
pub fn interleaved_file_groups(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_split_size: u64,
    preserve_order: bool,
) -> Option<Vec<FileGroup>> {
    if preserve_order || target_partitions <= 1 {
        return FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(min_split_size as usize)
            .with_preserve_order_within_groups(preserve_order)
            .repartition_file_groups(file_groups);
    }

    let slices_per_file = target_partitions * SLICES_PER_PARTITION;
    let mut pieces = Vec::new();
    let mut split_any = false;

    for file in file_groups.iter().flat_map(FileGroup::iter) {
        let (start, end) = file.range();
        if end.saturating_sub(start) <= min_split_size {
            // Too small to be worth a share each. Keep it whole.
            pieces.push(file.clone());
            continue;
        }

        // Equal slices over the file's own range, so each partition takes the
        // same count of chunks from it. The opener resolves a slice against the
        // chunk list, so equal byte ranges mean equal work whatever the
        // compression did in between.
        let step = (end - start).div_ceil(slices_per_file as u64);
        let mut at = start;
        while at < end {
            let stop = (at + step).min(end);
            pieces.push(file.clone().with_range(at as i64, stop as i64));
            at = stop;
        }
        split_any = true;
    }

    if !split_any {
        return None;
    }

    // Deal round-robin. A file's slices are `target_partitions` apart in this
    // list, so each partition takes one from each region of it.
    let mut dealt: Vec<Vec<_>> = vec![Vec::new(); target_partitions];
    for (index, piece) in pieces.into_iter().enumerate() {
        dealt[index % target_partitions].push(piece);
    }

    // A scan with fewer pieces than partitions leaves the tail empty. Keep the
    // empty groups: dropping them would change the partition count the caller
    // asked for, and an empty group is a partition that finishes at once.
    Some(dealt.into_iter().map(FileGroup::new).collect())
}

/// Give every partition the files worth sharing, and one each of the rest.
///
/// A file over `min_share_size` goes into *every* partition's group, carrying
/// the mark `mark` builds for it. Nothing about it is divided here: the
/// partitions divide it as they read it, by taking subsets from one queue they
/// share. Balance then follows completion rather than a guess made at plan time.
///
/// A file at or under the size is left whole and dealt to one partition. Every
/// partition opening it to take a subset or two would cost more than it returns,
/// and the listing has already spread these across the scan.
///
/// Returns `None` when no file is worth sharing, which leaves the scan's
/// grouping alone.
///
/// # The mark matters
///
/// A shared file is in every group. A reader that ignores the mark and reads it
/// whole therefore returns every row once per partition. The mark is not a hint
/// but an instruction, and the opener that receives it must refuse to read the
/// file any other way.
pub fn shared_file_groups<M>(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_share_size: u64,
    mark: M,
) -> Option<Vec<FileGroup>>
where
    M: Fn(usize) -> Arc<dyn std::any::Any + Send + Sync>,
{
    if target_partitions <= 1 {
        return None;
    }

    let mut shared = Vec::new();
    let mut whole = Vec::new();
    for file in file_groups.iter().flat_map(FileGroup::iter) {
        let (start, end) = file.range();
        if end.saturating_sub(start) > min_share_size {
            shared.push(file.clone());
        } else {
            whole.push(file.clone());
        }
    }

    if shared.is_empty() {
        return None;
    }

    let mut dealt: Vec<Vec<_>> = vec![Vec::new(); target_partitions];

    // Every partition holds every shared file, and knows how many others do.
    for file in shared {
        for group in dealt.iter_mut() {
            let mut copy = file.clone();
            copy.extensions = Some(mark(target_partitions));
            group.push(copy);
        }
    }

    // The rest are spread one per partition, as the listing would have.
    for (index, file) in whole.into_iter().enumerate() {
        dealt[index % target_partitions].push(file);
    }

    Some(dealt.into_iter().map(FileGroup::new).collect())
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::listing::PartitionedFile;

    use super::*;

    /// One file of `size` bytes, as the single group a listing produces.
    fn one_file(size: u64) -> Vec<FileGroup> {
        vec![FileGroup::new(vec![PartitionedFile::new("one.nc", size)])]
    }

    /// The byte ranges of a group, in the order the partition reads them.
    fn ranges(group: &FileGroup) -> Vec<(i64, i64)> {
        group
            .iter()
            .map(|file| {
                let range = file.range.as_ref().expect("a slice carries a range");
                (range.start, range.end)
            })
            .collect()
    }

    /// The deal covers every byte once, whatever the partition count.
    ///
    /// This is the property the split rests on: a slice maps to a run of the
    /// chunk list, so bytes covered twice are rows returned twice.
    #[test]
    fn the_deal_covers_every_byte_exactly_once() {
        const SIZE: u64 = 1_000_000;

        for target_partitions in 1..=8 {
            let groups = interleaved_file_groups(&one_file(SIZE), target_partitions, 0, false)
                .expect("a file this size splits");

            let mut covered: Vec<(i64, i64)> = groups.iter().flat_map(ranges).collect();
            covered.sort_unstable();

            let mut next = 0;
            for (start, end) in &covered {
                assert_eq!(*start, next, "target_partitions={target_partitions}");
                next = *end;
            }
            assert_eq!(
                next as u64, SIZE,
                "target_partitions={target_partitions}: the deal must cover the file"
            );
        }
    }

    /// Each partition holds slices from across the file, not one region of it.
    ///
    /// The contiguous deal gives partition 0 the first quarter and nothing else.
    /// This one gives it a slice from each quarter, so a predicate that prunes
    /// one region costs every partition the same.
    #[test]
    fn each_partition_holds_slices_from_across_the_file() {
        const SIZE: u64 = 1_000_000;
        const PARTITIONS: usize = 4;

        let groups = interleaved_file_groups(&one_file(SIZE), PARTITIONS, 0, false)
            .expect("a file this size splits");

        assert_eq!(groups.len(), PARTITIONS);
        for (index, group) in groups.iter().enumerate() {
            assert_eq!(
                group.len(),
                SLICES_PER_PARTITION,
                "partition {index} should hold {SLICES_PER_PARTITION} slices"
            );

            // The slices of one partition are `PARTITIONS` apart in the deal, so
            // they land in different quarters of the file.
            let quarters: Vec<u64> = ranges(group)
                .iter()
                .map(|(start, _)| (*start as u64) * 4 / SIZE)
                .collect();
            assert_eq!(
                quarters,
                vec![0, 1, 2, 3],
                "partition {index} should hold one slice per quarter"
            );
        }
    }

    /// A partition reads its own slices in file order.
    ///
    /// Interleaving reorders rows *between* partitions, never within one. A
    /// partition that read its slices out of order would break a scan that
    /// relies on file order within a group.
    #[test]
    fn a_partition_reads_its_slices_in_file_order() {
        let groups = interleaved_file_groups(&one_file(1_000_000), 3, 0, false)
            .expect("a file this size splits");

        for group in &groups {
            let starts: Vec<i64> = ranges(group).iter().map(|(start, _)| *start).collect();
            let mut sorted = starts.clone();
            sorted.sort_unstable();
            assert_eq!(starts, sorted, "a partition reads front to back");
        }
    }

    /// An ordered scan keeps DataFusion's contiguous deal.
    #[test]
    fn an_ordered_scan_keeps_contiguous_runs() {
        let groups = interleaved_file_groups(&one_file(1_000_000), 4, 0, true)
            .expect("a file this size splits");

        for group in &groups {
            assert_eq!(
                group.len(),
                1,
                "an ordered partition holds one contiguous run"
            );
        }
    }

/// A file worth sharing lands in every partition, marked.
    #[test]
    fn a_shared_file_lands_in_every_partition() {
        const MIN: u64 = 8 * 1024 * 1024;
        const PARTITIONS: usize = 4;

        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        let dealt = shared_file_groups(&groups, PARTITIONS, MIN, |consumers| Arc::new(consumers))
            .expect("a large file is shared");

        assert_eq!(dealt.len(), PARTITIONS);
        for group in &dealt {
            assert_eq!(group.len(), 1, "each partition holds the file once");
            let file = group.iter().next().unwrap();
            assert!(file.range.is_none(), "a shared file is not divided");
            let consumers = file
                .extensions
                .as_ref()
                .and_then(|ext| (ext.as_ref() as &dyn std::any::Any).downcast_ref::<usize>())
                .copied();
            assert_eq!(
                consumers,
                Some(PARTITIONS),
                "the mark says how many partitions hold it"
            );
        }
    }

    /// Small files are dealt one per partition, unmarked and undivided.
    #[test]
    fn small_files_are_dealt_whole_and_unmarked() {
        const MIN: u64 = 8 * 1024 * 1024;

        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..4).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));
        let groups = vec![FileGroup::new(files)];

        let dealt = shared_file_groups(&groups, 4, MIN, |consumers| Arc::new(consumers))
            .expect("the large file is shared");

        let small: Vec<_> = dealt
            .iter()
            .flat_map(|group| group.iter())
            .filter(|file| file.object_meta.location.to_string().starts_with("small-"))
            .collect();

        assert_eq!(small.len(), 4, "each small file appears once in the scan");
        for file in small {
            assert!(file.extensions.is_none(), "a whole file carries no mark");
            assert!(file.range.is_none(), "a whole file is not divided");
        }
    }

    /// A scan with nothing worth sharing is left alone.
    #[test]
    fn a_scan_with_no_large_file_is_left_alone() {
        const MIN: u64 = 8 * 1024 * 1024;

        let files: Vec<_> = (0..200)
            .map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024 * 1024))
            .collect();
        let groups = vec![FileGroup::new(files)];

        assert!(
            shared_file_groups(&groups, 4, MIN, |consumers| Arc::new(consumers)).is_none(),
            "200 MB of 1 MB files is still no file worth sharing"
        );
    }

    /// A file too small to divide is left alone.
    #[test]
    fn a_file_below_the_minimum_is_not_divided() {
        assert!(interleaved_file_groups(&one_file(1_000), 4, 10 * 1024 * 1024, false).is_none());
    }

    /// Many small files are not divided, however much they add up to.
    ///
    /// This is where the rule departs from DataFusion's, which tests the scan
    /// total: a thousand 1 MB files clear any total-based minimum, and every one
    /// of them would be cut into shares that each re-open it for a fraction of
    /// its rows. A share pays to open one file, so one file is what has to earn
    /// it. The listing has already spread these over the partitions.
    #[test]
    fn many_small_files_are_not_divided() {
        const MIN: u64 = 8 * 1024 * 1024;

        let files: Vec<_> = (0..200)
            .map(|index| PartitionedFile::new(format!("small-{index}.nc"), 1024 * 1024))
            .collect();
        let groups = vec![FileGroup::new(files)];

        assert!(
            interleaved_file_groups(&groups, 4, MIN, false).is_none(),
            "200 MB of 1 MB files must not be divided"
        );
    }

    /// In a mixed scan, only the large file is cut. The small ones ride along
    /// whole.
    #[test]
    fn only_the_large_file_of_a_mixed_scan_is_divided() {
        const MIN: u64 = 8 * 1024 * 1024;
        const LARGE: u64 = 64 * 1024 * 1024;

        let mut files = vec![PartitionedFile::new("large.nc", LARGE)];
        files.extend((0..3).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));
        let groups = vec![FileGroup::new(files)];

        let dealt = interleaved_file_groups(&groups, 4, MIN, false).expect("the large file splits");

        let mut whole = 0;
        let mut sliced = 0;
        for file in dealt.iter().flat_map(|group| group.iter()) {
            let name = file.object_meta.location.to_string();
            match file.range {
                None => {
                    assert!(name.starts_with("small-"), "{name} should have been sliced");
                    whole += 1;
                }
                Some(_) => {
                    assert_eq!(name, "large.nc", "{name} should have been left whole");
                    sliced += 1;
                }
            }
        }

        assert_eq!(whole, 3, "every small file rides along whole");
        assert_eq!(
            sliced,
            4 * SLICES_PER_PARTITION,
            "the large file is cut once per partition slice"
        );
    }
}
