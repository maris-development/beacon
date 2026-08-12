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
//! [`interleaved_file_groups`] keeps DataFusion's slicing and changes only the
//! deal. It asks for several times as many slices as there are partitions, then
//! deals them round-robin, so each partition holds slices spread across every
//! region of every file. A prefix that prunes away now costs every partition the
//! same.
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

/// Cut the scan into `target_partitions * SLICES_PER_PARTITION` slices and deal
/// them round-robin.
///
/// Returns `None` when there is nothing to repartition, matching
/// [`FileGroupPartitioner::repartition_file_groups`].
///
/// `preserve_order` falls back to DataFusion's contiguous deal. A partition that
/// must emit rows in order cannot hold slices that skip over one another, since
/// the rows between them belong to a different partition.
pub fn interleaved_file_groups(
    file_groups: &[FileGroup],
    target_partitions: usize,
    repartition_file_min_size: usize,
    preserve_order: bool,
) -> Option<Vec<FileGroup>> {
    let partitioner = FileGroupPartitioner::new()
        .with_repartition_file_min_size(repartition_file_min_size)
        .with_preserve_order_within_groups(preserve_order);

    if preserve_order || target_partitions <= 1 {
        return partitioner
            .with_target_partitions(target_partitions)
            .repartition_file_groups(file_groups);
    }

    // Ask for the finer cut. The slices come back in file order, one group per
    // slice, and they tile the scan exactly as the coarse cut would: this only
    // changes how many cuts there are, not where the bytes go.
    let slices = partitioner
        .with_target_partitions(target_partitions * SLICES_PER_PARTITION)
        .repartition_file_groups(file_groups)?;

    let mut dealt: Vec<Vec<_>> = vec![Vec::new(); target_partitions];
    for (index, slice) in slices.into_iter().enumerate() {
        dealt[index % target_partitions].extend(slice.into_inner());
    }

    // A scan with fewer slices than partitions leaves the tail empty. Keep the
    // empty groups: dropping them would change the partition count the caller
    // asked for, and an empty group is a partition that finishes at once.
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
            let groups = interleaved_file_groups(&one_file(SIZE), target_partitions, 1, false)
                .expect("a file this size splits");

            let mut covered: Vec<(i64, i64)> =
                groups.iter().flat_map(|group| ranges(group)).collect();
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

        let groups = interleaved_file_groups(&one_file(SIZE), PARTITIONS, 1, false)
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
        let groups = interleaved_file_groups(&one_file(1_000_000), 3, 1, false)
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
        let groups = interleaved_file_groups(&one_file(1_000_000), 4, 1, true)
            .expect("a file this size splits");

        for group in &groups {
            assert_eq!(
                group.len(),
                1,
                "an ordered partition holds one contiguous run"
            );
        }
    }

    /// A scan too small to divide is left alone, exactly as DataFusion leaves it.
    #[test]
    fn a_scan_below_the_minimum_is_not_divided() {
        assert!(interleaved_file_groups(&one_file(1_000), 4, 10 * 1024 * 1024, false).is_none());
    }
}
