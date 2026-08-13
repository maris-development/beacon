use datafusion::datasource::physical_plan::FileGroup;
use object_store::path::Path;

/// The deal [`shared_file_groups`] made.
#[derive(Debug, Clone)]
pub struct SharedDeal {
    /// One group per partition, in partition order.
    pub file_groups: Vec<FileGroup>,
    /// The files that landed in every group, and so have to be read through a
    /// share. Nothing else in the deal is repeated.
    pub shared: Vec<Path>,
}

/// Give every partition the files worth sharing, and one each of the rest.
///
/// A file over `min_share_size` goes into *every* partition's group. Nothing
/// about it is divided here: the partitions divide it as they read it, by taking
/// subsets from one queue they share.
///
/// A file at or under the size is left whole and dealt to one partition. Every
/// partition opening it to take a subset or two would cost more than it returns,
/// and the listing has already spread these across the scan.
///
/// Returns `None` when no file is worth sharing, which leaves the scan's
/// grouping alone.
///
/// # The caller has to honour [`SharedDeal::shared`]
///
/// A shared file is in every group. A scan that reads it whole therefore returns
/// every row once per partition. [`SharedDeal::shared`] names the files this is
/// true of, and the caller must read each of them through one share.
pub fn shared_file_groups(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_share_size: u64,
) -> Option<SharedDeal> {
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

    // Every partition holds every shared file, and holds it first: a partition
    // that opens the shared file before its own small files joins the share
    // sooner, and the queue is drawn from by all of them from the start.
    for file in &shared {
        for group in dealt.iter_mut() {
            group.push(file.clone());
        }
    }

    // The rest are spread one per partition, as the listing would have.
    for (index, file) in whole.into_iter().enumerate() {
        dealt[index % target_partitions].push(file);
    }

    Some(SharedDeal {
        file_groups: dealt.into_iter().map(FileGroup::new).collect(),
        shared: shared
            .into_iter()
            .map(|file| file.object_meta.location)
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::listing::PartitionedFile;

    use super::*;

    const MIN: u64 = 8 * 1024 * 1024;

    /// A file worth sharing lands in every partition, and is named as shared.
    #[test]
    fn a_shared_file_lands_in_every_partition() {
        const PARTITIONS: usize = 4;

        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        let deal = shared_file_groups(&groups, PARTITIONS, MIN).expect("a large file is shared");

        assert_eq!(deal.file_groups.len(), PARTITIONS);
        for group in &deal.file_groups {
            assert_eq!(group.len(), 1, "each partition holds the file once");
            let file = group.iter().next().unwrap();
            assert!(file.range.is_none(), "a shared file is not divided");
        }
        assert_eq!(
            deal.shared,
            vec![Path::from("large.nc")],
            "the caller is told which file it has to share"
        );
    }

    /// Small files are dealt one per partition, undivided and unshared.
    #[test]
    fn small_files_are_dealt_whole_and_unshared() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..4).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));
        let groups = vec![FileGroup::new(files)];

        let deal = shared_file_groups(&groups, 4, MIN).expect("the large file is shared");

        let small: Vec<_> = deal
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .filter(|file| file.object_meta.location.as_ref().starts_with("small-"))
            .collect();

        assert_eq!(small.len(), 4, "each small file appears once in the scan");
        for file in small {
            assert!(file.range.is_none(), "a whole file is not divided");
        }
        assert_eq!(deal.shared.len(), 1, "only the large file is shared");
    }

    /// Every partition opens the shared file before its own small files.
    #[test]
    fn the_shared_file_comes_first_in_every_group() {
        let mut files = vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)];
        files.extend((0..8).map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024)));
        let groups = vec![FileGroup::new(files)];

        let deal = shared_file_groups(&groups, 4, MIN).expect("the large file is shared");

        for group in &deal.file_groups {
            let first = group.iter().next().expect("a group holds the shared file");
            assert_eq!(
                first.object_meta.location,
                Path::from("large.nc"),
                "the shared file is opened first"
            );
        }
    }

    /// A scan with nothing worth sharing is left alone.
    ///
    /// This is where the rule departs from DataFusion's, which tests the scan
    /// total: 200 MB of 1 MB files clears any total-based minimum, and every one
    /// of them would be shared by partitions that each re-open it for a fraction
    /// of its rows. A share pays to open one file, so one file is what has to
    /// earn it. The listing has already spread these over the partitions.
    #[test]
    fn a_scan_with_no_large_file_is_left_alone() {
        let files: Vec<_> = (0..200)
            .map(|i| PartitionedFile::new(format!("small-{i}.nc"), 1024 * 1024))
            .collect();
        let groups = vec![FileGroup::new(files)];

        assert!(
            shared_file_groups(&groups, 4, MIN).is_none(),
            "200 MB of 1 MB files is still no file worth sharing"
        );
    }

    /// A single-partition scan shares nothing: there is nobody to share with.
    #[test]
    fn one_partition_is_left_alone() {
        let groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "large.nc",
            64 * 1024 * 1024,
        )])];

        assert!(shared_file_groups(&groups, 1, MIN).is_none());
    }

    /// Several large files are each shared by every partition.
    #[test]
    fn every_large_file_is_shared_by_every_partition() {
        const PARTITIONS: usize = 3;

        let files: Vec<_> = (0..2)
            .map(|i| PartitionedFile::new(format!("large-{i}.nc"), 64 * 1024 * 1024))
            .collect();
        let groups = vec![FileGroup::new(files)];

        let deal = shared_file_groups(&groups, PARTITIONS, MIN).expect("both files are shared");

        assert_eq!(deal.shared.len(), 2);
        for group in &deal.file_groups {
            assert_eq!(group.len(), 2, "every partition holds both files");
        }
    }
}
