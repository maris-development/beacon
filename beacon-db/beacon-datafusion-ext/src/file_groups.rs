use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner};

/// Give every partition the files worth sharing, and one each of the rest.
///
/// A file over `min_share_size` goes into *every* partition's group.
/// A file at or under the size is left whole and dealt to one partition. Every
/// partition opening it to take a subset or two would cost more than it returns,
/// and the listing has already spread these across the scan.
pub fn shared_file_groups(
    file_groups: &[FileGroup],
    target_partitions: usize,
    min_share_size: u64,
) -> Option<Vec<FileGroup>> {
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

    todo!("Shared files need to be copied into every partition so it runs parallel.")
}
