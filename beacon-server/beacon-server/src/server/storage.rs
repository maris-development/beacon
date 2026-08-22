//! Disk space of the datasets store, for `GET /api/admin/datasets/storage`.
//!
//! An operator must see when the store fills up without opening a shell. The two
//! store backings answer that question differently:
//!
//! - A local directory sits on a disk with a fixed capacity. `sysinfo` reports
//!   the capacity of the mount that holds the directory.
//! - An S3 bucket has no capacity. Only the size of the objects is known, so the
//!   free space and the used percent stay empty.

use std::path::{Path, PathBuf};

use futures::StreamExt;
use object_store::ObjectStore;

/// Which store backs the datasets, and therefore which values are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum StorageKind {
    /// A directory on a local disk. Every value is present.
    Local,
    /// An S3 bucket. A bucket has no capacity, so only the used space is present.
    S3,
}

/// Disk space of the datasets store.
///
/// `total_space`, `free_space` and `used_percent` are `null` for an S3 bucket.
/// The web UI shows `n/a` for each of them.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct DatasetStorageInfo {
    /// Which store holds the datasets.
    pub kind: StorageKind,
    /// The datasets directory, or the bucket name for S3.
    #[schema(example = "/beacon/data/datasets")]
    pub location: String,
    /// The mount point of the disk that holds the directory. `null` for S3.
    #[schema(example = "/")]
    pub mount_point: Option<String>,
    /// The capacity of the disk, in bytes. `null` for S3.
    pub total_space: Option<u64>,
    /// The used space of the disk, in bytes. For S3, the total size of the objects.
    pub used_space: Option<u64>,
    /// The free space of the disk, in bytes. `null` for S3.
    pub free_space: Option<u64>,
    /// The used space as a percent of the capacity. `null` for S3.
    pub used_percent: Option<f64>,
    /// The number of objects in the bucket. `null` for a local directory.
    pub object_count: Option<u64>,
}

/// Read the space of the disk that holds `dir`.
///
/// The directory does not have to exist yet: the nearest parent that does exist
/// sits on the same disk. A path that matches no mount point at all yields
/// `None` rather than an error, because the space is a display value.
pub fn local_storage(dir: &Path) -> DatasetStorageInfo {
    let mut info = DatasetStorageInfo {
        kind: StorageKind::Local,
        location: dir.display().to_string(),
        mount_point: None,
        total_space: None,
        used_space: None,
        free_space: None,
        used_percent: None,
        object_count: None,
    };

    let Some(target) = existing_ancestor(dir) else {
        return info;
    };

    let disks = sysinfo::Disks::new_with_refreshed_list_specifics(
        sysinfo::DiskRefreshKind::nothing().with_storage(),
    );

    // Several mounts can be a prefix of the path (`/` and `/data` both hold
    // `/data/datasets`). The longest one is the disk the directory is on.
    let disk = disks
        .list()
        .iter()
        .filter(|disk| target.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len());

    if let Some(disk) = disk {
        let total = disk.total_space();
        let free = disk.available_space();
        info.mount_point = Some(disk.mount_point().display().to_string());
        info.total_space = Some(total);
        info.free_space = Some(free);
        info.used_space = Some(total.saturating_sub(free));
        info.used_percent = percent(total.saturating_sub(free), total);
    }

    info
}

/// Sum the size and the count of every object in the bucket.
///
/// A bucket has no capacity, so a listing is the only measure of the used space.
/// The listing is a paged request per 1000 objects; a large bucket makes this
/// call slow. The caller must not put it on a fast refresh interval.
pub async fn s3_storage(store: &dyn ObjectStore, bucket: &str) -> DatasetStorageInfo {
    let mut used: u64 = 0;
    let mut count: u64 = 0;
    let mut stream = store.list(None);
    while let Some(entry) = stream.next().await {
        match entry {
            Ok(meta) => {
                used += meta.size;
                count += 1;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list the datasets bucket");
                break;
            }
        }
    }

    DatasetStorageInfo {
        kind: StorageKind::S3,
        location: bucket.to_string(),
        mount_point: None,
        total_space: None,
        used_space: Some(used),
        free_space: None,
        used_percent: None,
        object_count: Some(count),
    }
}

/// The nearest ancestor of `dir` that exists, canonicalized.
///
/// A datasets directory is created on first write, so it can still be absent.
/// Its parent is on the same disk, which makes the space the same value.
fn existing_ancestor(dir: &Path) -> Option<PathBuf> {
    let mut current = dir;
    loop {
        if let Ok(path) = current.canonicalize() {
            return Some(path);
        }
        current = current.parent()?;
    }
}

/// `used` as a percent of `total`, or `None` when the capacity is unknown.
fn percent(used: u64, total: u64) -> Option<f64> {
    (total > 0).then(|| (used as f64 / total as f64) * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_storage_reads_the_disk_of_an_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let info = local_storage(dir.path());

        assert_eq!(info.kind, StorageKind::Local);
        assert_eq!(info.location, dir.path().display().to_string());
        assert!(
            info.total_space.unwrap() > 0,
            "the disk must have a capacity"
        );
        assert_eq!(
            info.used_space.unwrap() + info.free_space.unwrap(),
            info.total_space.unwrap()
        );
        let pct = info.used_percent.unwrap();
        assert!(
            (0.0..=100.0).contains(&pct),
            "used percent out of range: {pct}"
        );
        assert!(info.mount_point.is_some());
        assert_eq!(info.object_count, None);
    }

    #[test]
    fn local_storage_falls_back_to_an_existing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("datasets");
        let info = local_storage(&missing);

        assert_eq!(info.location, missing.display().to_string());
        assert!(
            info.total_space.is_some(),
            "the parent disk answers instead"
        );
    }

    #[test]
    fn percent_of_an_unknown_capacity_is_none() {
        assert_eq!(percent(0, 0), None);
        assert_eq!(percent(1, 4), Some(25.0));
    }
}
