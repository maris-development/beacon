//! Finds Atlas collections in a listing, and deals them to partitions.
//!
//! A collection is a directory with one marker object, `data.atlas`, and an
//! optional `deleted.mask` sidecar. Every marker is its own collection.

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use object_store::{ObjectMeta, path::Path as OsPath};

/// The container object at the root of a collection.
pub const ATLAS_MARKER: &str = "data.atlas";

/// The deletion-mask sidecar beside it. Absent means nothing is deleted.
pub const ATLAS_MASK: &str = "deleted.mask";

/// `data.atlas` as it appears at the end of a nested path.
const MARKER_SUFFIX: &str = "/data.atlas";

/// Whether `path` names a collection's container object.
///
/// The name is fixed: `data.atlas`, resolved by [`atlas::Atlas::open`].
pub fn is_marker_path(path: &OsPath) -> bool {
    let path = path.as_ref();
    path == ATLAS_MARKER || path.ends_with(MARKER_SUFFIX)
}

/// The collection markers among `objects`, in listing order.
///
/// One marker is one collection, even nested under another marker's directory.
pub fn atlas_markers(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    objects
        .iter()
        .filter(|object| is_marker_path(&object.location))
        .cloned()
        .collect()
}

/// The prefix a collection is opened under: the marker's parent directory.
///
/// An empty path means the marker sits at the store root.
pub fn collection_prefix(marker: &OsPath) -> Option<OsPath> {
    let path = marker.as_ref();
    if path == ATLAS_MARKER {
        return Some(OsPath::default());
    }
    path.strip_suffix(MARKER_SUFFIX).map(OsPath::from)
}

/// Deal `markers` over `partitions` groups, every collection to every group.
///
/// Each group is the whole list, rotated so start points spread evenly.
pub(crate) fn deal_rotated(markers: &[ObjectMeta], partitions: usize) -> Vec<FileGroup> {
    let n = markers.len();
    if n == 0 {
        return Vec::new();
    }
    let partitions = partitions.max(1);
    (0..partitions)
        .map(|p| {
            let offset = p * n / partitions;
            (0..n)
                .map(|i| PartitionedFile::from(markers[(offset + i) % n].clone()))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn object(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: OsPath::from(path),
            last_modified: DateTime::UNIX_EPOCH,
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    // ── markers ─────────────────────────────────────────────────────────

    #[test]
    fn the_container_object_is_the_marker() {
        assert!(is_marker_path(&OsPath::from("data.atlas")));
        assert!(is_marker_path(&OsPath::from("store/data.atlas")));
        assert!(is_marker_path(&OsPath::from("a/b/c/data.atlas")));
    }

    #[test]
    fn nothing_else_is_a_marker() {
        // Neither the mask nor an old pre-0.16 registry format is a marker.
        for path in [
            "deleted.mask",
            "store/deleted.mask",
            "store/data.atlas.tmp",
            "store/mydata.atlas",
            "data.atlas/inner",
            "store/atlas.json",
        ] {
            assert!(!is_marker_path(&OsPath::from(path)), "{path}");
        }
    }

    #[test]
    fn the_prefix_is_the_marker_directory() {
        assert_eq!(
            collection_prefix(&OsPath::from("a/b/data.atlas")),
            Some(OsPath::from("a/b"))
        );
        assert_eq!(
            collection_prefix(&OsPath::from("data.atlas")),
            Some(OsPath::default())
        );
        assert_eq!(collection_prefix(&OsPath::from("a/b/other.txt")), None);
    }

    /// Every `data.atlas` is a collection, one under another's directory too.
    /// The mask beside a marker is not one.
    #[test]
    fn every_marker_is_its_own_collection() {
        let objects = vec![
            object("a/data.atlas"),
            object("a/b/data.atlas"),
            object("c/data.atlas"),
            object("c/deleted.mask"),
        ];
        let kept: Vec<String> = atlas_markers(&objects)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(kept, vec!["a/data.atlas", "a/b/data.atlas", "c/data.atlas"]);
    }
}

#[cfg(test)]
mod deal_tests {
    use super::*;

    fn markers(n: usize) -> Vec<ObjectMeta> {
        (0..n)
            .map(|i| ObjectMeta {
                location: object_store::path::Path::from(format!("c{i}/{ATLAS_MARKER}")),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            })
            .collect()
    }

    /// The collections of a group, by their directory.
    fn dealt(group: &FileGroup) -> Vec<String> {
        group
            .files()
            .iter()
            .map(|file| {
                file.object_meta
                    .location
                    .parts()
                    .next()
                    .unwrap()
                    .as_ref()
                    .to_string()
            })
            .collect()
    }

    /// The collection each group starts on.
    fn starts(groups: &[FileGroup]) -> Vec<String> {
        groups.iter().map(|group| dealt(group)[0].clone()).collect()
    }

    #[test]
    fn every_partition_holds_every_collection_in_its_own_rotation() {
        let groups = deal_rotated(&markers(4), 2);

        assert_eq!(groups.len(), 2);
        assert_eq!(dealt(&groups[0]), ["c0", "c1", "c2", "c3"]);
        assert_eq!(dealt(&groups[1]), ["c2", "c3", "c0", "c1"]);
    }

    #[test]
    fn the_starts_spread_evenly_over_the_collections() {
        assert_eq!(
            starts(&deal_rotated(&markers(3), 3)),
            ["c0", "c1", "c2"],
            "one start per collection"
        );
        assert_eq!(
            starts(&deal_rotated(&markers(4), 3)),
            ["c0", "c1", "c2"],
            "fewer partitions start as far apart as they can"
        );
        let groups = deal_rotated(&markers(2), 4);
        assert_eq!(
            starts(&groups),
            ["c0", "c0", "c1", "c1"],
            "more partitions double up evenly"
        );
        assert!(groups.iter().all(|group| group.len() == 2));
    }

    #[test]
    fn no_collection_makes_no_group() {
        assert!(deal_rotated(&markers(0), 4).is_empty());
        assert_eq!(
            deal_rotated(&markers(2), 0).len(),
            1,
            "no partition reads as one"
        );
    }
}
