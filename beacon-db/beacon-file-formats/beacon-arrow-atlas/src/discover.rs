//! Finding Atlas collections in a listing, and dealing them to partitions.
//!
//! A collection is a store prefix holding one required object, `data.atlas`,
//! and one optional sidecar, `deleted.mask`. The container object is the
//! *marker*: it is what a listing matches, what a plan entry carries, and what
//! the reader cache keys on. Its parent directory is the prefix
//! [`atlas::Atlas::open`] takes. The open itself lives in [`open`](crate::open).

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
/// The name is fixed. [`atlas::Atlas::open`] resolves `data.atlas` under the
/// prefix it is given, so a collection renamed to `sensor.atlas` cannot be
/// opened at all and is not a marker.
pub fn is_marker_path(path: &OsPath) -> bool {
    let path = path.as_ref();
    path == ATLAS_MARKER || path.ends_with(MARKER_SUFFIX)
}

/// Whether `obj` is a collection's container object.
pub fn is_atlas_marker(obj: &ObjectMeta) -> bool {
    is_marker_path(&obj.location)
}

/// The prefix a collection is opened under: the marker's parent directory.
///
/// An empty path means the marker sits at the store root, which is what a
/// store rooted on the collection's own directory reports.
pub fn collection_prefix(marker: &OsPath) -> Option<OsPath> {
    let path = marker.as_ref();
    if path == ATLAS_MARKER {
        return Some(OsPath::default());
    }
    path.strip_suffix(MARKER_SUFFIX).map(OsPath::from)
}

/// The directory of a marker, as a string. `""` for one at the root.
fn marker_directory(marker: &OsPath) -> Option<String> {
    let path = marker.as_ref();
    if path == ATLAS_MARKER {
        return Some(String::new());
    }
    path.strip_suffix(MARKER_SUFFIX).map(str::to_string)
}

/// Reduce `objects` to the unique outermost collection markers.
///
/// Two markers at two depths of one tree keep only the ancestor: a collection
/// is one file and never contains another, so a deeper marker is a collection
/// that happens to sit inside another's directory and would be read twice.
pub fn top_level_atlas_markers(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    // By directory, not by path. A path sort would put "a/b/data.atlas" before
    // "a/data.atlas", because 'b' sorts under 'd', and the nested collection
    // would then be the one kept.
    let mut markers: Vec<(String, &ObjectMeta)> = objects
        .iter()
        .filter_map(|object| {
            marker_directory(&object.location).map(|directory| (directory, object))
        })
        .collect();
    markers.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut kept: Vec<(String, ObjectMeta)> = Vec::new();
    'outer: for (directory, marker) in markers {
        for (held, _) in &kept {
            // A marker at the root sits above every path, but the collection
            // beside it is its own, so an empty directory excludes nothing.
            if !held.is_empty() && directory.starts_with(&format!("{held}/")) {
                continue 'outer;
            }
        }
        kept.push((directory, marker.clone()));
    }
    kept.into_iter().map(|(_, marker)| marker).collect()
}

/// Deal `markers` over `partitions` groups, every collection to every group.
///
/// A collection's queue shares it between the partitions that open it,
/// so a partition may hold every collection and still read nothing twice.
/// Every partition then reads until every collection is drained, whatever
/// the collections' sizes, and parallelism is bounded by the dataset count
/// rather than the collection count.
///
/// Each group is the whole list, rotated. Group `p` starts `p * n / partitions`
/// collections round the ring, so the start points spread evenly: with as many
/// groups as collections each starts on its own, with fewer they start as far
/// apart as they can, and with more they double up as evenly as they can. A
/// partition therefore works alone on its collection until the partitions
/// meet, and the shared queue takes over from there.
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
        assert!(is_atlas_marker(&object("data.atlas")));
        assert!(is_atlas_marker(&object("store/data.atlas")));
        assert!(is_atlas_marker(&object("a/b/c/data.atlas")));
    }

    #[test]
    fn nothing_else_is_a_marker() {
        // The mask sits beside the container and must never be read as one.
        // Neither must the registry of a pre-0.16 collection: this build reads
        // only the single-file format, so an old collection left on disk is
        // passed over rather than misread.
        for path in [
            "deleted.mask",
            "store/deleted.mask",
            "store/data.atlas.tmp",
            "store/mydata.atlas",
            "data.atlas/inner",
            "store/atlas.json",
        ] {
            assert!(!is_atlas_marker(&object(path)), "{path}");
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

    #[test]
    fn top_level_markers_drop_a_nested_collection() {
        let objects = vec![
            object("a/data.atlas"),
            object("a/b/data.atlas"),
            object("c/data.atlas"),
            object("c/deleted.mask"),
        ];
        let kept: Vec<String> = top_level_atlas_markers(&objects)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(kept, vec!["a/data.atlas", "c/data.atlas"]);
    }

    #[test]
    fn a_sibling_directory_is_not_nested() {
        // "argo2" starts with "argo", but it is not under it.
        let objects = vec![object("argo/data.atlas"), object("argo2/data.atlas")];
        assert_eq!(top_level_atlas_markers(&objects).len(), 2);
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
