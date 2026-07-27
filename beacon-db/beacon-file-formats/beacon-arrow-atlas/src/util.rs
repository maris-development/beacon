//! Atlas metadata-marker discovery helpers shared by the DataFusion
//! integration.
//!
//! An atlas store is a directory holding a single metadata marker at its root
//! — one of [`ATLAS_MARKER_NAMES`] — plus the per-array `.af` files. These
//! helpers recognize markers in an object listing and reduce a listing to the
//! outermost stores, mirroring `beacon-arrow-zarr`'s `zarr.json` discovery.

use object_store::{ObjectMeta, path::Path as OsPath};

/// All marker filenames atlas may emit at the root of a store, in the same
/// priority order atlas's own `META_VARIANTS` uses (uncompressed before
/// compressed within a format; JSON before MsgPack overall). Atlas detects the
/// actual variant on `open`, so beacon only needs to recognize the names.
pub const ATLAS_MARKER_NAMES: [&str; 6] = [
    "atlas.json",
    "atlas.json.zst",
    "atlas.json.lz4",
    "atlas.msgpack",
    "atlas.msgpack.zst",
    "atlas.msgpack.lz4",
];

/// Canonical marker filename, used as the format's listing extension and the
/// `get_ext` identity. Atlas defaults to uncompressed JSON, so this is the
/// common on-disk name.
pub const ATLAS_MARKER: &str = "atlas.json";

/// If `p` ends in one of the known atlas marker filenames, return that
/// filename. Used both to recognize markers and to recover the on-disk name for
/// path manipulation.
pub fn atlas_marker_filename(p: &OsPath) -> Option<&'static str> {
    let s = p.as_ref();
    ATLAS_MARKER_NAMES
        .iter()
        .copied()
        .find(|name| s == *name || s.ends_with(&format!("/{name}")))
}

/// `true` if `obj` is an atlas metadata marker.
pub fn is_atlas_marker(obj: &ObjectMeta) -> bool {
    atlas_marker_filename(&obj.location).is_some()
}

/// The store directory (marker's parent) as a string, or `None` if `p` is not a
/// marker. An empty string means the marker sits at the object-store root.
pub fn marker_parent(p: &OsPath) -> Option<String> {
    let s = p.as_ref();
    let name = atlas_marker_filename(p)?;
    Some(s.strip_suffix(name)?.trim_end_matches('/').to_string())
}

/// The object-store prefix an atlas store is opened under: the directory
/// containing `marker` (its parent). Passed straight to
/// [`atlas::Atlas::open`](atlas::Atlas::open).
pub fn atlas_store_prefix(marker: &OsPath) -> Option<OsPath> {
    marker_parent(marker).map(|dir| OsPath::from(dir.as_str()))
}

/// Filter `objects` down to the unique top-level atlas markers.
///
/// If two markers appear at different depths of a nested tree we keep only the
/// outermost (the ancestor) — atlas stores never contain other atlas stores, so
/// any deeper marker is spurious. Mirrors zarr's `top_level_zarr_meta_v3`.
pub fn top_level_atlas_markers(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    let mut markers: Vec<&ObjectMeta> = objects.iter().filter(|o| is_atlas_marker(o)).collect();
    markers.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

    let mut kept: Vec<ObjectMeta> = Vec::new();
    'outer: for meta in &markers {
        let dir = marker_parent(&meta.location).unwrap_or_default();
        for already in &kept {
            let already_dir = marker_parent(&already.location).unwrap_or_default();
            if !already_dir.is_empty() && dir.starts_with(&format!("{already_dir}/")) {
                continue 'outer;
            }
        }
        kept.push((*meta).clone());
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker_obj(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: OsPath::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn is_atlas_marker_matches_all_variants() {
        for name in &ATLAS_MARKER_NAMES {
            assert!(is_atlas_marker(&marker_obj(name)), "bare {name}");
            assert!(
                is_atlas_marker(&marker_obj(&format!("store/{name}"))),
                "store/{name}"
            );
            assert!(
                is_atlas_marker(&marker_obj(&format!("a/b/c/{name}"))),
                "a/b/c/{name}"
            );
        }
        for negative in [
            "foo/data.af",
            "store/atlas.json.tmp",
            "store/atlas.jsona",
            "atlas.json/inner",
        ] {
            assert!(!is_atlas_marker(&marker_obj(negative)), "{negative}");
        }
    }

    #[test]
    fn marker_parent_strips_each_variant() {
        for name in &ATLAS_MARKER_NAMES {
            assert_eq!(
                marker_parent(&OsPath::from(format!("store/{name}"))),
                Some("store".to_string()),
                "{name}"
            );
            assert_eq!(
                marker_parent(&OsPath::from(*name)),
                Some(String::new()),
                "bare {name}"
            );
        }
        assert_eq!(marker_parent(&OsPath::from("foo.txt")), None);
    }

    #[test]
    fn store_prefix_is_marker_parent() {
        assert_eq!(
            atlas_store_prefix(&OsPath::from("a/b/atlas.json")),
            Some(OsPath::from("a/b"))
        );
        assert_eq!(
            atlas_store_prefix(&OsPath::from("atlas.json")),
            Some(OsPath::from(""))
        );
    }

    #[test]
    fn top_level_markers_drops_nested_stores() {
        let objs = vec![
            marker_obj("a/atlas.json"),
            marker_obj("a/b/atlas.json"),
            marker_obj("c/atlas.msgpack"),
        ];
        let kept: Vec<String> = top_level_atlas_markers(&objs)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert!(kept.contains(&"a/atlas.json".to_string()));
        assert!(kept.contains(&"c/atlas.msgpack".to_string()));
        assert!(!kept.iter().any(|p| p == "a/b/atlas.json"));
    }
}
