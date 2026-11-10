//! The stable hash of one object version.

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use xxhash_rust::xxh3::Xxh3;

/// A stable hash of one version of `object` in the store at `store_url`.
///
/// The hash covers the store url, the location, the modification time in
/// milliseconds and the size. It does not cover the e-tag: on NFS the local
/// store builds the e-tag from the inode, which changes when the file does not.
/// xxh3 gives the same value in each process, so a stored stamp stays valid.
pub(crate) fn stamp(store_url: &ObjectStoreUrl, object: &ObjectMeta) -> u64 {
    let mut hasher = Xxh3::new();
    // A length prefix on each string keeps ("ab", "c") apart from ("a", "bc").
    for part in [store_url.as_str(), object.location.as_ref()] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.update(&object.last_modified.timestamp_millis().to_le_bytes());
    hasher.update(&object.size.to_le_bytes());
    hasher.digest()
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;

    use super::*;

    fn url(name: &str) -> ObjectStoreUrl {
        ObjectStoreUrl::parse(format!("{name}://")).expect("a valid store url")
    }

    fn object(path: &str, millis: i64, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::DateTime::from_timestamp_millis(millis).expect("a valid time"),
            size,
            e_tag: None,
            version: None,
        }
    }

    /// A change of the hash input makes stored stamps invalid, so this test fails on it.
    #[test]
    fn a_stamp_is_stable_across_processes() {
        let value = stamp(&url("memory"), &object("a/b.nc", 1_700_000_000_000, 42));
        assert_eq!(value, 4_956_598_281_496_764_451);
    }

    #[test]
    fn a_stamp_covers_the_store_the_time_and_the_size_but_not_the_e_tag() {
        let base = object("a/b.nc", 1_000, 42);
        let value = stamp(&url("memory"), &base);

        assert_ne!(value, stamp(&url("other"), &base));
        assert_ne!(value, stamp(&url("memory"), &object("a/b.nc", 1_001, 42)));
        assert_ne!(value, stamp(&url("memory"), &object("a/b.nc", 1_000, 43)));
        let tagged = ObjectMeta {
            e_tag: Some("inode-2".to_string()),
            ..base
        };
        assert_eq!(value, stamp(&url("memory"), &tagged));
    }

    #[test]
    fn a_length_prefix_keeps_split_points_apart() {
        assert_ne!(
            stamp(&url("ab"), &object("c", 0, 0)),
            stamp(&url("a"), &object("bc", 0, 0))
        );
    }
}
