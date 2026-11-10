//! The stable identity of one object version.

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// The stable identity of one object version in one store.
///
/// `store` is a hash of the store url. `file` is a hash of the store url, the
/// location, the modification time in milliseconds and the size. Neither covers
/// the e-tag: on NFS the local store builds the e-tag from the inode, which
/// changes when the file does not. xxh3 gives the same value in each process,
/// so a stored stamp stays valid.
///
/// Stamps sort by store first, so the stamps of one store are one key range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stamp {
    store: u64,
    file: u64,
}

impl Stamp {
    /// The stamp of `object` in the store at `store_url`.
    pub fn new(store_url: &ObjectStoreUrl, object: &ObjectMeta) -> Self {
        let mut hasher = Xxh3::new();
        // A length prefix on each string keeps ("ab", "c") apart from ("a", "bc").
        for part in [store_url.as_str(), object.location.as_ref()] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        hasher.update(&object.last_modified.timestamp_millis().to_le_bytes());
        hasher.update(&object.size.to_le_bytes());
        Self {
            store: store_hash(store_url),
            file: hasher.digest(),
        }
    }

    /// The lowest and the highest stamp of the store at `store_url`.
    pub(crate) fn store_range(store_url: &ObjectStoreUrl) -> (Self, Self) {
        let store = store_hash(store_url);
        (
            Self { store, file: 0 },
            Self {
                store,
                file: u64::MAX,
            },
        )
    }

    /// The stamp as 16 big-endian bytes, so the byte order is the stamp order.
    pub(crate) fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.store.to_be_bytes());
        bytes[8..].copy_from_slice(&self.file.to_be_bytes());
        bytes
    }

    /// The stamp of 16 bytes from [`Stamp::to_bytes`].
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Self {
        let (store, file) = bytes.split_at(8);
        // `split_at(8)` of 16 bytes gives two 8-byte halves, so each conversion succeeds.
        Self {
            store: u64::from_be_bytes(store.try_into().expect("8 bytes")),
            file: u64::from_be_bytes(file.try_into().expect("8 bytes")),
        }
    }

    /// A stamp from its two hashes, for tests.
    #[cfg(test)]
    pub(crate) fn from_parts(store: u64, file: u64) -> Self {
        Self { store, file }
    }
}

/// The hash of a store url.
fn store_hash(store_url: &ObjectStoreUrl) -> u64 {
    xxh3_64(store_url.as_str().as_bytes())
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
        let stamp = Stamp::new(&url("memory"), &object("a/b.nc", 1_700_000_000_000, 42));
        assert_eq!(
            stamp,
            Stamp::from_parts(7_049_216_141_237_563_594, 4_956_598_281_496_764_451)
        );
    }

    #[test]
    fn bytes_keep_the_order_and_the_value() {
        let low = Stamp::from_parts(1, u64::MAX);
        let high = Stamp::from_parts(2, 0);

        assert!(low.to_bytes() < high.to_bytes(), "the store comes first");
        assert_eq!(Stamp::from_bytes(low.to_bytes()), low);
    }

    #[test]
    fn a_store_range_holds_each_stamp_of_its_store_only() {
        let (start, end) = Stamp::store_range(&url("memory"));
        let inside = Stamp::new(&url("memory"), &object("a.nc", 0, 0));
        let outside = Stamp::new(&url("other"), &object("a.nc", 0, 0));

        assert!((start..=end).contains(&inside));
        assert!(!(start..=end).contains(&outside));
    }

    #[test]
    fn a_stamp_covers_the_store_the_time_and_the_size_but_not_the_e_tag() {
        let base = object("a/b.nc", 1_000, 42);
        let stamp = Stamp::new(&url("memory"), &base);

        assert_ne!(stamp, Stamp::new(&url("other"), &base));
        assert_ne!(
            stamp,
            Stamp::new(&url("memory"), &object("a/b.nc", 1_001, 42))
        );
        assert_ne!(
            stamp,
            Stamp::new(&url("memory"), &object("a/b.nc", 1_000, 43))
        );
        let tagged = ObjectMeta {
            e_tag: Some("inode-2".to_string()),
            ..base
        };
        assert_eq!(stamp, Stamp::new(&url("memory"), &tagged));
    }

    #[test]
    fn a_length_prefix_keeps_split_points_apart() {
        let left = Stamp::new(&url("ab"), &object("c", 0, 0));
        let right = Stamp::new(&url("a"), &object("bc", 0, 0));
        assert_ne!(left, right);
    }
}
