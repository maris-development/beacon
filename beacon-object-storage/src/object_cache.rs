//! In-memory index for `ObjectMeta` keyed by object-store path.
//!
//! This is used to efficiently support point lookups and prefix listings without
//! repeatedly scanning a backing store.

use chrono::{DateTime, Utc};
use object_store::ObjectMeta;
use radix_trie::{Trie, TrieCommon, TrieKey};
use smol_str::SmolStr;

/// Compact in-memory index keyed by object location.
///
/// Internally this uses a radix trie to support fast prefix listing.
#[derive(Debug)]
pub(crate) struct ObjectCache {
    tree: Trie<ObjectKey, CompactObjectMeta>,
}

#[derive(Debug, Clone)]
struct CompactObjectMeta {
    size: u64,
    last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectKey(SmolStr);

impl TrieKey for ObjectKey {
    fn encode_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}

impl ObjectCache {
    /// Build a new index from an initial set of object metadata.
    pub fn new(input: Vec<ObjectMeta>) -> Self {
        let mut tree = Trie::new();
        for meta in input {
            let compact_meta = CompactObjectMeta {
                size: meta.size,
                last_modified: meta.last_modified,
            };
            tree.insert(
                ObjectKey(SmolStr::new(meta.location.as_ref())),
                compact_meta,
            );
        }
        ObjectCache { tree }
    }

    /// Insert (or overwrite) a single object entry.
    pub fn insert(&mut self, object_meta: ObjectMeta) {
        self.tree.insert(
            ObjectKey(SmolStr::new(object_meta.location.as_ref())),
            CompactObjectMeta {
                size: object_meta.size,
                last_modified: object_meta.last_modified,
            },
        );
    }

    /// Remove an entry by its string path.
    pub fn remove(&mut self, path: &object_store::path::Path) {
        self.tree.remove(&ObjectKey(SmolStr::new(path.as_ref())));
    }

    /// Get an entry by its string path.
    ///
    /// Note: This is currently unused within this crate, but is intentionally
    /// kept as a stable API for downstream crates / planned query paths.
    #[allow(dead_code)]
    pub fn get(&self, path: &str) -> Option<ObjectMeta> {
        self.tree
            .get(&ObjectKey(SmolStr::new(path)))
            .map(|m| self.build_meta(path, m))
    }

    /// List all entries whose key starts with `prefix`.
    pub fn list_prefix(&self, prefix: &str) -> impl Iterator<Item = ObjectMeta> + '_ {
        let prefix = SmolStr::new(prefix);
        // Note: `radix_trie::TrieCommon::subtrie` returns `None` unless the prefix
        // exists as an explicit node in the trie. In our use-case we want
        // "string starts with" semantics regardless of whether `prefix` was
        // inserted as a key.
        self.tree
            .iter()
            .filter(move |(k, _)| k.0.starts_with(prefix.as_str()))
            .map(move |(k, v)| self.build_meta(&k.0, v))
    }

    fn build_meta(&self, path: &str, meta: &CompactObjectMeta) -> ObjectMeta {
        ObjectMeta {
            location: object_store::path::Path::from(path),
            size: meta.size,
            last_modified: meta.last_modified,
            e_tag: None,
            version: None,
        }
    }

    /// Return objects modified after `since` (optionally filtered by prefix).
    ///
    /// Note: This is currently unused within this crate, but is intentionally
    /// kept as a stable API for downstream crates / planned query paths.
    #[allow(dead_code)]
    pub fn modified_since(
        &self,
        since: DateTime<Utc>,
        prefix: Option<&str>,
    ) -> impl Iterator<Item = ObjectMeta> + '_ {
        let iter: Box<dyn Iterator<Item = ObjectMeta>> = if let Some(prefix) = prefix {
            Box::new(self.list_prefix(prefix))
        } else {
            Box::new(self.tree.iter().map(|(k, v)| self.build_meta(&k.0, v)))
        };

        iter.filter(move |meta| meta.last_modified > since)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};
    use object_store::path::Path;

    fn meta(path: &str, size: u64, last_modified: DateTime<Utc>) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            size,
            last_modified,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn get_round_trips_size_and_timestamp() {
        let dt = Utc
            .with_ymd_and_hms(2020, 1, 2, 3, 4, 5)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();

        let index = ObjectCache::new(vec![meta("a/b/c.txt", 42, dt)]);
        let got = index.get("a/b/c.txt").expect("expected entry");

        assert_eq!(got.size, 42);
        assert_eq!(got.location.as_ref(), "a/b/c.txt");
        assert_eq!(got.last_modified.timestamp(), dt.timestamp());
        assert_eq!(
            got.last_modified.timestamp_subsec_nanos(),
            dt.timestamp_subsec_nanos()
        );
    }

    #[test]
    fn insert_overwrites_existing() {
        let dt1 = Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap();
        let dt2 = Utc
            .with_ymd_and_hms(2021, 1, 1, 0, 0, 1)
            .unwrap()
            .with_nanosecond(1)
            .unwrap();

        let mut index = ObjectCache::new(vec![meta("x", 1, dt1)]);
        index.insert(meta("x", 999, dt2));

        let got = index.get("x").unwrap();
        assert_eq!(got.size, 999);
        assert_eq!(got.last_modified.timestamp(), dt2.timestamp());
        assert_eq!(
            got.last_modified.timestamp_subsec_nanos(),
            dt2.timestamp_subsec_nanos()
        );
    }

    #[test]
    fn remove_deletes_entry() {
        let dt = Utc.with_ymd_and_hms(2022, 6, 7, 8, 9, 10).unwrap();

        let mut index = ObjectCache::new(vec![meta("to/remove", 1, dt)]);
        assert!(index.get("to/remove").is_some());
        index.remove(&object_store::path::Path::from("to/remove"));
        assert!(index.get("to/remove").is_none());
    }

    #[test]
    fn list_prefix_returns_only_matching_keys() {
        let dt = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let index = ObjectCache::new(vec![
            meta("p/a", 1, dt),
            meta("p/b", 2, dt),
            meta("p2/c", 3, dt),
            meta("p/a/deeper", 4, dt),
        ]);

        let mut items: Vec<_> = index
            .list_prefix("p/")
            .map(|m| m.location.to_string())
            .collect();
        items.sort();

        assert_eq!(items, vec!["p/a", "p/a/deeper", "p/b"]);
    }
}
