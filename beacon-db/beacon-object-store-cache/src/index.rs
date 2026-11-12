//! Sorted lock-free index of object metadata.
//!
//! One skip list holds all objects of the store in path order. A read, a
//! write and the background sync use it at the same time, and no call waits
//! for a lock.
//!
//! Keys are object paths as strings. `-` and `.` sort before `/`, so the
//! children of prefix `p` are the keys that start with `p/`, not a `Path` range.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry as Node;
use object_store::ObjectMeta;

/// One object. `meta: None` is a tombstone for a deleted object.
#[derive(Debug)]
struct Entry {
    meta: Option<ObjectMeta>,
    /// Sequence number of the last source read that gave this metadata.
    seq: AtomicU64,
}

impl Entry {
    fn new(meta: Option<ObjectMeta>, seq: u64) -> Self {
        Self {
            meta,
            seq: AtomicU64::new(seq),
        }
    }

    fn seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }
}

/// Position of a range scan.
#[derive(Debug, Clone)]
pub(crate) enum Cursor {
    Start,
    From(String),
    After(String),
    Done,
}

impl Cursor {
    /// First position of a scan below `start`, after `offset` when given.
    pub(crate) fn begin(start: &str, offset: Option<&str>) -> Self {
        match offset {
            Some(offset) if offset >= start => Self::After(offset.to_string()),
            _ if start.is_empty() => Self::Start,
            _ => Self::From(start.to_string()),
        }
    }

    fn bound(&self) -> Bound<&str> {
        match self {
            Self::Start | Self::Done => Bound::Unbounded,
            Self::From(key) => Bound::Included(key.as_str()),
            Self::After(key) => Bound::Excluded(key.as_str()),
        }
    }
}

/// Key prefix shared by all objects below `prefix`.
pub(crate) fn range_start(prefix: &str) -> String {
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

#[derive(Debug, Default)]
pub(crate) struct Index {
    entries: SkipMap<String, Entry>,
    /// Sequence number of the last source listing. Zero means no listing.
    synced_seq: AtomicU64,
}

impl Index {
    /// Number of objects, without tombstones.
    pub(crate) fn len(&self) -> usize {
        self.entries.iter().filter(|node| node.value().meta.is_some()).count()
    }

    /// True when the index holds a full listing of the source.
    pub(crate) fn is_synced(&self) -> bool {
        self.synced_seq.load(Ordering::SeqCst) != 0
    }

    /// The metadata of `key`. `None` for an unknown or deleted object.
    pub(crate) fn get(&self, key: &str) -> Option<ObjectMeta> {
        self.entries.get(key).and_then(|node| node.value().meta.clone())
    }

    /// Store one change. The change with the highest sequence number wins.
    pub(crate) fn upsert(&self, key: &str, meta: Option<ObjectMeta>, seq: u64) {
        let newest = match self.entries.get(key) {
            Some(node) => node.value().seq(),
            None => self.synced_seq.load(Ordering::SeqCst),
        };
        if seq >= newest {
            self.entries
                .compare_insert(key.to_string(), Entry::new(meta, seq), |known| known.seq() <= seq);
        }
    }

    /// Make the index equal to a full source listing taken at `seq`.
    ///
    /// Changes with a higher sequence number stay.
    pub(crate) fn commit(&self, seq: u64, snapshot: Vec<ObjectMeta>) {
        for meta in snapshot {
            let key = meta.location.as_ref();
            match self.entries.get(key) {
                Some(node) if node.value().seq() > seq => {}
                // An object without a change keeps its node.
                Some(node) if node.value().meta.as_ref() == Some(&meta) => {
                    node.value().seq.fetch_max(seq, Ordering::SeqCst);
                }
                _ => {
                    let key = key.to_string();
                    self.entries
                        .compare_insert(key, Entry::new(Some(meta), seq), |known| known.seq() <= seq);
                }
            }
        }
        // An older entry is not in the listing: the source no longer has the object.
        for node in &self.entries {
            if node.value().seq() < seq {
                node.remove();
            }
        }
        self.synced_seq.fetch_max(seq, Ordering::SeqCst);
    }

    pub(crate) fn clear(&self) {
        self.synced_seq.store(0, Ordering::SeqCst);
        self.entries.clear();
    }

    fn scan<'a, 'c>(
        &'a self,
        start: &'c str,
        cursor: &'c Cursor,
    ) -> impl Iterator<Item = Node<'a, String, Entry>> + use<'a, 'c> {
        self.entries
            .range::<str, _>((cursor.bound(), Bound::Unbounded))
            .take_while(move |node| node.key().starts_with(start))
    }

    /// Read at most `limit` keys below `start`. Returns the live objects and the next position.
    pub(crate) fn chunk(&self, start: &str, cursor: &Cursor, limit: usize) -> (Vec<ObjectMeta>, Cursor) {
        let mut objects = Vec::new();
        let mut last = None;
        let mut scanned = 0;
        for node in self.scan(start, cursor).take(limit) {
            scanned += 1;
            objects.extend(node.value().meta.clone());
            last = Some(node);
        }
        let next = match last {
            Some(node) if scanned == limit => Cursor::After(node.key().clone()),
            _ => Cursor::Done,
        };
        (objects, next)
    }

    /// Objects directly in `prefix` and the folders directly below it.
    pub(crate) fn delimited(&self, prefix: &str) -> (Vec<String>, Vec<ObjectMeta>) {
        let start = range_start(prefix);
        let mut folders = Vec::new();
        let mut objects = Vec::new();
        let mut cursor = Cursor::begin(&start, None);
        loop {
            let Some(node) = self.scan(&start, &cursor).next() else {
                break;
            };
            let key = node.key();
            let folder_end = key[start.len()..].find('/').map(|at| start.len() + at);
            cursor = match (folder_end, &node.value().meta) {
                (Some(end), Some(_)) => {
                    folders.push(key[..end].to_string());
                    // `0` is the character after `/`: this skips the folder content.
                    Cursor::From(format!("{}0", &key[..end]))
                }
                (None, Some(meta)) => {
                    objects.push(meta.clone());
                    Cursor::After(key.clone())
                }
                (_, None) => Cursor::After(key.clone()),
            };
        }
        (folders, objects)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use object_store::path::Path;

    use super::*;

    fn meta(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(key),
            last_modified: DateTime::UNIX_EPOCH,
            size,
            e_tag: None,
            version: None,
        }
    }

    fn listed(index: &Index, prefix: &str) -> Vec<String> {
        let start = range_start(prefix);
        let (objects, _) = index.chunk(&start, &Cursor::begin(&start, None), usize::MAX);
        objects.iter().map(|meta| meta.location.to_string()).collect()
    }

    #[test]
    fn a_prefix_needs_a_segment_boundary() {
        let index = Index::default();
        index.commit(1, vec![meta("p/a", 1), meta("p-x/b", 1), meta("p/c/d", 1), meta("q", 1)]);
        assert_eq!(listed(&index, "p"), ["p/a", "p/c/d"]);
        assert_eq!(listed(&index, ""), ["p-x/b", "p/a", "p/c/d", "q"]);
        let (folders, objects) = index.delimited("");
        assert_eq!(folders, ["p-x", "p"]);
        assert_eq!(objects.len(), 1);
    }

    #[test]
    fn a_commit_replaces_older_entries_and_keeps_newer_changes() {
        let index = Index::default();
        assert!(!index.is_synced());
        index.commit(1, vec![meta("p/a", 1), meta("p/b", 1)]);
        assert!(index.is_synced());

        // A write and a delete arrive after the listing with number 3 started.
        index.upsert("p/new", Some(meta("p/new", 1)), 4);
        index.upsert("p/a", None, 5);
        index.commit(3, vec![meta("p/a", 1), meta("p/c", 1)]);
        assert_eq!(listed(&index, "p"), ["p/c", "p/new"]);

        index.commit(6, vec![meta("p/c", 9)]);
        assert_eq!(listed(&index, "p"), ["p/c"]);
        assert_eq!(index.get("p/c").unwrap().size, 9);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn a_change_older_than_the_listing_has_no_effect() {
        let index = Index::default();
        index.commit(5, vec![meta("p/a", 1)]);
        index.upsert("p/old", Some(meta("p/old", 1)), 3);
        index.upsert("p/a", None, 4);
        assert_eq!(listed(&index, "p"), ["p/a"]);
    }
}
