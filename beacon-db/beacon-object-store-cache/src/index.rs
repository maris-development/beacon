//! Sorted in-memory index of object metadata.
//!
//! Keys are object paths as strings. `-` and `.` sort before `/`, so the
//! children of prefix `p` are the keys that start with `p/`, not a `Path` range.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use object_store::ObjectMeta;
use tokio::time::Instant;

/// One object. `meta: None` is a tombstone for a deleted object.
#[derive(Debug)]
pub(crate) struct Entry {
    pub meta: Option<ObjectMeta>,
    pub seq: u64,
}

/// A prefix that the cache listed completely from the source.
#[derive(Debug)]
pub(crate) struct Cover {
    pub synced_seq: u64,
    pub synced_at: Instant,
    /// Milliseconds since the cache start, set on each read.
    pub last_used: AtomicU64,
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

/// True when `key` is an object below folder `prefix`.
pub(crate) fn is_under(key: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || (key.len() > prefix.len()
            && key.starts_with(prefix)
            && key.as_bytes()[prefix.len()] == b'/')
}

/// The root, then each parent folder of `key`. With `inclusive`, `key` too.
pub(crate) fn ancestors(key: &str, inclusive: bool) -> impl Iterator<Item = &str> {
    let parents = key.match_indices('/').map(move |(at, _)| &key[..at]);
    let own = (inclusive && !key.is_empty()).then_some(key);
    std::iter::once("").chain(parents).chain(own)
}

#[derive(Debug, Default)]
pub(crate) struct Index {
    entries: BTreeMap<String, Entry>,
    covers: BTreeMap<String, Cover>,
}

impl Index {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn cover_prefixes(&self) -> Vec<String> {
        self.covers.keys().cloned().collect()
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key)
    }

    fn cover_of(&self, key: &str, inclusive: bool) -> Option<&Cover> {
        ancestors(key, inclusive).find_map(|prefix| self.covers.get(prefix))
    }

    /// True when a cover holds `key`, at any age.
    pub(crate) fn has_cover(&self, key: &str, inclusive: bool) -> bool {
        self.cover_of(key, inclusive).is_some()
    }

    /// True when a cover with a recent sync holds `key`. Marks the cover as read.
    pub(crate) fn is_fresh(&self, key: &str, inclusive: bool, max_age: Duration, used: u64) -> bool {
        let fresh = ancestors(key, inclusive)
            .filter_map(|prefix| self.covers.get(prefix))
            .find(|cover| cover.synced_at.elapsed() <= max_age);
        if let Some(cover) = fresh {
            cover.last_used.store(used, Ordering::Relaxed);
        }
        fresh.is_some()
    }

    /// Store one change. The change with the highest sequence number wins.
    pub(crate) fn upsert(&mut self, key: &str, meta: Option<ObjectMeta>, seq: u64) {
        let newest = match self.entries.get(key) {
            Some(entry) => entry.seq,
            None => self.cover_of(key, false).map_or(0, |cover| cover.synced_seq),
        };
        if seq >= newest {
            self.entries.insert(key.to_string(), Entry { meta, seq });
        }
    }

    /// Replace all objects below `prefix` with a source listing taken at `seq`.
    ///
    /// Changes with a higher sequence number stay.
    pub(crate) fn commit(&mut self, prefix: &str, seq: u64, snapshot: Vec<ObjectMeta>) {
        let start = range_start(prefix);
        let old: Vec<String> = self
            .scan(&start, &Cursor::begin(&start, None))
            .filter(|(_, entry)| entry.seq <= seq)
            .map(|(key, _)| key.clone())
            .collect();
        for key in old {
            self.entries.remove(&key);
        }
        for meta in snapshot {
            self.entries
                .entry(meta.location.as_ref().to_string())
                .or_insert(Entry { meta: Some(meta), seq });
        }
    }

    /// Record that `prefix` is complete as of `seq`. Covers below it merge into it.
    pub(crate) fn set_cover(&mut self, prefix: &str, seq: u64, used: u64) {
        self.covers.retain(|other, _| !is_under(other, prefix));
        let last_used = self
            .covers
            .get(prefix)
            .map_or(used, |cover| cover.last_used.load(Ordering::Relaxed));
        self.covers.insert(
            prefix.to_string(),
            Cover {
                synced_seq: seq,
                synced_at: Instant::now(),
                last_used: AtomicU64::new(last_used),
            },
        );
    }

    /// Drop the cover that holds `key` and the objects only that cover held.
    pub(crate) fn uncover(&mut self, key: &str, inclusive: bool) {
        let prefix = ancestors(key, inclusive)
            .find(|prefix| self.covers.contains_key(*prefix))
            .map(str::to_string);
        if let Some(prefix) = prefix {
            self.covers.remove(&prefix);
            self.purge_uncovered(&prefix);
        }
    }

    /// Remove the objects below `prefix` that no cover holds.
    pub(crate) fn purge_uncovered(&mut self, prefix: &str) {
        let start = range_start(prefix);
        let stray: Vec<String> = self
            .scan(&start, &Cursor::begin(&start, None))
            .filter(|(key, _)| !self.has_cover(key, false))
            .map(|(key, _)| key.clone())
            .collect();
        for key in stray {
            self.entries.remove(&key);
        }
    }

    /// Drop each cover that no call read since `oldest_use`.
    pub(crate) fn evict_idle(&mut self, oldest_use: u64) {
        let idle: Vec<String> = self
            .covers
            .iter()
            .filter(|(_, cover)| cover.last_used.load(Ordering::Relaxed) < oldest_use)
            .map(|(prefix, _)| prefix.clone())
            .collect();
        for prefix in idle {
            self.covers.remove(&prefix);
            self.purge_uncovered(&prefix);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.covers.clear();
    }

    fn scan<'a>(
        &'a self,
        start: &'a str,
        cursor: &Cursor,
    ) -> impl Iterator<Item = (&'a String, &'a Entry)> + use<'a> {
        self.entries
            .range::<str, _>((cursor.bound(), Bound::Unbounded))
            .take_while(move |(key, _)| key.starts_with(start))
    }

    /// Read at most `limit` keys below `start`. Returns the live objects and the next position.
    pub(crate) fn chunk(&self, start: &str, cursor: &Cursor, limit: usize) -> (Vec<ObjectMeta>, Cursor) {
        let mut objects = Vec::new();
        let mut last = None;
        let mut scanned = 0;
        for (key, entry) in self.scan(start, cursor).take(limit) {
            scanned += 1;
            last = Some(key);
            objects.extend(entry.meta.clone());
        }
        let next = match last {
            Some(key) if scanned == limit => Cursor::After(key.clone()),
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
        while let Some((key, entry)) = self.scan(&start, &cursor).next() {
            let folder_end = key[start.len()..].find('/').map(|at| start.len() + at);
            cursor = match (folder_end, &entry.meta) {
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
    use super::*;

    #[test]
    fn ancestors_lists_root_and_parents() {
        let all: Vec<&str> = ancestors("a/b/c", false).collect();
        assert_eq!(all, ["", "a", "a/b"]);
        let own: Vec<&str> = ancestors("a/b", true).collect();
        assert_eq!(own, ["", "a", "a/b"]);
        assert_eq!(ancestors("", true).collect::<Vec<_>>(), [""]);
    }

    #[test]
    fn is_under_needs_a_segment_boundary() {
        assert!(is_under("p/x", "p"));
        assert!(!is_under("p-x", "p"));
        assert!(!is_under("p", "p"));
        assert!(is_under("anything", ""));
    }
}
