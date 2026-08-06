//! The segment list, and the cheap test that skips a segment without reading it.
//!
//! # Why the per-column min/max is not here
//!
//! The obvious manifest keeps each segment's per-column min/max, so a predicate
//! skips segments before any read. At 160K columns and ~100 segments that is 16M
//! metadata entries, roughly 500 MB. The metadata would then cost more than the
//! data it guards.
//!
//! So the manifest keeps only what stays small: the file id range, and the
//! sorted list of column ids the segment holds. "Could this segment hold column
//! X" becomes a binary search over a few MB in total. The per-column min/max
//! stays in the segment footer, read only for the segments that survive this
//! test.
//!
//! A sorted `Vec<ColumnId>` rather than a compressed bitmap: for a segment with
//! ~5K of 160K columns the difference is ~20 KB against ~10 KB, which is not
//! worth a dependency. Swap in a roaring bitmap if segments ever grow much
//! wider.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};

use crate::error::{FileStatsError, Result};
use crate::types::{ColumnId, FileId};

/// Where the manifest lives inside the statistics prefix.
pub const MANIFEST_NAME: &str = "manifest.bin";

/// One segment, as the manifest sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// Monotonic write order, assigned by [`Manifest::add_segment`].
    ///
    /// This is what "newest wins" means when a file appears in more than one
    /// segment. It cannot be the position in [`Manifest::segments`]: a
    /// compaction replaces many entries with one, and position stops tracking
    /// age the moment it does. A stale range winning over a fresh one is a
    /// silently wrong answer, so recency gets its own field.
    pub seq: u64,
    /// Object name within the statistics prefix.
    pub name: String,
    pub min_file_id: FileId,
    pub max_file_id: FileId,
    pub num_files: u64,
    /// Ascending, so [`contains_column`](Self::contains_column) binary-searches.
    pub column_ids: Vec<ColumnId>,
}

impl SegmentEntry {
    pub fn contains_column(&self, column_id: ColumnId) -> bool {
        self.column_ids.binary_search(&column_id).is_ok()
    }

    /// Whether this segment covers any file id in `range`.
    pub fn overlaps(&self, range: (FileId, FileId)) -> bool {
        self.min_file_id <= range.1 && range.0 <= self.max_file_id
    }
}

/// Every segment in one statistics store.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub segments: Vec<SegmentEntry>,
    /// Bumped on every write, so a reader can tell it read a stale copy.
    pub generation: u64,
    /// Never decreases, so a name is never reused. Deriving names from
    /// `segments.len()` would hand a live segment's name to the next write as
    /// soon as a compaction shrank the list.
    pub next_seq: u64,
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Claim the next write order. The caller names its object from this, so the
    /// name is claimed before the segment exists.
    pub fn claim_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    pub fn add_segment(&mut self, entry: SegmentEntry) {
        self.next_seq = self.next_seq.max(entry.seq + 1);
        self.segments.push(entry);
        self.generation += 1;
    }

    /// The segments that could hold statistics for `column_id` within
    /// `file_id_range`.
    ///
    /// This is the whole point of the manifest: it answers without touching a
    /// segment, so a selective query reads only the few segments that matter.
    /// Returned oldest first, by [`SegmentEntry::seq`], because the reader folds
    /// them in that order and lets the newest row for a file win.
    pub fn candidates(
        &self,
        column_id: ColumnId,
        file_id_range: (FileId, FileId),
    ) -> Vec<&SegmentEntry> {
        let mut found: Vec<&SegmentEntry> = self
            .segments
            .iter()
            .filter(|entry| entry.overlaps(file_id_range) && entry.contains_column(column_id))
            .collect();
        found.sort_by_key(|entry| entry.seq);
        found
    }

    /// Files covered across every segment, tombstones included.
    pub fn num_files(&self) -> u64 {
        self.segments.iter().map(|s| s.num_files).sum()
    }

    /// Load the manifest, or an empty one when the store holds none yet.
    pub async fn load(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Self> {
        let path = prefix.clone().join(MANIFEST_NAME);
        match store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                bincode::deserialize(&bytes)
                    .map_err(|e| FileStatsError::Format(format!("manifest: {e}")))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self::new()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn save(&self, store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<()> {
        let bytes = bincode::serialize(self)
            .map_err(|e| FileStatsError::Format(format!("manifest: {e}")))?;
        store
            .put(&prefix.clone().join(MANIFEST_NAME), bytes.into())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn entry(name: &str, files: (FileId, FileId), columns: &[ColumnId]) -> SegmentEntry {
        SegmentEntry {
            seq: 0,
            name: name.to_string(),
            min_file_id: files.0,
            max_file_id: files.1,
            num_files: files.1 - files.0 + 1,
            column_ids: columns.to_vec(),
        }
    }

    #[test]
    fn a_segment_is_skipped_on_either_test() {
        let mut manifest = Manifest::new();
        manifest.add_segment(entry("a", (0, 99), &[1, 2, 3]));
        manifest.add_segment(entry("b", (100, 199), &[3, 4]));

        // Column present, file range disjoint.
        assert!(manifest.candidates(3, (300, 400)).is_empty());
        // File range overlaps, column absent.
        assert!(manifest.candidates(9, (0, 199)).is_empty());
        // Both hold.
        let hits = manifest.candidates(3, (0, 199));
        assert_eq!(hits.len(), 2);
        let hits = manifest.candidates(1, (0, 199));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "a");
    }

    #[test]
    fn overlap_includes_the_boundaries() {
        let segment = entry("a", (10, 20), &[1]);
        assert!(segment.overlaps((20, 30)));
        assert!(segment.overlaps((0, 10)));
        assert!(!segment.overlaps((21, 30)));
        assert!(!segment.overlaps((0, 9)));
    }

    /// Recency must follow `seq`, not position, because a compaction rewrites the
    /// list. If it followed position, a compacted segment could sort ahead of a
    /// fresher one and its stale range would win.
    #[test]
    fn candidates_come_back_oldest_first_whatever_the_list_order() {
        let mut manifest = Manifest::new();
        let mut old = entry("compacted", (0, 99), &[1]);
        old.seq = 2;
        let mut new = entry("fresh", (0, 99), &[1]);
        new.seq = 9;

        // Pushed newest first, as a compaction that replaced entries would leave
        // them.
        manifest.segments.push(new);
        manifest.segments.push(old);

        let order: Vec<&str> = manifest
            .candidates(1, (0, 99))
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert_eq!(order, vec!["compacted", "fresh"]);
    }

    /// A name is never handed out twice, however far the list shrinks.
    #[test]
    fn sequence_numbers_never_go_backwards() {
        let mut manifest = Manifest::new();
        for _ in 0..3 {
            let seq = manifest.claim_seq();
            let mut e = entry(&format!("segment-{seq}"), (0, 9), &[1]);
            e.seq = seq;
            manifest.add_segment(e);
        }
        assert_eq!(manifest.next_seq, 3);

        // A compaction collapses all three into one.
        manifest.segments.clear();
        let seq = manifest.claim_seq();
        assert_eq!(seq, 3, "the next name must not collide with a retired one");
    }

    #[tokio::test]
    async fn a_missing_manifest_loads_as_empty_and_round_trips() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("stats");

        let loaded = Manifest::load(&store, &prefix).await.unwrap();
        assert!(loaded.segments.is_empty());

        let mut manifest = Manifest::new();
        manifest.add_segment(entry("a", (0, 9), &[1, 5]));
        manifest.save(&store, &prefix).await.unwrap();

        let reloaded = Manifest::load(&store, &prefix).await.unwrap();
        assert_eq!(reloaded, manifest);
        assert_eq!(reloaded.generation, 1);
    }
}
