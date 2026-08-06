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
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_segment(&mut self, entry: SegmentEntry) {
        self.segments.push(entry);
        self.generation += 1;
    }

    /// The segments that could hold statistics for `column_id` within
    /// `file_id_range`.
    ///
    /// This is the whole point of the manifest: it answers without touching a
    /// segment, so a selective query reads only the few segments that matter.
    pub fn candidates(
        &self,
        column_id: ColumnId,
        file_id_range: (FileId, FileId),
    ) -> Vec<&SegmentEntry> {
        self.segments
            .iter()
            .filter(|entry| entry.overlaps(file_id_range) && entry.contains_column(column_id))
            .collect()
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
