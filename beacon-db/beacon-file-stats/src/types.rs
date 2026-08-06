//! Identities and the per-file record the registry keeps.

use serde::{Deserialize, Serialize};

/// A dense, stable ordinal for one file path.
///
/// Ids exist so a segment can reference a file in 8 bytes. A 200-byte path
/// repeated across 50M cells costs 10 GB; the ids cost 400 MB. Ids never shift:
/// a delete sets [`FileState::Deleted`] and keeps the slot, so a segment written
/// last year still means what it said. Only compaction renumbers.
pub type FileId = u64;

/// A dense ordinal for one column name.
///
/// The same reasoning as [`FileId`], plus it keeps the per-segment column index
/// to 4 bytes per entry instead of a name.
pub type ColumnId = u32;

/// Where a file sits in the statistics lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileState {
    /// Known to the registry, not analyzed yet.
    Pending,
    /// Statistics are committed to a segment.
    Analyzed,
    /// Analysis failed. The collector may retry.
    Failed,
    /// The file changed after analysis. Readers treat its statistics as absent.
    Stale,
    /// The file is gone. The slot stays so ids never shift.
    Deleted,
}

/// What the registry knows about one file.
///
/// `num_rows` and `total_byte_size` live here on purpose. DataFusion asks for
/// those per file, and answering from the registry means that question never
/// touches a column block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRecord {
    pub path: String,
    pub size: u64,
    pub last_modified_millis: i64,
    pub e_tag: Option<String>,
    /// The file format that produced the statistics, for diagnostics.
    pub format: String,
    pub num_rows: Option<u64>,
    pub total_byte_size: Option<u64>,
    /// Columns this file contributed statistics for.
    ///
    /// Zero is the interesting value. A format that returns no ranges (ODV,
    /// Zarr, TIFF, CSV today) analyzes cleanly and contributes nothing, which
    /// otherwise looks identical to a format that works. Recording it is what
    /// lets an operator see `odv: 12000 files analyzed, 0 columns` instead of
    /// wondering why pruning never helps.
    pub column_count: u32,
    pub state: FileState,
    /// Bumped every time the collector rewrites this file's statistics. Lets a
    /// reader tell a stale segment entry from a current one.
    pub stats_epoch: u64,
}

impl FileRecord {
    /// A newly discovered file, not analyzed yet.
    pub fn pending(path: impl Into<String>, size: u64, last_modified_millis: i64) -> Self {
        Self {
            path: path.into(),
            size,
            last_modified_millis,
            e_tag: None,
            format: String::new(),
            num_rows: None,
            total_byte_size: None,
            column_count: 0,
            state: FileState::Pending,
            stats_epoch: 0,
        }
    }

    /// Whether the committed statistics still describe `observed`.
    ///
    /// An etag settles it when both sides carry one. Otherwise size and
    /// last-modified decide. A doubtful match reads as changed, because a wrong
    /// "unchanged" silently prunes real rows away.
    pub fn matches(&self, observed: &ObservedFile) -> bool {
        match (&self.e_tag, &observed.e_tag) {
            (Some(mine), Some(theirs)) => mine == theirs,
            _ => self.size == observed.size && self.last_modified_millis == observed.last_modified_millis,
        }
    }
}

/// What a listing reports about a file right now, before the registry knows it.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedFile {
    pub path: String,
    pub size: u64,
    pub last_modified_millis: i64,
    pub e_tag: Option<String>,
}

impl ObservedFile {
    pub fn new(path: impl Into<String>, size: u64, last_modified_millis: i64) -> Self {
        Self {
            path: path.into(),
            size,
            last_modified_millis,
            e_tag: None,
        }
    }

    pub fn with_e_tag(mut self, e_tag: Option<String>) -> Self {
        self.e_tag = e_tag;
        self
    }
}
