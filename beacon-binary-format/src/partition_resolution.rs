//! Partition blob resolution metadata.
//!
//! The "blob" layout stores multiple logical objects (array partitions,
//! pruning indices, masks, ...) inside partition-level binary objects
//! (for example `partition_blob.bbb` and `pruning_index.bbpi`).
//! `resolution.json` maps a content hash to the byte offset + size of that
//! object within the appropriate blob.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// JSON envelope stored as `resolution.json` inside a partition directory.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionResolution {
    /// Mapping from object hash -> slice information in the blob.
    pub objects: IndexMap<String, ResolvedSlice>,
}

/// Byte slice inside a partition-level blob.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResolvedSlice {
    pub offset: u64,
    pub size: u64,
}
