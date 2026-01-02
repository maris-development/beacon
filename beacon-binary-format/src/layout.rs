//! Shared on-disk layout constants.
//!
//! These constants define file and directory names used throughout the
//! `beacon-binary-format` crate. Centralizing them helps avoid drift between
//! writers, readers, tests, and documentation.

/// Directory under a collection root that contains per-partition folders.
pub const PARTITIONS_DIR: &str = "partitions";

/// Partition-level blob that contains all array IPC payloads.
pub const PARTITION_BLOB_FILE: &str = "partition_blob.bbb";

/// JSON file that maps content hashes to `{offset,size}` slices.
pub const PARTITION_RESOLUTION_FILE: &str = "resolution.json";

/// Partition-level blob that contains concatenated pruning index IPC payloads.
pub const PARTITION_PRUNING_INDEX_FILE: &str = "pruning_index.bbpi";
