//! Building blocks for the Beacon binary format.
//!
//! This crate hosts the low-level readers, writers, caches, and helper
//! utilities that manage Arrow-centric array partitions stored inside an
//! object store.  Each module focuses on a single concern so applications can
//! pick the components they need: from `array_partition` for authoring Arrow
//! IPC blobs, to `collection` for orchestrating partition metadata, and
//! `io_cache` for deduplicating expensive round-trips.
//!
//! The public API is intentionally granular; production services typically
//! compose higher-level workflows by mixing the provided writers/readers with
//! their own scheduling and persistence layers.
//!
//!
//! Format Overview
//!
//! /file.bbf/
//!     ├── bbf.json
//!     ├── partitions/   
//!     │   ├── hash1/
//!     │   │   ├── partition_blob.bbb (contains all the arrays for this partition. )
//!     │   │   ├── resolution.json (contains metadata for resolving hash to the offset + size of each array in the blob)
//!     │   │   ├── pruning_index.bbpi (optional pruning index blob containing concatenated indices; individual indices are resolved via resolution.json using the pruning index hash)
//!     │   │   ├── entry_mask.bbem (optional entry mask for the partition, used to filter entries. Should be resolved via resolution.json)
//!     │   ├── hash2/
//!    │   │   ├── ...

pub mod array_group;
pub mod array_partition;
pub mod array_partition_group;
pub mod array_partition_index;
pub mod collection;
pub mod collection_partition;
pub mod error;
pub mod io_cache;
pub mod layout;
pub mod partition_resolution;
pub mod stream;
pub mod util;
