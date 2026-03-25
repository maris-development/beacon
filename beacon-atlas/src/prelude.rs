//! Convenience re-exports for common Atlas collection workflows.
//!
//! This prelude focuses on creating and managing collections and partitions.

pub use crate::collection::{
    AtlasCollection, AtlasCollectionState, CollectionMetadata, CollectionPartitionWriter,
};
pub use crate::column::Column;
pub use crate::partition::ops::read::Dataset;
pub use crate::schema::{AtlasColumn, AtlasSchema, AtlasSuperTypingMode};
