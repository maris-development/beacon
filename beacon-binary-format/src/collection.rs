use std::sync::Arc;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::collection_partition::CollectionPartitionMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    collection_byte_size: usize,
    collection_num_elements: usize,
    partitions: IndexMap<usize, CollectionPartitionMetadata>,
    schema: Arc<arrow::datatypes::Schema>,
}
