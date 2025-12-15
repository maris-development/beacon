use std::sync::Arc;

use arrow_schema::{ArrowError, DataType};
use nd_arrow_array::error::NdArrayError;
use object_store::Error as ObjectStoreError;

use crate::util::SuperTypeError;

pub type BBFResult<T> = std::result::Result<T, BBFError>;

#[derive(Debug, thiserror::Error)]
pub enum BBFError {
    #[error("BBF writing error: {0}")]
    Writing(#[from] BBFWritingError),
    #[error("BBF reading error: {0}")]
    Reading(#[from] BBFReadingError),
    #[error("Shared error: {0}")]
    Shared(#[from] Arc<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum BBFWritingError {
    #[error("Failed to cast arrow array with data type {0} => {1} : {2}")]
    ArrowCastFailure(DataType, DataType, ArrowError),
    #[error("Failed to create NdArrowArray: {0}: {1}")]
    NdArrowArrayCreationFailure(NdArrayError, String),
    #[error("Unable to find super type for array group: {0}")]
    SuperTypeNotFound(SuperTypeError),
    #[error("Failed to build array group: {0}")]
    ArrayGroupBuildFailure(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to create temporary file: {0} for array partition: {1}")]
    TempFileCreationFailure(std::io::Error, String),
    #[error("Failed to write array group to partition: {0}")]
    ArrayGroupWriteFailure(ArrowError),
    #[error("Failed to finalize array partition: {0}")]
    ArrayPartitionFinalizeFailure(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to write pruning index for array partition: {0}")]
    ArrayPartitionPruningIndexWriteFailure(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to write collection metadata: {0}")]
    CollectionMetadataWriteFailure(Box<dyn std::error::Error + Send + Sync>),
    #[error("Collection schema mismatch: expected {expected}, observed {actual}")]
    CollectionSchemaMismatch { expected: String, actual: String },
}

#[derive(Debug, thiserror::Error)]
pub enum BBFReadingError {
    #[error("Failed reading array from array group: {0} with error: {1}")]
    ArrayGroupReadFailure(String, Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to fetch metadata for array partition group at {meta_path}: {source}")]
    PartitionGroupMetadataFetch {
        meta_path: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("Failed to decode metadata for array partition group at {meta_path}: {reason}")]
    PartitionGroupMetadataDecode { meta_path: String, reason: String },
    #[error("Failed to fetch metadata for collection at {meta_path}: {source}")]
    CollectionMetadataFetch {
        meta_path: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("Failed to decode metadata for collection at {meta_path}: {reason}")]
    CollectionMetadataDecode { meta_path: String, reason: String },
    #[error("Failed to fetch bytes for partition {partition_path}: {source}")]
    PartitionBytesFetch {
        partition_path: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error(
        "Partition {partition_path} is smaller than the required Arrow IPC trailer ({required} bytes), actual size: {actual}"
    )]
    PartitionTooSmall {
        partition_path: String,
        required: u64,
        actual: u64,
    },
    #[error("Invalid Arrow IPC footer for partition {partition_path}: {reason}")]
    PartitionFooterDecode {
        partition_path: String,
        reason: String,
    },
    #[error(
        "Partition {partition_path} group index {group_index} is out of bounds (total groups: {total_groups})"
    )]
    PartitionGroupIndexOutOfBounds {
        partition_path: String,
        group_index: usize,
        total_groups: usize,
    },
    #[error("Failed to fetch bytes for partition {partition_path} group {group_index}: {source}")]
    PartitionGroupBytesFetch {
        partition_path: String,
        group_index: usize,
        #[source]
        source: ObjectStoreError,
    },
    #[error("Invalid block length for partition {partition_path} group {group_index}: {reason}")]
    PartitionGroupLengthInvalid {
        partition_path: String,
        group_index: usize,
        reason: String,
    },
    #[error(
        "Failed to decode record batch for partition {partition_path} group {group_index}: {reason}"
    )]
    PartitionGroupDecode {
        partition_path: String,
        group_index: usize,
        reason: String,
    },
    #[error("Failed to decode pruning index for partition {partition_path}: {reason}")]
    PartitionPruningIndexDecode {
        partition_path: String,
        reason: String,
    },
}
