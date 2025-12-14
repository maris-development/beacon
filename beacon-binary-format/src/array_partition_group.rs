//! Metadata helpers for collections of array partitions.
//!
//! Array partition groups track every Arrow partition produced for a single
//! logical array.  The reader and writer surfaces defined here own the JSON
//! descriptor, enforce schema compatibility, and expose ergonomic helpers for
//! fetching concrete `ArrayPartitionReader`s.

use std::sync::Arc;

use arrow_schema::DataType;
use indexmap::IndexMap;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::{
    array_partition::{ArrayPartitionMetadata, ArrayPartitionReader},
    error::{BBFReadingError, BBFResult},
    io_cache::ArrayIoCache,
    util::super_type_arrow,
};

/// Metadata persisted for a set of array partitions sharing the same logical
/// array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPartitionGroupMetadata {
    pub array_name: String,
    pub total_bytes_size: usize,
    pub combined_num_elements: usize,
    pub data_type: arrow::datatypes::DataType,
    pub partitions: IndexMap<usize, ArrayPartitionMetadata>,
}

/// Wrapper around group metadata to allow future helpers on the reader/writer
/// side.
pub struct ArrayPartitionGroup {
    metadata: ArrayPartitionGroupMetadata,
}

impl ArrayPartitionGroup {
    const META_PATH: &str = "apg.json";

    /// Returns the metadata envelope describing this partition group.
    pub fn metadata(&self) -> &ArrayPartitionGroupMetadata {
        &self.metadata
    }
}

/// Provides lazy access to individual partitions referenced by a group.
pub struct ArrayPartitionGroupReader {
    metadata: ArrayPartitionGroupMetadata,
    object_store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    io_cache: ArrayIoCache,
}

impl ArrayPartitionGroupReader {
    /// Construct a reader by downloading the group metadata json and preparing
    /// to lazily open individual partitions when requested.
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        array_io_cache: ArrayIoCache,
    ) -> BBFResult<Self> {
        // Read metadata
        let meta_path = path.child(ArrayPartitionGroup::META_PATH);
        let meta_display = meta_path.to_string();
        let metadata_bytes = object_store
            .get(&meta_path)
            .await
            .map_err(|source| BBFReadingError::PartitionGroupMetadataFetch {
                meta_path: meta_display.clone(),
                source,
            })?
            .bytes()
            .await
            .map_err(|source| BBFReadingError::PartitionGroupMetadataFetch {
                meta_path: meta_display.clone(),
                source,
            })?;
        let metadata: ArrayPartitionGroupMetadata = serde_json::from_slice(&metadata_bytes)
            .map_err(|err| BBFReadingError::PartitionGroupMetadataDecode {
                meta_path: meta_display,
                reason: err.to_string(),
            })?;

        Ok(Self {
            metadata,
            object_store,
            path,
            io_cache: array_io_cache,
        })
    }

    /// Expose the group metadata to callers without re-fetching the json blob.
    pub fn metadata(&self) -> &ArrayPartitionGroupMetadata {
        &self.metadata
    }

    /// Returns an [`ArrayPartitionReader`] for the provided partition index if it
    /// exists, cloning the cached metadata and reusing the IO cache.
    pub async fn get_partition_reader(
        &self,
        partition: usize,
    ) -> BBFResult<Option<ArrayPartitionReader>> {
        if let Some(partition_metadata) = self.metadata.partitions.get(&partition) {
            let partition_path = self.path.child(partition_metadata.hash.clone());
            let partition_reader = ArrayPartitionReader::new(
                self.object_store.clone(),
                self.metadata.array_name.clone(),
                partition_path,
                partition_metadata.clone(),
                self.io_cache.clone(),
            )
            .await?;
            Ok(Some(partition_reader))
        } else {
            Ok(None)
        }
    }
}

/// Accumulates partition metadata and handles super-type resolution while
/// persisting the json descriptor back to object storage.
pub struct ArrayPartitionGroupWriter {
    metadata: ArrayPartitionGroupMetadata,
}

impl ArrayPartitionGroupWriter {
    /// Load existing metadata (when present) or initialize a blank descriptor.
    pub async fn new(
        array_blob_dir: object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
    ) -> BBFResult<Self> {
        // Read existing metadata if exists
        let meta_path = array_blob_dir.child(ArrayPartitionGroup::META_PATH);
        let metadata = if let Ok(metadata_bytes) = object_store.get(&meta_path).await {
            let bytes = metadata_bytes.bytes().await.map_err(|source| {
                BBFReadingError::PartitionGroupMetadataFetch {
                    meta_path: meta_path.to_string(),
                    source,
                }
            })?;
            serde_json::from_slice(&bytes).map_err(|err| {
                BBFReadingError::PartitionGroupMetadataDecode {
                    meta_path: meta_path.to_string(),
                    reason: err.to_string(),
                }
            })?
        } else {
            ArrayPartitionGroupMetadata {
                array_name: String::new(),
                total_bytes_size: 0,
                combined_num_elements: 0,
                data_type: DataType::Null,
                partitions: IndexMap::new(),
            }
        };

        Ok(Self { metadata })
    }

    /// Records metadata for `partition_index`, updating totals and upcasting
    /// the tracked Arrow data type if needed.
    pub fn append_partition(
        &mut self,
        partition_index: usize,
        partition_metadata: ArrayPartitionMetadata,
    ) -> BBFResult<()> {
        // Compare data types
        if self.metadata.data_type != partition_metadata.data_type {
            // Find super type
            let super_type =
                super_type_arrow(&self.metadata.data_type, &partition_metadata.data_type)
                    .ok_or_else(|| BBFReadingError::PartitionGroupMetadataDecode {
                        meta_path: "N/A".to_string(),
                        reason: format!(
                            "Unable to find super type for data types: {:?} and {:?}",
                            self.metadata.data_type, partition_metadata.data_type
                        ),
                    })?;
            self.metadata.data_type = super_type;
        }

        // Accumulate overall byte/row counts so clients can quickly estimate the
        // combined footprint without enumerating each partition.
        self.metadata.total_bytes_size += partition_metadata.partition_byte_size;
        self.metadata.combined_num_elements += partition_metadata.num_elements;
        self.metadata
            .partitions
            .insert(partition_index, partition_metadata);

        Ok(())
    }

    /// View the currently collected metadata prior to persisting it.
    pub fn metadata(&self) -> &ArrayPartitionGroupMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutPayload, memory::InMemory, path::Path};
    use std::sync::Arc as StdArc;

    /// Builds a lightweight `ArrayPartitionMetadata` pointing at a fake hash for
    /// use inside unit tests.
    fn sample_partition_metadata(
        hash: &str,
        data_type: DataType,
        byte_size: usize,
        elements: usize,
    ) -> ArrayPartitionMetadata {
        ArrayPartitionMetadata {
            num_elements: elements,
            partition_offset: 0,
            partition_byte_size: byte_size,
            hash: hash.to_string(),
            data_type,
            groups: IndexMap::new(),
        }
    }

    #[tokio::test]
    /// Writer falls back to empty metadata when no apg.json exists yet.
    async fn writer_initializes_default_metadata_when_missing_file() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/apg_defaults");

        let writer = ArrayPartitionGroupWriter::new(dir, store)
            .await
            .expect("writer init");
        let metadata = writer.metadata();

        assert!(metadata.partitions.is_empty());
        assert_eq!(metadata.total_bytes_size, 0);
        assert_eq!(metadata.combined_num_elements, 0);
        assert_eq!(metadata.data_type, DataType::Null);
    }

    #[tokio::test]
    /// Existing apg.json files are parsed and returned untouched.
    async fn writer_reads_existing_metadata_blob() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/apg_existing");
        let meta_path = dir.child(ArrayPartitionGroup::META_PATH);
        let mut metadata = ArrayPartitionGroupMetadata {
            array_name: "temp".to_string(),
            total_bytes_size: 10,
            combined_num_elements: 2,
            data_type: DataType::Int32,
            partitions: IndexMap::new(),
        };
        metadata.partitions.insert(
            0,
            sample_partition_metadata("hash-0", DataType::Int32, 10, 2),
        );
        let payload = serde_json::to_vec(&metadata).expect("serialize");
        store
            .put(&meta_path, PutPayload::from_bytes(payload.into()))
            .await
            .expect("write metadata");

        let writer = ArrayPartitionGroupWriter::new(dir, store)
            .await
            .expect("writer init");
        let metadata_ref = writer.metadata();

        assert_eq!(metadata_ref.array_name, "temp");
        assert_eq!(metadata_ref.total_bytes_size, 10);
        assert_eq!(metadata_ref.combined_num_elements, 2);
        assert_eq!(metadata_ref.data_type, DataType::Int32);
        assert!(metadata_ref.partitions.contains_key(&0));
    }

    #[tokio::test]
    /// Appending partitions updates totals and promotes data types to their
    /// appropriate super type.
    async fn append_partition_updates_totals_and_supertypes() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/apg_append");
        let mut writer = ArrayPartitionGroupWriter::new(dir, store)
            .await
            .expect("writer init");

        let int32_partition = sample_partition_metadata("hash-int", DataType::Int32, 128, 10);
        writer
            .append_partition(0, int32_partition.clone())
            .expect("append int32");
        let metadata = writer.metadata();
        assert_eq!(metadata.total_bytes_size, 128);
        assert_eq!(metadata.combined_num_elements, 10);
        assert_eq!(metadata.data_type, DataType::Int32);

        let utf8_partition = sample_partition_metadata("hash-str", DataType::Utf8, 64, 5);
        writer
            .append_partition(1, utf8_partition.clone())
            .expect("append utf8");

        let metadata = writer.metadata();
        assert_eq!(metadata.total_bytes_size, 192);
        assert_eq!(metadata.combined_num_elements, 15);
        assert_eq!(metadata.data_type, DataType::Utf8);
        assert_eq!(metadata.partitions.len(), 2);
        assert!(metadata.partitions.contains_key(&0));
        assert!(metadata.partitions.contains_key(&1));
    }
}
