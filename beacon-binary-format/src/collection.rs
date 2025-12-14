//! Collection metadata management.
//!
//! A _collection_ orchestrates a set of `CollectionPartition`s that share a
//! logical schema.  The metadata stored in `bbf.json` is the primary contract
//! consumers rely on to discover which partitions exist, how large they are,
//! and which Arrow schema they adhere to.  This module exposes a
//! `CollectionWriter` that keeps the metadata consistent while new partitions
//! are produced, and a `CollectionReader` that hands out partition readers for
//! downstream consumers.

use std::sync::Arc;

use arrow::datatypes::Schema;
use indexmap::IndexMap;
use object_store::{Error as ObjectStoreError, ObjectStore, PutPayload, path::Path};
use serde::{Deserialize, Serialize};

use crate::{
    collection_partition::{CollectionPartitionMetadata, CollectionPartitionReader},
    error::{BBFReadingError, BBFResult, BBFWritingError},
    io_cache::ArrayIoCache,
};

pub const COLLECTION_META_FILE: &str = "bbf.json";
fn default_library_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Aggregate metadata for a collection spanning multiple collection partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// Sum of partition byte sizes.
    pub collection_byte_size: usize,
    /// Sum of partition element counts.
    pub collection_num_elements: usize,
    /// Map of partition index to its metadata descriptor.
    pub partitions: IndexMap<String, CollectionPartitionMetadata>,
    /// Logical schema shared by every partition in the collection.
    pub schema: Arc<Schema>,
    /// Version of `beacon-binary-format` that produced the metadata.
    #[serde(default = "default_library_version")]
    pub library_version: String,
}

impl CollectionMetadata {
    fn empty() -> Self {
        Self {
            collection_byte_size: 0,
            collection_num_elements: 0,
            partitions: IndexMap::new(),
            schema: Arc::new(Schema::empty()),
            library_version: default_library_version(),
        }
    }
}

/// Reader that loads collection metadata and hands out collection partition readers.
pub struct CollectionReader {
    metadata: CollectionMetadata,
    object_store: Arc<dyn ObjectStore>,
    root_path: Path,
    io_cache: ArrayIoCache,
}

impl CollectionReader {
    /// Loads `collection.json` under `root_path` and prepares to serve partition readers.
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        root_path: Path,
        io_cache: ArrayIoCache,
    ) -> BBFResult<Self> {
        let meta_path = root_path.child(COLLECTION_META_FILE);
        let meta_display = meta_path.to_string();
        let meta_object = object_store.get(&meta_path).await.map_err(|source| {
            BBFReadingError::CollectionMetadataFetch {
                meta_path: meta_display.clone(),
                source,
            }
        })?;
        let bytes = meta_object.bytes().await.map_err(|source| {
            BBFReadingError::CollectionMetadataFetch {
                meta_path: meta_display.clone(),
                source,
            }
        })?;
        let metadata = serde_json::from_slice(&bytes).map_err(|err| {
            BBFReadingError::CollectionMetadataDecode {
                meta_path: meta_display,
                reason: err.to_string(),
            }
        })?;

        Ok(Self {
            metadata,
            object_store,
            root_path,
            io_cache,
        })
    }

    /// Returns the cached metadata snapshot.
    pub fn metadata(&self) -> &CollectionMetadata {
        &self.metadata
    }

    /// Builds a `CollectionPartitionReader` for `partition_index` when metadata is present.
    pub fn partition_reader(&self, partition_name: &str) -> Option<CollectionPartitionReader> {
        self.metadata
            .partitions
            .get(partition_name)
            .cloned()
            .map(|partition_metadata| {
                let partition_path = self.root_path.child(partition_name.to_string());
                CollectionPartitionReader::new(
                    partition_path,
                    self.object_store.clone(),
                    partition_metadata,
                    self.io_cache.clone(),
                )
            })
    }
}

/// Writer that manages collection metadata while new collection partitions are appended.
pub struct CollectionWriter {
    metadata: CollectionMetadata,
    object_store: Arc<dyn ObjectStore>,
    root_path: Path,
}

impl CollectionWriter {
    /// Loads existing metadata if present, otherwise starts from an empty descriptor.
    pub async fn new(object_store: Arc<dyn ObjectStore>, root_path: Path) -> BBFResult<Self> {
        let meta_path = root_path.child(COLLECTION_META_FILE);
        let meta_display = meta_path.to_string();
        let mut metadata = match object_store.get(&meta_path).await {
            Ok(existing) => {
                let bytes = existing.bytes().await.map_err(|source| {
                    BBFReadingError::CollectionMetadataFetch {
                        meta_path: meta_display.clone(),
                        source,
                    }
                })?;
                serde_json::from_slice(&bytes).map_err(|err| {
                    BBFReadingError::CollectionMetadataDecode {
                        meta_path: meta_display.clone(),
                        reason: err.to_string(),
                    }
                })?
            }
            Err(ObjectStoreError::NotFound { .. }) => CollectionMetadata::empty(),
            Err(source) => {
                return Err(BBFReadingError::CollectionMetadataFetch {
                    meta_path: meta_display,
                    source,
                }
                .into());
            }
        };

        if metadata.library_version.is_empty() {
            metadata.library_version = default_library_version();
        }

        Ok(Self {
            metadata,
            object_store,
            root_path,
        })
    }

    /// Exposes the current metadata snapshot.
    pub fn metadata(&self) -> &CollectionMetadata {
        &self.metadata
    }

    /// Records `partition_metadata` in the collection, ensuring schema
    /// compatibility and unique partition names.
    pub fn append_partition(
        &mut self,
        partition_metadata: CollectionPartitionMetadata,
    ) -> BBFResult<()> {
        // Check if partition with the same name already exists
        if self
            .metadata
            .partitions
            .contains_key(&partition_metadata.partition_name)
        {
            return Err(BBFWritingError::CollectionSchemaMismatch {
                expected: format!(
                    "Unique partition name, found duplicate: {}",
                    partition_metadata.partition_name
                ),
                actual: format!("{:?}", partition_metadata.partition_schema.as_ref()),
            }
            .into());
        }

        if self.metadata.schema.fields().is_empty() {
            self.metadata.schema = partition_metadata.partition_schema.clone();
        } else if self.metadata.schema.as_ref() != partition_metadata.partition_schema.as_ref() {
            // Super type schema
            todo!()
        }

        self.metadata.partitions.insert(
            partition_metadata.partition_name.clone(),
            partition_metadata.clone(),
        );
        self.metadata.collection_byte_size += partition_metadata.byte_size;
        self.metadata.collection_num_elements += partition_metadata.num_elements;

        Ok(())
    }

    /// Persists the metadata to `collection.json`.
    pub async fn persist(&self) -> BBFResult<()> {
        let meta_path = self.root_path.child(COLLECTION_META_FILE);
        let payload = serde_json::to_vec(&self.metadata)
            .map_err(|err| BBFWritingError::CollectionMetadataWriteFailure(Box::new(err)))?;
        self.object_store
            .put(&meta_path, PutPayload::from_bytes(payload.into()))
            .await
            .map_err(|err| BBFWritingError::CollectionMetadataWriteFailure(Box::new(err)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use object_store::memory::InMemory;

    fn sample_partition(byte_size: usize, num_elements: usize) -> CollectionPartitionMetadata {
        let schema = Arc::new(Schema::new(vec![Field::new("temp", DataType::Int32, true)]));
        CollectionPartitionMetadata {
            byte_size,
            num_elements,
            num_entries: 1,
            partition_schema: schema,
            partition_name: "sample-partition".to_string(),
            arrays: IndexMap::new(),
        }
    }

    #[tokio::test]
    async fn writer_appends_and_persists_metadata() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("collections/demo");
        let mut writer = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer init");

        writer
            .append_partition(sample_partition(64, 10))
            .expect("append partition");
        writer.persist().await.expect("persist metadata");

        let writer_again = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("reload metadata");

        assert_eq!(writer_again.metadata().collection_byte_size, 64);
        assert_eq!(writer_again.metadata().collection_num_elements, 10);
        assert!(
            writer_again
                .metadata()
                .partitions
                .contains_key("sample-partition")
        );
        assert_eq!(
            writer_again.metadata().library_version,
            default_library_version()
        );
    }

    #[tokio::test]
    async fn reader_loads_metadata_and_returns_partition_reader() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("collections/readers");
        let mut writer = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer init");
        writer
            .append_partition(sample_partition(32, 5))
            .expect("append partition");
        writer.persist().await.expect("persist metadata");

        let reader = CollectionReader::new(store.clone(), root.clone(), ArrayIoCache::new(1024))
            .await
            .expect("reader init");
        assert_eq!(reader.metadata().partitions.len(), 1);
        assert_eq!(reader.metadata().library_version, default_library_version());

        let partition_reader = reader
            .partition_reader("sample-partition")
            .expect("reader exists");
        assert_eq!(partition_reader.metadata.num_entries, 1);
        assert_eq!(partition_reader.metadata.num_elements, 5);
    }
}
