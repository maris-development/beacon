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
    collection_partition::{
        CollectionPartitionMetadata, CollectionPartitionReader,
        apply_entry_mask_deleted_by_entry_keys,
    },
    error::{BBFReadingError, BBFResult, BBFWritingError},
    io_cache::ArrayIoCache,
    layout::PARTITIONS_DIR,
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
                let partition_path = self
                    .root_path
                    .child(PARTITIONS_DIR.to_string())
                    .child(partition_name.to_string());
                CollectionPartitionReader::new(
                    partition_path,
                    self.object_store.clone(),
                    partition_metadata,
                    self.io_cache.clone(),
                )
            })
    }

    /// Returns an iterator over all partition names in this collection.
    ///
    /// The iteration order matches the order stored in `bbf.json`.
    pub fn partition_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.metadata.partitions.keys().map(|name| name.as_str())
    }

    /// Returns an iterator over all partitions and their metadata.
    ///
    /// The iteration order matches the order stored in `bbf.json`.
    pub fn partitions(&self) -> impl Iterator<Item = (&str, &CollectionPartitionMetadata)> + '_ {
        self.metadata
            .partitions
            .iter()
            .map(|(name, metadata)| (name.as_str(), metadata))
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

    /// Logically delete entries from a partition by matching `__entry_key` values.
    ///
    /// This updates the target partition by writing/merging `entry_mask.bbem` +
    /// updating `resolution.json`, and updates the in-memory `bbf.json` metadata
    /// for the partition (specifically `entry_mask_hash`). Call `persist()` to
    /// store the updated collection metadata.
    pub async fn delete_entries_in_partition_by_entry_keys(
        &mut self,
        partition_name: &str,
        entry_keys_to_delete: impl IntoIterator<Item = String>,
    ) -> BBFResult<()> {
        let Some(partition_metadata) = self.metadata.partitions.get_mut(partition_name) else {
            return Err(BBFReadingError::CollectionMetadataDecode {
                meta_path: self.root_path.child(COLLECTION_META_FILE).to_string(),
                reason: format!("partition '{partition_name}' not found"),
            }
            .into());
        };

        let partition_path = self
            .root_path
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.to_string());

        apply_entry_mask_deleted_by_entry_keys(
            self.object_store.clone(),
            partition_path,
            partition_metadata,
            entry_keys_to_delete,
        )
        .await?;

        Ok(())
    }

    /// Logically delete entries across every partition by matching `__entry_key` values.
    ///
    /// This applies the same delete set to each partition in the collection, updating
    /// `entry_mask.bbem`, `resolution.json`, and the in-memory `bbf.json` metadata for
    /// each partition. Call `persist()` to store the updated collection metadata.
    ///
    /// Returns the number of partitions processed.
    pub async fn delete_entries_across_partitions_by_entry_keys(
        &mut self,
        entry_keys_to_delete: impl IntoIterator<Item = String>,
    ) -> BBFResult<usize> {
        let keys: Vec<String> = entry_keys_to_delete.into_iter().collect();
        if keys.is_empty() {
            return Ok(0);
        }

        let mut processed = 0usize;
        for (partition_name, partition_metadata) in self.metadata.partitions.iter_mut() {
            let partition_path = self
                .root_path
                .child(PARTITIONS_DIR.to_string())
                .child(partition_name.to_string());

            apply_entry_mask_deleted_by_entry_keys(
                self.object_store.clone(),
                partition_path,
                partition_metadata,
                keys.clone(),
            )
            .await?;
            processed = processed.saturating_add(1);
        }

        Ok(processed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field};
    use futures::{StreamExt, stream};
    use nd_arrow_array::NdArrowArray;
    use nd_arrow_array::dimensions::{Dimension, Dimensions};
    use object_store::memory::InMemory;
    use std::sync::Arc as StdArc;

    use crate::collection_partition::{
        CollectionPartitionReadOptions, CollectionPartitionWriter, WriterOptions,
    };

    fn scalar_int32(values: &[i32]) -> NdArrowArray {
        let array: ArrayRef = StdArc::new(Int32Array::from(values.to_vec()));
        let dimension = Dimension {
            name: "dim0".to_string(),
            size: values.len(),
        };
        NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
            .expect("nd array creation")
    }

    fn sample_partition(byte_size: usize, num_elements: usize) -> CollectionPartitionMetadata {
        let schema = Arc::new(Schema::new(vec![Field::new("temp", DataType::Int32, true)]));
        CollectionPartitionMetadata {
            byte_size,
            num_elements,
            num_entries: 1,
            partition_schema: schema,
            partition_name: "sample-partition".to_string(),
            arrays: IndexMap::new(),
            entry_mask_hash: None,
        }
    }

    fn sample_partition_named(
        partition_name: &str,
        byte_size: usize,
        num_elements: usize,
    ) -> CollectionPartitionMetadata {
        let mut meta = sample_partition(byte_size, num_elements);
        meta.partition_name = partition_name.to_string();
        meta
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

    #[tokio::test]
    async fn reader_exposes_all_partitions_for_iteration() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("collections/partition-iter");

        let mut writer = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer init");
        writer
            .append_partition(sample_partition_named("p1", 1, 1))
            .expect("append p1");
        writer
            .append_partition(sample_partition_named("p2", 1, 1))
            .expect("append p2");
        writer.persist().await.expect("persist");

        let reader = CollectionReader::new(store.clone(), root.clone(), ArrayIoCache::new(1024))
            .await
            .expect("reader init");

        let names: Vec<&str> = reader.partition_names().collect();
        assert_eq!(names, vec!["p1", "p2"]);

        let entries: Vec<&str> = reader.partitions().map(|(name, _)| name).collect();
        assert_eq!(entries, vec!["p1", "p2"]);
    }

    #[tokio::test]
    async fn delete_entry_by_key_persists_and_filters_reads() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("collections/delete-demo");

        // Create one partition with two entries.
        let mut partition_writer = CollectionPartitionWriter::new(
            root.clone(),
            store.clone(),
            "p1".to_string(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );
        let temp_field: arrow_schema::FieldRef =
            StdArc::new(Field::new("temp", DataType::Int32, true));

        partition_writer
            .write_entry(
                "k1",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[1]))]),
            )
            .await
            .expect("write k1");
        partition_writer
            .write_entry(
                "k2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[2]))]),
            )
            .await
            .expect("write k2");

        let mut partition_meta = partition_writer.finish().await.expect("finish");
        partition_meta.partition_schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Int32, true),
            Field::new("__entry_key", DataType::Utf8, false),
        ]));

        // Persist collection metadata.
        let mut writer = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer init");
        writer.append_partition(partition_meta).expect("append");
        writer.persist().await.expect("persist");

        // Re-open, delete k2 from p1, persist.
        let mut writer2 = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer reload");
        writer2
            .delete_entries_in_partition_by_entry_keys("p1", vec!["k2".to_string()])
            .await
            .expect("delete");
        writer2.persist().await.expect("persist after delete");

        // Read and confirm only one entry remains.
        let reader =
            CollectionReader::new(store.clone(), root.clone(), ArrayIoCache::new(1024 * 1024))
                .await
                .expect("reader init");
        let partition_reader = reader.partition_reader("p1").expect("partition reader");
        let scheduler = partition_reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("read");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 1);
    }

    async fn write_two_entry_partition(
        store: Arc<dyn ObjectStore>,
        root: Path,
        partition_name: &str,
        v1: i32,
        v2: i32,
    ) -> CollectionPartitionMetadata {
        let mut partition_writer = CollectionPartitionWriter::new(
            root.clone(),
            store,
            partition_name.to_string(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: arrow_schema::FieldRef =
            StdArc::new(Field::new("temp", DataType::Int32, true));

        partition_writer
            .write_entry(
                "k1",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[v1]))]),
            )
            .await
            .expect("write k1");
        partition_writer
            .write_entry(
                "k2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[v2]))]),
            )
            .await
            .expect("write k2");

        let mut meta = partition_writer.finish().await.expect("finish");
        meta.partition_schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Int32, true),
            Field::new("__entry_key", DataType::Utf8, false),
        ]));
        meta
    }

    #[tokio::test]
    async fn delete_entry_by_key_across_collection_filters_all_partitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("collections/delete-demo-all");

        let p1 = write_two_entry_partition(store.clone(), root.clone(), "p1", 1, 2).await;
        let p2 = write_two_entry_partition(store.clone(), root.clone(), "p2", 10, 20).await;

        let mut writer = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer init");
        writer.append_partition(p1).expect("append p1");
        writer.append_partition(p2).expect("append p2");
        writer.persist().await.expect("persist");

        let mut writer2 = CollectionWriter::new(store.clone(), root.clone())
            .await
            .expect("writer reload");
        let processed = writer2
            .delete_entries_across_partitions_by_entry_keys(vec!["k2".to_string()])
            .await
            .expect("delete across");
        assert_eq!(processed, 2);
        writer2.persist().await.expect("persist after delete");

        let reader =
            CollectionReader::new(store.clone(), root.clone(), ArrayIoCache::new(1024 * 1024))
                .await
                .expect("reader init");
        for name in ["p1", "p2"] {
            let partition_reader = reader.partition_reader(name).expect("partition reader");
            let scheduler = partition_reader
                .read(
                    None,
                    CollectionPartitionReadOptions {
                        max_concurrent_reads: 2,
                        entry_selection: None,
                    },
                )
                .await
                .expect("read");
            let stream = scheduler.shared_pollable_stream_ref().await;
            let batches = stream.collect::<Vec<_>>().await;
            assert_eq!(batches.len(), 1, "{name} should be filtered");
        }
    }
}
