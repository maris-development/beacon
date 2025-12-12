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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPartitionGroupMetadata {
    pub array_name: String,
    pub total_bytes_size: usize,
    pub combined_num_elements: usize,
    pub data_type: arrow::datatypes::DataType,
    pub partitions: IndexMap<usize, ArrayPartitionMetadata>,
}

pub struct ArrayPartitionGroup {
    metadata: ArrayPartitionGroupMetadata,
}

impl ArrayPartitionGroup {
    const META_PATH: &str = "apg.json";

    pub fn metadata(&self) -> &ArrayPartitionGroupMetadata {
        &self.metadata
    }
}

pub struct ArrayPartitionGroupReader {
    metadata: ArrayPartitionGroupMetadata,
    object_store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    io_cache: ArrayIoCache,
}

impl ArrayPartitionGroupReader {
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

    pub fn metadata(&self) -> &ArrayPartitionGroupMetadata {
        &self.metadata
    }

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

pub struct ArrayPartitionGroupWriter {
    metadata: ArrayPartitionGroupMetadata,
}

impl ArrayPartitionGroupWriter {
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

        self.metadata.total_bytes_size += partition_metadata.partition_byte_size;
        self.metadata.combined_num_elements += partition_metadata.num_elements;
        self.metadata
            .partitions
            .insert(partition_index, partition_metadata);

        Ok(())
    }
}
