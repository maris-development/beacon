use std::{fs::File, sync::Arc};

use arrow::{
    array::RecordBatch,
    ipc::{
        Block, CompressionType,
        reader::FileDecoder,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use arrow_schema::DataType;
use hmac_sha256::Hash;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tempfile::tempfile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{
    array_group::{ArrayGroup, ArrayGroupBuilder, ArrayGroupMetadata},
    error::{BBFError, BBFResult, BBFWritingError},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPartitionMetadata {
    pub num_elements: usize,
    pub hash: String,
    pub data_type: arrow::datatypes::DataType,
    #[serde(with = "range_index_map")]
    pub groups: IndexMap<std::ops::Range<usize>, ArrayGroupMetadata>,
}

/// Writer that groups arrays into Arrow IPC batches and uploads them to an
/// object store.
///
/// `ArrayPartitionWriter` maintains a temporary IPC file; groups appended
/// arrays into `ArrayGroup`s and writes them as record batches. On `finish()`
/// the temporary file is hashed and uploaded to the configured `ObjectStore`.
pub struct ArrayPartitionWriter {
    /// Hasher used to compute the hash of the final IPC file.
    pub hasher: Hash,
    /// Optional temp file writer used while building the IPC file.
    pub temp_file: Option<arrow::ipc::writer::FileWriter<std::fs::File>>,
    /// The object store to which the completed partition will be uploaded.
    /// Object store used to upload finalized partition files.
    pub store: Arc<dyn ObjectStore>,
    /// Directory path in the object store where files are written.
    pub dir: object_store::path::Path,
    /// Name of the array being written.
    pub array_name: String,
    /// Maximum byte size for a group before it is flushed.
    pub max_group_size: usize,
    /// Partition-wide super type for arrays (if known).
    pub partition_data_type: arrow::datatypes::DataType,
    /// Offset of the first chunk in this partition.
    pub array_chunk_offset: usize,
    /// Total number of chunks written to the partition.
    pub total_chunks: usize,
    /// Currently accumulating group builder (flushed when large enough).
    pub current_group_writer: ArrayGroupBuilder,
    /// Metadata collected for the partition as arrays/groups are flushed.
    pub partition_metadata: ArrayPartitionMetadata,
}

impl ArrayPartitionWriter {
    /// Return IPC writer options used when creating temporary Arrow writers.
    ///
    /// Currently uses Arrow's default IPC options (no compression requested)
    /// to remain compatible with environments where optional compression
    /// features may not be available.
    pub fn ipc_opts() -> IpcWriteOptions {
        IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap_or_default()
    }

    /// Create a new `ArrayPartitionWriter`.
    ///
    /// `store` is the object store to which finalized partition files will be
    /// uploaded. `array_blob_dir` is the destination path inside the store.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        array_blob_dir: object_store::path::Path,
        array_name: String,
        max_group_size: usize,
        partition_data_type: Option<DataType>,
        array_chunk_offset: usize,
    ) -> BBFResult<Self> {
        let partition_data_type = partition_data_type.unwrap_or(DataType::Null);
        Ok(Self {
            hasher: Hash::new(),
            temp_file: None,
            store,
            dir: array_blob_dir,
            max_group_size,
            partition_metadata: ArrayPartitionMetadata {
                num_elements: 0,
                hash: String::new(),
                data_type: partition_data_type.clone(),
                groups: IndexMap::new(),
            },
            array_name: array_name.clone(),
            partition_data_type: partition_data_type.clone(),
            current_group_writer: ArrayGroupBuilder::new(
                array_name.clone(),
                Some(partition_data_type.clone()),
            ),
            array_chunk_offset,
            total_chunks: 0,
        })
    }

    /// Append an `Option<NdArrowArray>` to the current partition.
    ///
    /// Arrays are accumulated in an internal `ArrayGroupBuilder`. When the
    /// current group's byte size exceeds `max_group_size` the group is
    /// flushed and written to the temporary IPC file.
    pub async fn append_array(&mut self, array: Option<NdArrowArray>) -> BBFResult<()> {
        match array {
            Some(arr) => self.current_group_writer.append_array(arr)?,
            None => self.current_group_writer.append_null_array(),
        };

        if self.current_group_writer.group_size() >= self.max_group_size {
            // Flush the current group
            self.flush_current_group()?;
        }

        Ok(())
    }

    /// Flush the current group builder (if any), write its IPC batch and
    /// update partition metadata.
    fn flush_current_group(&mut self) -> BBFResult<()> {
        // Set the partition data type based on the current group writer
        self.partition_data_type = self.current_group_writer.array_data_type().clone();

        let new_group_writer = ArrayGroupBuilder::new(
            self.array_name.clone(),
            self.partition_data_type.clone().into(),
        );

        // Swap out the current group writer to take ownership.
        let old_group_writer = std::mem::replace(&mut self.current_group_writer, new_group_writer);

        // Flush the old group writer
        let group = old_group_writer.build()?;

        // Flush the group to the temp file
        Self::write_array_group(&mut self.temp_file, &self.array_name, &group)?;

        let group_metadata = group.metadata;
        self.partition_metadata.num_elements += group_metadata.num_elements;
        let chunk_range = self.total_chunks..self.total_chunks + group_metadata.num_chunks;
        self.total_chunks += group_metadata.num_chunks;
        self.partition_metadata
            .groups
            .insert(chunk_range, group_metadata);

        Ok(())
    }

    /// Write a single `ArrayGroup` as a record batch into the provided
    /// temporary `FileWriter`. Initializes the writer if it doesn't exist
    /// yet.
    fn write_array_group(
        current_temp_file: &mut Option<FileWriter<File>>,
        array_name: &str,
        array_group: &ArrayGroup,
    ) -> BBFResult<()> {
        // Check if temp_file is initialized
        let file_writer = match current_temp_file.as_mut() {
            Some(fw) => fw,
            None => {
                let temp_file = tempfile().map_err(|e| {
                    BBFWritingError::TempFileCreationFailure(e, array_name.to_string())
                })?;
                let schema = array_group.batch.schema();
                let file_writer = arrow::ipc::writer::FileWriter::try_new_with_options(
                    temp_file,
                    &schema,
                    Self::ipc_opts(),
                )
                .map_err(BBFWritingError::ArrayGroupWriteFailure)?;
                *current_temp_file = Some(file_writer);
                current_temp_file.as_mut().unwrap()
            }
        };

        // Compare schema's. If different, then map the type for the values list array column to the current array group as that always contains the super type of the two.
        if *file_writer.schema() != array_group.batch.schema() {
            // Iterate through the batches and update the values list array column to the partition type.
            file_writer.finish().unwrap();
            let new_temp_file = tempfile()
                .map_err(|e| BBFWritingError::TempFileCreationFailure(e, array_name.to_string()))?;
            let new_writer = arrow::ipc::writer::FileWriter::try_new_with_options(
                new_temp_file,
                &array_group.batch.schema(),
                Self::ipc_opts(),
            )
            .map_err(BBFWritingError::ArrayGroupWriteFailure)?;
            let input_file = std::mem::replace(file_writer, new_writer)
                .into_inner()
                .map_err(BBFWritingError::ArrayGroupWriteFailure)?;

            let reader = arrow::ipc::reader::FileReader::try_new(input_file, None).unwrap();

            for maybe_batch in reader {
                let batch = maybe_batch.map_err(BBFWritingError::ArrayGroupWriteFailure)?;
                let updated_batch = batch
                    .columns()
                    .iter()
                    .zip(array_group.batch.columns().iter())
                    .map(|(old_col, new_col)| {
                        if old_col.data_type() != new_col.data_type() {
                            // Cast old_col to new_col's data type

                            arrow::compute::cast(old_col, new_col.data_type()).unwrap()
                        } else {
                            old_col.clone()
                        }
                    })
                    .collect::<Vec<_>>();

                let updated_record_batch =
                    RecordBatch::try_new(array_group.batch.schema(), updated_batch)
                        .map_err(BBFWritingError::ArrayGroupWriteFailure)?;

                file_writer
                    .write(&updated_record_batch)
                    .map_err(BBFWritingError::ArrayGroupWriteFailure)?;
            }
        }

        // Write the new batch
        file_writer
            .write(&array_group.batch)
            .map_err(BBFWritingError::ArrayGroupWriteFailure)?;

        Ok(())
    }

    /// Finalize the partition: flush remaining groups, finish the temp
    /// IPC file, compute its hash and upload it to the object store. Returns
    /// the finalized `ArrayPartitionMetadata`.
    pub async fn finish(mut self) -> Result<ArrayPartitionMetadata, BBFError> {
        self.flush_current_group()?;
        // Finalize the temp file (if any)
        match self.temp_file {
            Some(mut fw) => {
                fw.finish().unwrap();
                let file = fw.into_inner().unwrap();
                let tokio_f = tokio::fs::File::from_std(file);

                // Create a hash of the temp file, read the file in chunks of 1MB
                let chunk_size = 1024 * 1024;
                let mut reader = tokio::io::BufReader::with_capacity(1024 * 1024, tokio_f);
                let mut buffer = vec![0; chunk_size];
                loop {
                    let bytes_read = reader.read(&mut buffer).await.unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    self.hasher.update(&buffer[..bytes_read]);
                }
                let hash_result = self.hasher.finalize();
                let hash_string = String::from_utf8_lossy(&hash_result).to_string();
                // Set the hash of the partition in the metadata
                self.partition_metadata.hash = hash_string.clone();

                // Upload the temp file to object store
                let object_path = self.dir.child(format!("{}.arrow", hash_string));

                // Rewind the reader
                reader
                    .rewind()
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                // Create put upload stream
                let mut obj_writer =
                    object_store::buffered::BufWriter::new(self.store, object_path);

                tokio::io::copy_buf(&mut reader, &mut obj_writer)
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                obj_writer
                    .flush()
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                Ok(self.partition_metadata)
            }
            None => {
                // No data was written
                Err(BBFError::Writing(
                    BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(
                        std::io::Error::other("No data written to partition"),
                    )),
                ))
            }
        }
    }
}

mod range_index_map {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct RangeSerde {
        start: usize,
        end: usize,
    }

    impl From<&std::ops::Range<usize>> for RangeSerde {
        fn from(range: &std::ops::Range<usize>) -> Self {
            Self {
                start: range.start,
                end: range.end,
            }
        }
    }

    impl From<RangeSerde> for std::ops::Range<usize> {
        fn from(range: RangeSerde) -> Self {
            range.start..range.end
        }
    }

    pub fn serialize<S, V>(
        map: &IndexMap<std::ops::Range<usize>, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        let items: Vec<(RangeSerde, &V)> = map
            .iter()
            .map(|(range, value)| (RangeSerde::from(range), value))
            .collect();
        items.serialize(serializer)
    }

    pub fn deserialize<'de, D, V>(
        deserializer: D,
    ) -> Result<IndexMap<std::ops::Range<usize>, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let items: Vec<(RangeSerde, V)> = Vec::deserialize(deserializer)?;
        let mut map = IndexMap::with_capacity(items.len());
        for (range, value) in items {
            map.insert(range.into(), value);
        }
        Ok(map)
    }
}

struct IPCDecoder {
    file_decoder: FileDecoder,
    blocks: Vec<Block>,
}
