//! Array partition readers and writers built on Arrow IPC.
//!
//! This module defines the primitives responsible for flushing streamed
//! `NdArrowArray`s into compressed Arrow IPC files as well as the matching
//! readers that recover `RecordBatch` data out of object storage.

use std::{convert::TryFrom, fs::File, io::Cursor, sync::Arc};

use arrow::{
    array::RecordBatch,
    ipc::{
        Block, CompressionType,
        reader::FileDecoder,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use arrow_ipc::{
    convert::fb_to_schema,
    reader::{FileReader, read_footer_length},
    root_as_footer,
};
use arrow_schema::{DataType, Schema};
use bytes::Bytes;
use hmac_sha256::Hash;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use tempfile::tempfile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{
    array_group::{ArrayGroup, ArrayGroupBuilder, ArrayGroupMetadata, ArrayGroupReader},
    array_partition_index::build_pruning_index,
    error::{BBFError, BBFReadingError, BBFResult, BBFWritingError},
    io_cache::{ArrayIoCache, CacheKey},
};

/// Summary describing one Arrow IPC partition stored in object storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPartitionMetadata {
    pub num_elements: usize,
    pub partition_offset: usize,
    pub partition_byte_size: usize,
    pub hash: String,
    pub data_type: arrow::datatypes::DataType,
    #[serde(with = "crate::util::range_index_map")]
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
    pub array_partition_offset: usize,
    /// Total number of chunks written to the partition.
    pub total_chunks: usize,
    /// Currently accumulating group builder (flushed when large enough).
    pub current_group_writer: ArrayGroupBuilder,
    /// Metadata collected for the partition as arrays/groups are flushed.
    pub partition_metadata: ArrayPartitionMetadata,
}

impl ArrayPartitionWriter {
    /// Return IPC writer options used when creating temporary Arrow writers.
    pub fn ipc_opts() -> IpcWriteOptions {
        IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap_or_default()
    }

    fn create_temp_file_writer(
        array_name: &str,
        schema: &Schema,
    ) -> Result<FileWriter<File>, BBFWritingError> {
        let make_temp_file = || {
            tempfile()
                .map_err(|e| BBFWritingError::TempFileCreationFailure(e, array_name.to_string()))
        };

        match arrow::ipc::writer::FileWriter::try_new_with_options(
            make_temp_file()?,
            schema,
            Self::ipc_opts(),
        ) {
            Ok(writer) => Ok(writer),
            Err(_) => arrow::ipc::writer::FileWriter::try_new(make_temp_file()?, schema)
                .map_err(BBFWritingError::ArrayGroupWriteFailure),
        }
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
        array_partition_offset: usize,
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
                partition_offset: array_partition_offset,
                partition_byte_size: 0,
            },
            array_name: array_name.clone(),
            partition_data_type: partition_data_type.clone(),
            current_group_writer: ArrayGroupBuilder::new(
                array_name.clone(),
                Some(partition_data_type.clone()),
            ),
            array_partition_offset,
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
        self.partition_metadata.partition_byte_size += group_metadata.uncompressed_array_byte_size;
        let chunk_range = self.total_chunks..self.total_chunks + group_metadata.num_chunks;
        self.total_chunks += group_metadata.num_chunks;
        self.partition_metadata
            .groups
            .insert(chunk_range, group_metadata);
        self.partition_metadata.data_type = self.partition_data_type.clone();

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
                let schema = array_group.batch.schema();
                let file_writer = Self::create_temp_file_writer(array_name, &schema)?;
                *current_temp_file = Some(file_writer);
                current_temp_file.as_mut().unwrap()
            }
        };

        // Compare schema's. If different, then map the type for the values list array column to the current array group as that always contains the super type of the two.
        if *file_writer.schema() != array_group.batch.schema() {
            // Iterate through the batches and update the values list array column to the partition type.
            file_writer.finish().unwrap();
            let schema = array_group.batch.schema();
            let new_writer = Self::create_temp_file_writer(array_name, &schema)?;
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
                let mut file = fw.into_inner().unwrap();

                let pruning_index = build_pruning_index(
                    &self.array_name,
                    &mut file,
                    self.partition_data_type.clone(),
                )?;

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
                let hash_string: String =
                    hash_result.iter().map(|b| format!("{:02x}", b)).collect();
                // Set the hash of the partition in the metadata
                self.partition_metadata.hash = hash_string.clone();
                self.partition_metadata.data_type = self.partition_data_type.clone();

                // Upload the temp file to object store
                let object_path = self.dir.child(format!("{}.arrow", hash_string));

                // Rewind the reader
                reader
                    .rewind()
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                // Create put upload stream
                let mut obj_writer =
                    object_store::buffered::BufWriter::new(self.store.clone(), object_path);

                tokio::io::copy_buf(&mut reader, &mut obj_writer)
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                obj_writer
                    .flush()
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                obj_writer
                    .shutdown()
                    .await
                    .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

                // Write the pruning index if present
                if let Some(pruning_index) = pruning_index {
                    Self::write_pruning_index(
                        self.store.clone(),
                        self.dir.clone(),
                        self.partition_metadata.hash.clone(),
                        pruning_index,
                    )
                    .await?;
                }

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

    async fn write_pruning_index(
        store: Arc<dyn ObjectStore>,
        dir_path: object_store::path::Path,
        partition_hash: String,
        index: RecordBatch,
    ) -> BBFResult<()> {
        let path = dir_path.child(format!("{}.pruning_index.arrow", partition_hash));

        // Encode the pruning batch using the same IPC options as array groups so the
        // reader can rely on consistent metadata/compression settings.
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer =
                FileWriter::try_new_with_options(&mut cursor, &index.schema(), Self::ipc_opts())
                    .map_err(|err| {
                        Self::pruning_index_write_error(&path, "initialise IPC writer", err)
                    })?;

            writer.write(&index).map_err(|err| {
                Self::pruning_index_write_error(&path, "serialize pruning batch", err)
            })?;

            writer.finish().map_err(|err| {
                Self::pruning_index_write_error(&path, "finalize IPC writer", err)
            })?;
        }

        // Upload to object store
        let payload = PutPayload::from_bytes(Bytes::from(cursor.into_inner()));
        store
            .put(&path, payload)
            .await
            .map_err(|err| Self::pruning_index_write_error(&path, "upload pruning index", err))?;

        Ok(())
    }

    fn pruning_index_write_error(
        path: &object_store::path::Path,
        stage: &'static str,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> BBFWritingError {
        let display_path = path.to_string();
        let message = format!("failed to {stage} for pruning index {display_path}: {err}");
        BBFWritingError::ArrayPartitionPruningIndexWriteFailure(Box::new(std::io::Error::other(
            message,
        )))
    }
}

struct IPCDecoder {
    file_decoder: FileDecoder,
    blocks: Vec<Block>,
}

/// Lazily reads batches out of an Arrow IPC partition.
pub struct ArrayPartitionReader {
    store: Arc<dyn ObjectStore>,
    decoder: Arc<IPCDecoder>,
    array_name: String,
    partition_group_path: object_store::path::Path,
    partition_path: object_store::path::Path,
    partition_metadata: Arc<ArrayPartitionMetadata>,
    cache: ArrayIoCache,
}

impl ArrayPartitionReader {
    const IPC_TRAILER_LEN_BYTES: usize = 10;

    pub async fn new(
        store: Arc<dyn ObjectStore>,
        array_name: String,
        partition_group_path: object_store::path::Path,
        partition_metadata: ArrayPartitionMetadata,
        cache: ArrayIoCache,
    ) -> BBFResult<Self> {
        let partition_path =
            partition_group_path.child(format!("{}.arrow", partition_metadata.hash));

        let decoder = Arc::new(Self::build_ipc_decoder(&store, &partition_path).await?);

        Ok(Self {
            array_name,
            cache,
            decoder,
            store,
            partition_group_path,
            partition_path,
            partition_metadata: Arc::new(partition_metadata),
        })
    }

    async fn build_ipc_decoder(
        store: &Arc<dyn ObjectStore>,
        partition_path: &object_store::path::Path,
    ) -> BBFResult<IPCDecoder> {
        let partition_display = partition_path.to_string();
        let trailer_len = Self::IPC_TRAILER_LEN_BYTES as u64;

        let file_head = store.head(partition_path).await.map_err(|source| {
            BBFReadingError::PartitionBytesFetch {
                partition_path: partition_display.clone(),
                source,
            }
        })?;
        let file_size = file_head.size as u64;

        if file_size < trailer_len {
            return Err(BBFReadingError::PartitionTooSmall {
                partition_path: partition_display.clone(),
                required: trailer_len,
                actual: file_size,
            }
            .into());
        }

        let trailer_start = file_size - trailer_len;
        let footer_len_bytes = store
            .get_range(partition_path, trailer_start..file_size)
            .await
            .map_err(|source| BBFReadingError::PartitionBytesFetch {
                partition_path: partition_display.clone(),
                source,
            })?;

        let trailer_array: [u8; Self::IPC_TRAILER_LEN_BYTES] = footer_len_bytes
            .as_ref()
            .try_into()
            .map_err(|_| BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: format!(
                    "expected {} trailer bytes, received {}",
                    Self::IPC_TRAILER_LEN_BYTES,
                    footer_len_bytes.len()
                ),
            })?;

        let footer_len = read_footer_length(trailer_array).map_err(|err| {
            BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: err.to_string(),
            }
        })?;

        let footer_len =
            u64::try_from(footer_len).map_err(|_| BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: "footer length reported as negative".to_string(),
            })?;

        if footer_len == 0 {
            return Err(BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: "footer length reported as zero".to_string(),
            }
            .into());
        }

        let footer_start = trailer_start.checked_sub(footer_len).ok_or_else(|| {
            BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: format!("footer length {footer_len} exceeds available bytes ({file_size})"),
            }
        })?;

        let footer_bytes = store
            .get_range(partition_path, footer_start..trailer_start)
            .await
            .map_err(|source| BBFReadingError::PartitionBytesFetch {
                partition_path: partition_display.clone(),
                source,
            })?;

        let footer = root_as_footer(&footer_bytes).map_err(|err| {
            BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: err.to_string(),
            }
        })?;

        let schema_fb = footer
            .schema()
            .ok_or_else(|| BBFReadingError::PartitionFooterDecode {
                partition_path: partition_display.clone(),
                reason: "missing schema in footer".to_string(),
            })?;

        let schema = fb_to_schema(schema_fb);
        let file_decoder = FileDecoder::new(Arc::new(schema), footer.version());
        let blocks: Vec<Block> = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        Ok(IPCDecoder {
            blocks,
            file_decoder,
        })
    }

    async fn fetch_partition_group(
        store: Arc<dyn ObjectStore>,
        array_partition_path: object_store::path::Path,
        group_index: usize,
        decoder: Arc<IPCDecoder>,
        cache: ArrayIoCache,
    ) -> Result<Option<RecordBatch>, BBFError> {
        let cache_key = CacheKey {
            array_partition_path: array_partition_path.clone(),
            group_index,
        };

        let decoder_for_loader = decoder.clone();
        let store_for_loader = store.clone();
        let partition_path_for_loader = array_partition_path.clone();
        let partition_display = array_partition_path.to_string();

        cache
            .try_get_or_insert_with(cache_key, move |_key| {
                let decoder = decoder_for_loader.clone();
                let store = store_for_loader.clone();
                let partition_path = partition_path_for_loader.clone();
                let partition_display = partition_display.clone();

                async move {
                    let block = decoder
                        .blocks
                        .get(group_index)
                        .copied()
                        .ok_or_else(|| BBFReadingError::PartitionGroupIndexOutOfBounds {
                            partition_path: partition_display.clone(),
                            group_index,
                            total_groups: decoder.blocks.len(),
                        })?;

                    let offset = u64::try_from(block.offset()).map_err(|_| {
                        BBFReadingError::PartitionGroupLengthInvalid {
                            partition_path: partition_display.clone(),
                            group_index,
                            reason: format!("negative block offset: {}", block.offset()),
                        }
                    })?;

                    let metadata_len = u64::try_from(block.metaDataLength()).map_err(|_| {
                        BBFReadingError::PartitionGroupLengthInvalid {
                            partition_path: partition_display.clone(),
                            group_index,
                            reason: format!(
                                "negative metadata length: {}",
                                block.metaDataLength()
                            ),
                        }
                    })?;

                    let body_len = u64::try_from(block.bodyLength()).map_err(|_| {
                        BBFReadingError::PartitionGroupLengthInvalid {
                            partition_path: partition_display.clone(),
                            group_index,
                            reason: format!("negative body length: {}", block.bodyLength()),
                        }
                    })?;

                    let range_end = offset
                        .checked_add(metadata_len)
                        .and_then(|v| v.checked_add(body_len))
                        .ok_or_else(|| BBFReadingError::PartitionGroupLengthInvalid {
                            partition_path: partition_display.clone(),
                            group_index,
                            reason: format!(
                                "block range overflow: offset={offset}, metadata_len={metadata_len}, body_len={body_len}"
                            ),
                        })?;

                    let group_bytes = store
                        .get_range(&partition_path, offset..range_end)
                        .await
                        .map_err(|source| BBFReadingError::PartitionGroupBytesFetch {
                            partition_path: partition_display.clone(),
                            group_index,
                            source,
                        })?;

                    let ipc_buffer = arrow::buffer::Buffer::from(group_bytes);

                    let batch = decoder
                        .file_decoder
                        .read_record_batch(&block, &ipc_buffer)
                        .map_err(|err| BBFReadingError::PartitionGroupDecode {
                            partition_path: partition_display.clone(),
                            group_index,
                            reason: err.to_string(),
                        })?;

                    Ok::<Option<RecordBatch>, BBFError>(batch)
                }
            })
            .await
    }

    pub async fn read_partition_pruning_index(&self) -> BBFResult<Option<RecordBatch>> {
        let pruning_index_path = self.partition_group_path.child(format!(
            "{}.pruning_index.arrow",
            self.partition_metadata.hash
        ));

        let bytes = match self.store.get(&pruning_index_path).await {
            Ok(obj) => obj
                .bytes()
                .await
                .map_err(|e| BBFReadingError::PartitionBytesFetch {
                    partition_path: pruning_index_path.to_string(),
                    source: e,
                })?,
            Err(object_store::Error::NotFound { .. }) => {
                // No pruning index present
                return Ok(None);
            }
            Err(e) => {
                return Err(BBFReadingError::PartitionBytesFetch {
                    partition_path: pruning_index_path.to_string(),
                    source: e,
                }
                .into());
            }
        };

        let io_buf = Cursor::new(bytes);

        let reader = FileReader::try_new(io_buf, None).map_err(|e| {
            BBFReadingError::PartitionPruningIndexDecode {
                partition_path: pruning_index_path.to_string(),
                reason: e.to_string(),
            }
        })?;

        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
            BBFReadingError::PartitionPruningIndexDecode {
                partition_path: pruning_index_path.to_string(),
                reason: e.to_string(),
            }
        })?;

        // Concatenate all batches into one
        if batches.is_empty() {
            Ok(None)
        } else if batches.len() == 1 {
            Ok(Some(batches.into_iter().next().unwrap()))
        } else {
            let schema = batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                BBFReadingError::PartitionPruningIndexDecode {
                    partition_path: pruning_index_path.to_string(),
                    reason: e.to_string(),
                }
            })?;
            Ok(Some(concatenated))
        }
    }

    pub async fn read_array(&self, entry_index: usize) -> BBFResult<Option<NdArrowArray>> {
        let entry_partition_index =
            match entry_index.checked_sub(self.partition_metadata.partition_offset) {
                Some(entry_index) => entry_index,
                None => return Ok(None),
            };
        let group_match = self
            .partition_metadata
            .groups
            .iter()
            .enumerate()
            .find(|(_, (range, _))| range.contains(&entry_partition_index));

        if let Some((group_index, (range, _))) = group_match {
            let read_req = Self::fetch_partition_group(
                self.store.clone(),
                self.partition_path.clone(),
                group_index,
                self.decoder.clone(),
                self.cache.clone(),
            )
            .await?;

            if let Some(batch) = read_req {
                let array_group_reader = ArrayGroupReader::new(self.array_name.clone(), batch);
                let group_array_entry_index = entry_partition_index - range.start;
                // Fetch the row within the batch
                return array_group_reader.try_get_array(group_array_entry_index);
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, UInt64Array};
    use nd_arrow_array::dimensions::{Dimension, Dimensions};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc as StdArc;

    fn scalar_int32(values: &[i32]) -> NdArrowArray {
        let array: ArrayRef = StdArc::new(Int32Array::from(values.to_vec()));
        let dimension = Dimension {
            name: "dim0".to_string(),
            size: values.len(),
        };
        NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
            .expect("nd array creation")
    }

    fn scalar_int32_nullable(values: &[Option<i32>]) -> NdArrowArray {
        let array: ArrayRef = StdArc::new(Int32Array::from(values.to_vec()));
        let dimension = Dimension {
            name: "dim0".to_string(),
            size: values.len(),
        };
        NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
            .expect("nd array creation")
    }

    async fn build_writer(
        store: StdArc<dyn ObjectStore>,
        dir: Path,
        max_group_size: usize,
    ) -> ArrayPartitionWriter {
        ArrayPartitionWriter::new(
            store,
            dir,
            "test_array".to_string(),
            max_group_size,
            None,
            0,
        )
        .await
        .expect("writer init")
    }

    #[tokio::test]
    async fn finish_writes_partition_and_uploads() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/arrays");
        let mut writer = build_writer(store.clone(), dir.clone(), usize::MAX).await;

        writer
            .append_array(Some(scalar_int32(&[1, 2])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32(&[3])))
            .await
            .expect("append second");
        writer
            .append_array(Some(scalar_int32(&[4, 5])))
            .await
            .expect("append third");

        let metadata = writer.finish().await.expect("finish success");

        assert_eq!(metadata.num_elements, 5);
        assert_eq!(metadata.data_type, DataType::Int32);
        assert_eq!(metadata.groups.len(), 1);
        let (range, group_metadata) = metadata.groups.iter().next().unwrap();
        assert_eq!(range.clone(), 0..3);
        assert_eq!(group_metadata.num_chunks, 3);
        assert!(group_metadata.uncompressed_array_byte_size > 0);
        assert!(!metadata.hash.is_empty());

        let object_path = dir.child(format!("{}.arrow", metadata.hash));
        let stored_bytes = store
            .get(&object_path)
            .await
            .expect("object exists")
            .bytes()
            .await
            .expect("object bytes");
        assert!(!stored_bytes.is_empty());
    }

    #[tokio::test]
    async fn finish_tracks_null_chunks() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/nulls");
        let mut writer = build_writer(store, dir, usize::MAX).await;

        writer
            .append_array(Some(scalar_int32(&[1, 2])))
            .await
            .expect("append first");
        writer.append_array(None).await.expect("append null");
        writer
            .append_array(Some(scalar_int32(&[3, 4, 5])))
            .await
            .expect("append last");

        let metadata = writer.finish().await.expect("finish success");

        assert_eq!(metadata.num_elements, 5);
        assert_eq!(metadata.groups.len(), 1);
        let (range, group_metadata) = metadata.groups.iter().next().unwrap();
        assert_eq!(range.clone(), 0..3);
        assert_eq!(group_metadata.num_chunks, 3);
    }

    #[tokio::test]
    async fn finish_records_multiple_groups_when_limit_hit() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/multi_group");
        let mut writer = build_writer(store, dir, 1).await;

        writer
            .append_array(Some(scalar_int32(&[1, 2, 3])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32(&[4, 5, 6])))
            .await
            .expect("append second");
        writer
            .append_array(Some(scalar_int32(&[])))
            .await
            .expect("append zero length");

        let metadata = writer.finish().await.expect("finish success");

        assert_eq!(metadata.num_elements, 6);
        assert_eq!(metadata.groups.len(), 3);
        let ranges: Vec<_> = metadata.groups.keys().cloned().collect();
        assert_eq!(ranges, vec![0..1, 1..2, 2..3]);
        for group in metadata.groups.values() {
            assert_eq!(group.num_chunks, 1);
        }
    }

    async fn build_partition_for_reader(
        store: StdArc<dyn ObjectStore>,
        dir: Path,
    ) -> (
        ArrayPartitionMetadata,
        object_store::path::Path,
        Arc<IPCDecoder>,
    ) {
        let mut writer = build_writer(store.clone(), dir.clone(), usize::MAX).await;
        writer
            .append_array(Some(scalar_int32(&[1, 2])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32(&[3, 4, 5])))
            .await
            .expect("append second");

        let metadata = writer.finish().await.expect("finish success");
        let partition_path = dir.child(format!("{}.arrow", metadata.hash));
        let decoder = ArrayPartitionReader::build_ipc_decoder(&store, &partition_path)
            .await
            .expect("decoder build");

        (metadata, partition_path, Arc::new(decoder))
    }

    #[tokio::test]
    async fn fetch_partition_group_reads_expected_batch() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/fetch_success");
        let (metadata, partition_path, decoder) =
            build_partition_for_reader(store.clone(), dir).await;

        let batch = ArrayPartitionReader::fetch_partition_group(
            store.clone(),
            partition_path.clone(),
            0,
            decoder,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("fetch succeeds")
        .expect("batch present");

        assert_eq!(batch.num_columns(), 3);
        let expected_chunks = metadata
            .groups
            .values()
            .next()
            .expect("group metadata")
            .num_chunks;
        assert_eq!(batch.num_rows(), expected_chunks);
        let values_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("list column");
        assert_eq!(values_column.len(), expected_chunks);
    }

    #[tokio::test]
    async fn fetch_partition_group_errors_when_index_is_out_of_bounds() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/fetch_oob");
        let (_metadata, partition_path, decoder) =
            build_partition_for_reader(store.clone(), dir).await;
        let missing_index = decoder.blocks.len();

        let _err = ArrayPartitionReader::fetch_partition_group(
            store,
            partition_path,
            missing_index,
            decoder,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect_err("fetch should fail");
    }

    #[tokio::test]
    async fn read_array_returns_expected_chunk() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/read_array_single_group");
        let mut writer = build_writer(store.clone(), dir.clone(), usize::MAX).await;

        writer
            .append_array(Some(scalar_int32(&[10, 20])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32(&[30, 40, 50])))
            .await
            .expect("append second");

        let metadata = writer.finish().await.expect("finish success");
        let reader = ArrayPartitionReader::new(
            store.clone(),
            "test_array".to_string(),
            dir,
            metadata,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("reader init");

        let maybe_array = reader.read_array(1).await.expect("read succeeds");
        let nd_array = maybe_array.expect("array exists");
        let arrow_array = nd_array.as_arrow_array();
        let int_array = arrow_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        let actual: Vec<_> = (0..int_array.len())
            .map(|idx| int_array.value(idx))
            .collect();
        assert_eq!(actual, vec![30, 40, 50]);
    }

    #[tokio::test]
    async fn read_array_handles_group_offsets() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/read_array_group_offsets");

        let first = scalar_int32(&[1, 2, 3]);
        let second = scalar_int32(&[4, 5]);
        let third = scalar_int32(&[6]);

        let first_group_size = first.as_arrow_array().get_buffer_memory_size()
            + second.as_arrow_array().get_buffer_memory_size();

        let mut writer = build_writer(store.clone(), dir.clone(), first_group_size).await;

        writer
            .append_array(Some(first))
            .await
            .expect("append first chunk");
        writer
            .append_array(Some(second))
            .await
            .expect("append second chunk");
        writer
            .append_array(Some(third))
            .await
            .expect("append third chunk");

        let metadata = writer.finish().await.expect("finish success");
        assert_eq!(metadata.groups.len(), 2, "expected two groups recorded");

        let reader = ArrayPartitionReader::new(
            store,
            "test_array".to_string(),
            dir,
            metadata,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("reader init");

        let maybe_array = reader.read_array(2).await.expect("read succeeds");
        let nd_array = maybe_array.expect("array exists");
        let arrow_array = nd_array.as_arrow_array();
        let int_array = arrow_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(int_array.len(), 1);
        assert_eq!(int_array.value(0), 6);
    }

    #[tokio::test]
    async fn finish_writes_pruning_index_and_reader_loads_it() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/pruning_index");
        let mut writer = build_writer(store.clone(), dir.clone(), usize::MAX).await;

        writer
            .append_array(Some(scalar_int32(&[11, 7, 9])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32(&[42, 41])))
            .await
            .expect("append second");

        let metadata = writer.finish().await.expect("finish success");
        let reader = ArrayPartitionReader::new(
            store.clone(),
            "test_array".to_string(),
            dir,
            metadata,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("reader init");

        let pruning_batch = reader
            .read_partition_pruning_index()
            .await
            .expect("read index")
            .expect("pruning batch present");

        assert_eq!(pruning_batch.num_columns(), 4);
        assert_eq!(pruning_batch.num_rows(), 2);

        let mins = pruning_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 mins");
        let maxes = pruning_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 maxes");
        let null_counts = pruning_batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("null counts");
        let row_counts = pruning_batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row counts");

        assert_eq!(mins.value(0), 7);
        assert_eq!(maxes.value(0), 11);
        assert_eq!(row_counts.value(0), 3);
        assert_eq!(null_counts.value(0), 0);

        assert_eq!(mins.value(1), 41);
        assert_eq!(maxes.value(1), 42);
        assert_eq!(row_counts.value(1), 2);
        assert_eq!(null_counts.value(1), 0);
    }

    #[tokio::test]
    async fn pruning_index_tracks_null_counts() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/pruning_index_nulls");
        let mut writer = build_writer(store.clone(), dir.clone(), usize::MAX).await;

        writer
            .append_array(Some(scalar_int32_nullable(&[
                Some(5),
                None,
                Some(1),
                Some(3),
            ])))
            .await
            .expect("append first");
        writer
            .append_array(Some(scalar_int32_nullable(&[None, None])))
            .await
            .expect("append second");

        let metadata = writer.finish().await.expect("finish success");
        let reader = ArrayPartitionReader::new(
            store.clone(),
            "test_array".to_string(),
            dir,
            metadata,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("reader init");

        let pruning_batch = reader
            .read_partition_pruning_index()
            .await
            .expect("read index")
            .expect("pruning batch present");

        assert_eq!(pruning_batch.num_rows(), 2);
        let mins = pruning_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("mins");
        let maxes = pruning_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("maxes");
        let null_counts = pruning_batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("null counts");
        let row_counts = pruning_batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row counts");

        assert_eq!(mins.value(0), 1);
        assert_eq!(maxes.value(0), 5);
        assert_eq!(row_counts.value(0), 4);
        assert_eq!(null_counts.value(0), 1);

        assert!(mins.is_null(1));
        assert!(maxes.is_null(1));
        assert_eq!(row_counts.value(1), 2);
        assert_eq!(null_counts.value(1), 2);
    }

    #[tokio::test]
    async fn pruning_index_covers_multiple_groups() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let dir = Path::from("tests/pruning_index_multi_group");
        let chunk1_vals: Vec<i32> = (0..50).collect();
        let chunk2_vals: Vec<i32> = (100..160).collect();
        let chunk3_vals = vec![200];
        let chunk4_vals = vec![300, 301];

        let chunk1 = scalar_int32(&chunk1_vals);
        let chunk2 = scalar_int32(&chunk2_vals);
        let chunk3 = scalar_int32(&chunk3_vals);
        let chunk4 = scalar_int32(&chunk4_vals);

        let chunk3_size = chunk3.as_arrow_array().get_buffer_memory_size();
        let chunk4_size = chunk4.as_arrow_array().get_buffer_memory_size();
        let max_group_size = chunk3_size + chunk4_size + 1; // large enough for last group, small enough for earlier ones
        assert!(chunk1.as_arrow_array().get_buffer_memory_size() > max_group_size);
        assert!(chunk2.as_arrow_array().get_buffer_memory_size() > max_group_size);

        let mut writer = build_writer(store.clone(), dir.clone(), max_group_size).await;

        writer
            .append_array(Some(chunk1))
            .await
            .expect("append first");
        writer
            .append_array(Some(chunk2))
            .await
            .expect("append second");
        writer
            .append_array(Some(chunk3))
            .await
            .expect("append third");
        writer
            .append_array(Some(chunk4))
            .await
            .expect("append fourth");

        let metadata = writer.finish().await.expect("finish success");
        assert!(
            metadata.groups.len() >= 2,
            "expected at least two flushed groups"
        );

        let reader = ArrayPartitionReader::new(
            store.clone(),
            "test_array".to_string(),
            dir,
            metadata,
            ArrayIoCache::new(1024 * 1024),
        )
        .await
        .expect("reader init");

        let pruning_batch = reader
            .read_partition_pruning_index()
            .await
            .expect("read index")
            .expect("pruning batch present");

        assert_eq!(pruning_batch.num_rows(), 4);
        let mins = pruning_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("mins");
        let maxes = pruning_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("maxes");
        let row_counts = pruning_batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row counts");

        let expected = vec![(0, 49, 50), (100, 159, 60), (200, 200, 1), (300, 301, 2)];
        for (idx, (min, max, rows)) in expected.into_iter().enumerate() {
            assert_eq!(mins.value(idx), min);
            assert_eq!(maxes.value(idx), max);
            assert_eq!(row_counts.value(idx), rows);
        }
    }

    #[test]
    fn range_index_map_round_trip() {
        let mut groups = IndexMap::new();
        groups.insert(
            0..2,
            ArrayGroupMetadata {
                uncompressed_array_byte_size: 10,
                num_chunks: 2,
                num_elements: 4,
            },
        );
        groups.insert(
            2..5,
            ArrayGroupMetadata {
                uncompressed_array_byte_size: 20,
                num_chunks: 3,
                num_elements: 6,
            },
        );

        let metadata = ArrayPartitionMetadata {
            num_elements: 10,
            hash: "abc123".to_string(),
            data_type: DataType::Int32,
            groups,
            partition_offset: 0,
            partition_byte_size: 30,
        };

        let serialized = serde_json::to_string(&metadata).expect("serialize");
        let restored: ArrayPartitionMetadata =
            serde_json::from_str(&serialized).expect("deserialize");

        assert_eq!(restored.num_elements, metadata.num_elements);
        assert_eq!(restored.hash, metadata.hash);
        let restored_ranges: Vec<_> = restored.groups.keys().cloned().collect();
        assert_eq!(restored_ranges, vec![0..2, 2..5]);
    }
}
