use std::{convert::TryFrom, fs::File, sync::Arc};

use arrow::{
    array::RecordBatch,
    ipc::{
        Block, CompressionType,
        reader::FileDecoder,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use arrow_ipc::{convert::fb_to_schema, reader::read_footer_length, root_as_footer};
use arrow_schema::{DataType, Schema};
use hmac_sha256::Hash;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tempfile::tempfile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{
    array_group::{ArrayGroup, ArrayGroupBuilder, ArrayGroupMetadata, ArrayGroupReader},
    error::{BBFError, BBFReadingError, BBFResult, BBFWritingError},
    io_cache::{ArrayIoCache, CacheKey},
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
                    object_store::buffered::BufWriter::new(self.store, object_path);

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

struct IPCDecoder {
    file_decoder: FileDecoder,
    blocks: Vec<Block>,
}

pub struct ArrayPartitionReader {
    store: Arc<dyn ObjectStore>,
    decoder: Arc<IPCDecoder>,
    array_name: String,
    partition_path: object_store::path::Path,
    partition_metadata: Arc<ArrayPartitionMetadata>,
    partition_offset: usize,
    partition_hash: String,
    cache: ArrayIoCache,
}

impl ArrayPartitionReader {
    const IPC_TRAILER_LEN_BYTES: usize = 10;

    pub async fn new(
        store: Arc<dyn ObjectStore>,
        array_name: String,
        partition_path: object_store::path::Path,
        partition_metadata: ArrayPartitionMetadata,
        partition_offset: usize,
        cache: ArrayIoCache,
    ) -> BBFResult<Self> {
        let decoder = Arc::new(Self::build_ipc_decoder(&store, &partition_path).await?);

        Ok(Self {
            array_name,
            cache,
            decoder,
            store,
            partition_path,
            partition_offset,
            partition_hash: partition_metadata.hash.clone(),
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
        blob_partition_path: object_store::path::Path,
        array_name: String,
        partition_hash: String,
        group_index: usize,
        decoder: Arc<IPCDecoder>,
        cache: ArrayIoCache,
    ) -> Result<Option<RecordBatch>, BBFError> {
        let cache_key = CacheKey {
            array_name,
            partition_hash,
            group_index,
        };

        let decoder_for_loader = decoder.clone();
        let store_for_loader = store.clone();
        let partition_path_for_loader = blob_partition_path.clone();
        let partition_display = blob_partition_path.to_string();

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

    pub async fn read_array(&self, entry_index: usize) -> BBFResult<Option<NdArrowArray>> {
        let entry_partition_index = match entry_index.checked_sub(self.partition_offset) {
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
                self.array_name.clone(),
                self.partition_hash.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array};
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
            "test_array".to_string(),
            metadata.hash.clone(),
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
        let (metadata, partition_path, decoder) =
            build_partition_for_reader(store.clone(), dir).await;
        let missing_index = decoder.blocks.len();

        let _err = ArrayPartitionReader::fetch_partition_group(
            store,
            partition_path,
            "test_array".to_string(),
            metadata.hash,
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
        let partition_path = dir.child(format!("{}.arrow", metadata.hash));
        let reader = ArrayPartitionReader::new(
            store.clone(),
            "test_array".to_string(),
            partition_path,
            metadata,
            0,
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
        let partition_path = dir.child(format!("{}.arrow", metadata.hash));

        let reader = ArrayPartitionReader::new(
            store,
            "test_array".to_string(),
            partition_path,
            metadata,
            0,
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
