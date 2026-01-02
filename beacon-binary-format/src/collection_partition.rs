//! Collection partition reader and writer for reconstructing logical entries
//! from per-array partitions.
//!
//! The binary format stores each logical entry as a set of independent array
//! partitions so writers can flush and upload data incrementally. This module
//! provides the glue required to go from the decomposed representation back to
//! cohesive Arrow record batches. It also exposes the inverse operation so new
//! entries can be fanned out into array partitions while tracking metadata
//! such as byte sizes and element counts.

use std::io::SeekFrom;
use std::pin;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::LargeStringArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::datatypes::Schema;
use arrow_ipc::reader::FileReader;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use nd_arrow_array::batch::NdRecordBatch;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::OnceCell;

use crate::array_partition::ArrayPartitionReader;
use crate::entry_mask::{decode_deleted_mask, encode_deleted_mask_to_tempfile};
use crate::error::BBFError;
use crate::error::BBFReadingError;
use crate::error::BBFWritingError;
use crate::io_cache;
use crate::layout::{
    PARTITION_BLOB_FILE, PARTITION_ENTRY_MASK_FILE, PARTITION_PRUNING_INDEX_FILE,
    PARTITION_RESOLUTION_FILE, PARTITIONS_DIR,
};
use crate::partition_resolution::{PartitionResolution, ResolvedSlice};
use crate::stream::AsyncStreamScheduler;
use crate::{
    array_partition::{ArrayPartitionMetadata, ArrayPartitionWriter},
    error::BBFResult,
};

/// Metadata describing a collection partition and its constituent arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPartitionMetadata {
    /// Name of the partition (typically derived from its directory).
    pub partition_name: String,
    /// Total uncompressed byte size across all child arrays.
    pub byte_size: usize,
    /// Number of flattened elements contained in all arrays.
    pub num_elements: usize,
    /// Number of logical entries present in the partition.
    pub num_entries: usize,
    /// Arrow schema describing every array (including `__entry_key`).
    pub partition_schema: Arc<arrow::datatypes::Schema>,
    /// Mapping from array name to its partition metadata.
    pub arrays: IndexMap<String, ArrayPartitionMetadata>,
    /// Optional content hash for an entry deletion mask stored in `entry_mask.bbem`.
    /// When present, readers should skip entries where the mask marks them as deleted.
    #[serde(default)]
    pub entry_mask_hash: Option<String>,
}

/// Reader that stitches multiple array partitions back into logical entries.
pub struct CollectionPartitionReader {
    /// Per-partition metadata loaded from disk/object store.
    pub metadata: CollectionPartitionMetadata,
    /// Root path where array partitions and metadata live.
    pub path: object_store::path::Path,
    /// Object store used to fetch partition bytes.
    pub object_store: Arc<dyn ObjectStore>,
    /// Cache shared with array partition readers to avoid duplicate fetches.
    pub io_cache: io_cache::ArrayIoCache,
    resolution: OnceCell<Arc<PartitionResolution>>,
    entry_mask: OnceCell<Option<Arc<Vec<bool>>>>,
}

/// Tunable options that control how collection partitions are read.
///
/// These options primarily influence concurrency when the reader builds a
/// future per entry. Keeping the value modest helps avoid overwhelming the
/// backing object store.
pub struct CollectionPartitionReadOptions {
    /// Maximum number of concurrent entry read tasks.
    pub max_concurrent_reads: usize,
    /// Optional selection mask to enable reading a subset of entries.
    ///
    /// When provided, `entry_selection.len()` must equal `metadata.num_entries`.
    /// Entries are read when `entry_selection[i] == true`.
    pub entry_selection: Option<Vec<bool>>,
}

impl CollectionPartitionReader {
    /// Create a new reader over the provided metadata and backing object store.
    ///
    /// The caller is responsible for ensuring `metadata.partition_schema`
    /// contains every field that may be requested via projections.
    pub fn new(
        path: object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
        metadata: CollectionPartitionMetadata,
        io_cache: io_cache::ArrayIoCache,
    ) -> Self {
        Self {
            metadata,
            path,
            object_store,
            io_cache,
            resolution: OnceCell::new(),
            entry_mask: OnceCell::new(),
        }
    }

    /// Read partition entries, optionally projecting a subset of arrays.
    ///
    /// The returned scheduler multiplexes a future per entry, allowing callers
    /// to bound parallelism via `options.max_concurrent_reads` when fetching
    /// data from the object store.
    pub async fn read(
        &self,
        projection: Option<Arc<[String]>>,
        options: CollectionPartitionReadOptions,
    ) -> BBFResult<AsyncStreamScheduler<BBFResult<NdRecordBatch>>> {
        let (shared_readers, projected_schema) = self.prepare_read(projection).await?;
        let entry_indices = self
            .selected_entry_indices(options.entry_selection.as_ref())
            .await?;
        let mut futures = Vec::new();
        for index in entry_indices {
            let shared_readers = shared_readers.clone();
            let projected_schema = projected_schema.clone();
            let read_fut = async move {
                let mut read_tasks = Vec::new();
                for (_, array_reader) in shared_readers.as_ref() {
                    let array_read_task = array_reader.read_array(index);
                    read_tasks.push(array_read_task);
                }

                let array_results = futures::future::join_all(read_tasks).await;
                let mut fields = Vec::new();
                let mut arrays = Vec::new();

                for (i, array_result) in array_results.into_iter().enumerate() {
                    let field = projected_schema.field(i).clone();
                    let array = array_result?.unwrap_or(NdArrowArray::new_null_scalar(Some(
                        field.data_type().clone(),
                    )));
                    fields.push(field);
                    arrays.push(array);
                }

                let nd_batch = NdRecordBatch::new(fields, arrays).map_err(|e| {
                    BBFError::Reading(BBFReadingError::ArrayGroupReadFailure(
                        format!("entry index {}", index),
                        Box::new(e),
                    ))
                })?;
                Ok::<_, BBFError>(nd_batch)
            };
            futures.push(read_fut);
        }

        let scheduler = AsyncStreamScheduler::new(futures, options.max_concurrent_reads);
        Ok(scheduler)
    }

    /// Read partition entries while retaining the logical entry index
    /// associated with every batch.
    pub async fn read_indexed(
        &self,
        projection: Option<Arc<[String]>>,
        options: CollectionPartitionReadOptions,
    ) -> BBFResult<AsyncStreamScheduler<BBFResult<(usize, NdRecordBatch)>>> {
        let (shared_readers, projected_schema) = self.prepare_read(projection).await?;
        let entry_indices = self
            .selected_entry_indices(options.entry_selection.as_ref())
            .await?;
        let mut futures = Vec::new();
        for index in entry_indices {
            let shared_readers = shared_readers.clone();
            let projected_schema = projected_schema.clone();
            let read_fut = async move {
                let mut read_tasks = Vec::new();
                for (_, array_reader) in shared_readers.as_ref() {
                    let array_read_task = array_reader.read_array(index);
                    read_tasks.push(array_read_task);
                }

                let array_results = futures::future::join_all(read_tasks).await;
                let mut fields = Vec::new();
                let mut arrays = Vec::new();

                for (i, array_result) in array_results.into_iter().enumerate() {
                    let field = projected_schema.field(i).clone();
                    let array = array_result?.unwrap_or(NdArrowArray::new_null_scalar(Some(
                        field.data_type().clone(),
                    )));
                    fields.push(field);
                    arrays.push(array);
                }

                let nd_batch = NdRecordBatch::new(fields, arrays).map_err(|e| {
                    BBFError::Reading(BBFReadingError::ArrayGroupReadFailure(
                        format!("entry index {}", index),
                        Box::new(e),
                    ))
                })?;
                Ok::<_, BBFError>((index, nd_batch))
            };
            futures.push(read_fut);
        }

        let scheduler = AsyncStreamScheduler::new(futures, options.max_concurrent_reads);
        Ok(scheduler)
    }

    /// Fetch the pruning index for the requested projection.
    ///
    /// Returns a single Arrow [`RecordBatch`] whose columns are the concatenation of
    /// each projected array's pruning index columns:
    /// `"{name}:min"`, `"{name}:max"`, `"{name}:null_count"`, `"{name}:row_count"`.
    ///
    /// If none of the projected arrays have a pruning index, returns `Ok(None)`.
    pub async fn read_pruning_index(
        &self,
        projection: Option<Arc<[String]>>,
    ) -> BBFResult<Option<RecordBatch>> {
        let (shared_readers, _projected_schema) = self.prepare_read(projection).await?;

        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut expected_rows: Option<usize> = None;

        for (name, reader) in shared_readers.as_ref() {
            // Pruning indices are not meaningful for the synthetic entry key.
            if name.as_str() == "__entry_key" {
                continue;
            }

            let Some(batch) = reader.read_partition_pruning_index().await? else {
                continue;
            };

            if let Some(expected) = expected_rows {
                if batch.num_rows() != expected {
                    return Err(BBFReadingError::PartitionPruningIndexDecode {
                        partition_path: self.path.child(PARTITION_PRUNING_INDEX_FILE).to_string(),
                        reason: format!(
                            "pruning index row count mismatch for '{name}': expected {expected}, got {}",
                            batch.num_rows()
                        ),
                    }
                    .into());
                }
            } else {
                expected_rows = Some(batch.num_rows());
            }

            fields.extend(batch.schema().fields().iter().cloned());
            columns.extend(batch.columns().iter().cloned());
        }

        if columns.is_empty() {
            return Ok(None);
        }

        let schema = Arc::new(Schema::new(fields));
        let combined = RecordBatch::try_new(schema, columns).map_err(|e| {
            BBFError::Reading(BBFReadingError::PartitionPruningIndexDecode {
                partition_path: self.path.child(PARTITION_PRUNING_INDEX_FILE).to_string(),
                reason: e.to_string(),
            })
        })?;

        Ok(Some(combined))
    }

    async fn prepare_read(
        &self,
        projection: Option<Arc<[String]>>,
    ) -> BBFResult<(
        Arc<IndexMap<String, ArrayPartitionReader>>,
        Arc<arrow::datatypes::Schema>,
    )> {
        let resolution = self
            .resolution
            .get_or_try_init(|| async {
                let meta_path = self.path.child(PARTITION_RESOLUTION_FILE);
                let meta_display = meta_path.to_string();
                let meta_object = self.object_store.get(&meta_path).await.map_err(|source| {
                    BBFReadingError::PartitionResolutionFetch {
                        meta_path: meta_display.clone(),
                        source,
                    }
                })?;
                let bytes = meta_object.bytes().await.map_err(|source| {
                    BBFReadingError::PartitionResolutionFetch {
                        meta_path: meta_display.clone(),
                        source,
                    }
                })?;
                let resolution: PartitionResolution =
                    serde_json::from_slice(&bytes).map_err(|e| {
                        BBFReadingError::PartitionResolutionDecode {
                            meta_path: meta_display,
                            reason: e.to_string(),
                        }
                    })?;
                Ok::<_, BBFError>(Arc::new(resolution))
            })
            .await?;

        let arrays_to_read = match projection {
            Some(proj) => proj
                .iter()
                .filter_map(|name| {
                    self.metadata
                        .arrays
                        .get(name)
                        .map(|meta| (name.clone(), meta.clone()))
                })
                .collect::<IndexMap<String, ArrayPartitionMetadata>>(),
            None => self.metadata.arrays.clone(),
        };

        let blob_path = self.path.child(PARTITION_BLOB_FILE);
        let pruning_blob_path = self.path.child(PARTITION_PRUNING_INDEX_FILE);

        let mut array_readers = IndexMap::new();
        for (array_name, array_metadata) in &arrays_to_read {
            let slice = resolution
                .objects
                .get(&array_metadata.hash)
                .copied()
                .ok_or_else(|| BBFReadingError::PartitionResolutionMissing {
                    meta_path: self.path.child(PARTITION_RESOLUTION_FILE).to_string(),
                    hash: array_metadata.hash.clone(),
                })?;

            let pruning_index = match array_metadata.pruning_index_hash.as_ref() {
                Some(index_hash) => {
                    let index_slice =
                        resolution.objects.get(index_hash).copied().ok_or_else(|| {
                            BBFReadingError::PartitionResolutionMissing {
                                meta_path: self.path.child(PARTITION_RESOLUTION_FILE).to_string(),
                                hash: index_hash.clone(),
                            }
                        })?;
                    Some((pruning_blob_path.clone(), index_slice))
                }
                None => None,
            };

            let array_partition_reader = ArrayPartitionReader::new(
                self.object_store.clone(),
                array_name.clone(),
                blob_path.clone(),
                slice,
                pruning_index,
                array_metadata.clone(),
                self.io_cache.clone(),
            )
            .await?;
            array_readers.insert(array_name.clone(), array_partition_reader);
        }
        let shared_readers = Arc::new(array_readers);

        let mut projected_fields = Vec::new();
        for (array_name, _) in &arrays_to_read {
            let field = self
                .metadata
                .partition_schema
                .field_with_name(array_name)
                .expect("field exists")
                .clone();
            projected_fields.push(field);
        }
        let projected_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));
        Ok((shared_readers, projected_schema))
    }

    async fn active_entry_indices(&self) -> BBFResult<Vec<usize>> {
        let mask = self
            .entry_mask
            .get_or_try_init(|| async {
                let Some(mask_hash) = self.metadata.entry_mask_hash.as_ref() else {
                    return Ok::<_, BBFError>(None);
                };

                let resolution = self
                    .resolution
                    .get_or_try_init(|| async {
                        // Ensure resolution is loaded.
                        let meta_path = self.path.child(PARTITION_RESOLUTION_FILE);
                        let meta_display = meta_path.to_string();
                        let meta_object =
                            self.object_store.get(&meta_path).await.map_err(|source| {
                                BBFReadingError::PartitionResolutionFetch {
                                    meta_path: meta_display.clone(),
                                    source,
                                }
                            })?;
                        let bytes = meta_object.bytes().await.map_err(|source| {
                            BBFReadingError::PartitionResolutionFetch {
                                meta_path: meta_display.clone(),
                                source,
                            }
                        })?;
                        let resolution: PartitionResolution = serde_json::from_slice(&bytes)
                            .map_err(|e| BBFReadingError::PartitionResolutionDecode {
                                meta_path: meta_display,
                                reason: e.to_string(),
                            })?;
                        Ok::<_, BBFError>(Arc::new(resolution))
                    })
                    .await?;

                let slice = resolution.objects.get(mask_hash).copied().ok_or_else(|| {
                    BBFReadingError::PartitionResolutionMissing {
                        meta_path: self.path.child(PARTITION_RESOLUTION_FILE).to_string(),
                        hash: mask_hash.clone(),
                    }
                })?;

                let mask_path = self.path.child(PARTITION_ENTRY_MASK_FILE);
                let bytes = self
                    .object_store
                    .get_range(&mask_path, slice.offset..slice.offset + slice.size)
                    .await
                    .map_err(|source| BBFReadingError::PartitionBytesFetch {
                        partition_path: mask_path.to_string(),
                        source,
                    })?;

                let reader =
                    FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(|e| {
                        BBFReadingError::EntryMaskDecode {
                            reason: e.to_string(),
                        }
                    })?;
                let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
                    BBFReadingError::EntryMaskDecode {
                        reason: e.to_string(),
                    }
                })?;

                if batches.is_empty() {
                    return Ok::<_, BBFError>(Some(Arc::new(Vec::new())));
                }
                let batch = if batches.len() == 1 {
                    batches.into_iter().next().expect("batch")
                } else {
                    let schema = batches[0].schema();
                    arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                        BBFReadingError::EntryMaskDecode {
                            reason: e.to_string(),
                        }
                    })?
                };

                let deleted = decode_deleted_mask(&batch)?;
                Ok::<_, BBFError>(Some(Arc::new(deleted)))
            })
            .await?;

        let indices: Vec<usize> = match mask {
            None => (0..self.metadata.num_entries).collect(),
            Some(deleted) => {
                if deleted.len() != self.metadata.num_entries {
                    return Err(BBFReadingError::EntryMaskLengthMismatch {
                        expected: self.metadata.num_entries,
                        actual: deleted.len(),
                    }
                    .into());
                }
                deleted
                    .iter()
                    .enumerate()
                    .filter_map(|(i, is_deleted)| (!*is_deleted).then_some(i))
                    .collect()
            }
        };

        Ok(indices)
    }

    async fn selected_entry_indices(&self, selection: Option<&Vec<bool>>) -> BBFResult<Vec<usize>> {
        if let Some(selection) = selection {
            if selection.len() != self.metadata.num_entries {
                return Err(BBFReadingError::EntrySelectionLengthMismatch {
                    expected: self.metadata.num_entries,
                    actual: selection.len(),
                }
                .into());
            }
        }

        let active = self.active_entry_indices().await?;
        let Some(selection) = selection else {
            return Ok(active);
        };

        Ok(active
            .into_iter()
            .filter(|&idx| *selection.get(idx).unwrap_or(&false))
            .collect())
    }
}

/// Writer that splits incoming entry streams into per-array partitions.
pub struct CollectionPartitionWriter {
    /// Metadata being accumulated while writing entries.
    pub metadata: CollectionPartitionMetadata,
    /// Destination path under which array partitions will be stored.
    pub path: object_store::path::Path,
    /// Object store that will persist partition data.
    pub object_store: Arc<dyn ObjectStore>,
    /// Active per-array partition writers keyed by array name.
    pub array_writers: IndexMap<String, ArrayPartitionWriter>,
    /// Global writer configuration shared by all arrays.
    pub write_options: WriterOptions,

    entry_mask: Option<Vec<bool>>,
}

/// Options controlling the size/shape of generated array partitions.
///
/// Larger group sizes incur more memory usage but reduce the number of objects
/// written to the object store.
pub struct WriterOptions {
    /// Maximum buffer size (in bytes) before array groups are flushed.
    pub max_group_size: usize,
}

impl CollectionPartitionWriter {
    /// Create a writer that will materialize array partitions under `path`.
    ///
    /// Writers stay entirely in-memory until the underlying
    /// `ArrayPartitionWriter`s flush, so callers should size
    /// `WriterOptions::max_group_size` accordingly.
    pub fn new(
        collection_root: object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
        partition_name: String,
        options: WriterOptions,
    ) -> Self {
        let metadata = CollectionPartitionMetadata {
            partition_name: partition_name.clone(),
            byte_size: 0,
            num_elements: 0,
            partition_schema: Arc::new(arrow::datatypes::Schema::empty()),
            arrays: IndexMap::new(),
            num_entries: 0,
            entry_mask_hash: None,
        };

        Self {
            metadata,
            path: collection_root
                .child(PARTITIONS_DIR.to_string())
                .child(partition_name.clone()),
            object_store,
            array_writers: IndexMap::new(),
            write_options: options,
            entry_mask: None,
        }
    }

    /// Set an entry deletion mask for this partition.
    ///
    /// `deleted[i] == true` means entry `i` is logically deleted and should be
    /// skipped by readers.
    pub fn set_entry_mask_deleted(&mut self, deleted: Vec<bool>) {
        self.entry_mask = Some(deleted);
    }

    /// Write a logical entry comprised of multiple arrays (streamed per field).
    ///
    /// `arrays` should yield each projected field once. Missing fields are
    /// automatically padded with null entries so subsequent reads retain a
    /// consistent cardinality across arrays.
    pub async fn write_entry(
        &mut self,
        entry_name: &str,
        arrays: impl Stream<Item = (FieldRef, NdArrowArray)>,
    ) -> BBFResult<()> {
        let mut pinned = pin::pin!(arrays);
        let mut skipped_arrays = (0..self.array_writers.len()).collect::<Vec<usize>>();

        while let Some((field, array)) = pinned.next().await {
            // Check if we have an array writer for this array
            if !self.array_writers.contains_key(field.name()) {
                // Create new array writer
                let array_partition_writer = ArrayPartitionWriter::new(
                    field.name().to_string(),
                    self.write_options.max_group_size,
                    Some(field.data_type().to_owned()),
                    self.metadata.num_entries,
                )
                .await?;
                self.array_writers
                    .insert(field.name().to_string(), array_partition_writer);
            }

            // Write to array writer
            let (idx, _, array_writer) = self.array_writers.get_full_mut(field.name()).unwrap();
            array_writer.append_array(Some(array)).await?;
            // Remove from skipped arrays
            skipped_arrays.retain(|&i| i != idx);
        }
        // Write __entry_key array
        if !self.array_writers.contains_key("__entry_key") {
            let array_partition_writer = ArrayPartitionWriter::new(
                "__entry_key".to_string(),
                self.write_options.max_group_size,
                Some(arrow::datatypes::DataType::Utf8),
                self.metadata.num_entries,
            )
            .await?;
            self.array_writers
                .insert("__entry_key".to_string(), array_partition_writer);
        }
        let (idx, _, entry_key_writer) = self.array_writers.get_full_mut("__entry_key").unwrap();
        let arrow_array = StringArray::from(vec![entry_name]);
        let nd_arrow_array = NdArrowArray::new(
            Arc::new(arrow_array),
            nd_arrow_array::dimensions::Dimensions::Scalar,
        )
        .expect("create entry key array");
        entry_key_writer.append_array(Some(nd_arrow_array)).await?;
        // Remove from skipped arrays
        skipped_arrays.retain(|&i| i != idx);

        // For skipped arrays, write a null entry
        for idx in skipped_arrays {
            let (_, array_writer) = self
                .array_writers
                .get_index_mut(idx)
                .expect("array writer exists");
            array_writer.append_array(None).await?;
        }

        self.metadata.num_entries += 1;
        Ok(())
    }

    /// Finalize all array partitions, returning the completed metadata.
    ///
    /// This drains every `ArrayPartitionWriter`, ensuring their buffers flush
    /// to the object store before aggregating byte counts and element totals.
    pub async fn finish(mut self) -> BBFResult<CollectionPartitionMetadata> {
        let mut total_byte_size = 0;
        let mut total_num_elements = 0;
        let mut resolution = PartitionResolution::default();
        let mut offset: u64 = 0;
        let mut pruning_offset: u64 = 0;

        let blob_path = self.path.child(PARTITION_BLOB_FILE);
        let mut blob_writer =
            object_store::buffered::BufWriter::new(self.object_store.clone(), blob_path.clone());

        let pruning_blob_path = self.path.child(PARTITION_PRUNING_INDEX_FILE);
        let mut pruning_writer: Option<object_store::buffered::BufWriter> = None;

        let entry_mask_path = self.path.child(PARTITION_ENTRY_MASK_FILE);

        for (array_name, array_writer) in self.array_writers {
            let artifact = array_writer.finish().await?;

            total_byte_size += artifact.metadata.partition_byte_size;
            total_num_elements += artifact.metadata.num_elements;

            let array_size = artifact.file_size;
            resolution.objects.insert(
                artifact.metadata.hash.clone(),
                ResolvedSlice {
                    offset,
                    size: array_size,
                },
            );

            let mut array_file = artifact.file;
            array_file.seek(SeekFrom::Start(0)).await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
            })?;
            tokio::io::copy(&mut array_file, &mut blob_writer)
                .await
                .map_err(|e| {
                    crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
                })?;

            offset = offset.saturating_add(array_size);

            // Append optional pruning index to the partition-level pruning blob.
            if let (Some(index_hash), Some(mut index_file), Some(index_size)) = (
                artifact.metadata.pruning_index_hash.clone(),
                artifact.pruning_index_file,
                artifact.pruning_index_size,
            ) {
                resolution.objects.insert(
                    index_hash,
                    ResolvedSlice {
                        offset: pruning_offset,
                        size: index_size,
                    },
                );

                index_file.seek(SeekFrom::Start(0)).await.map_err(|e| {
                    crate::error::BBFWritingError::ArrayPartitionPruningIndexWriteFailure(Box::new(
                        e,
                    ))
                })?;

                if pruning_writer.is_none() {
                    pruning_writer = Some(object_store::buffered::BufWriter::new(
                        self.object_store.clone(),
                        pruning_blob_path.clone(),
                    ));
                }

                let writer = pruning_writer.as_mut().expect("pruning writer");
                tokio::io::copy(&mut index_file, writer)
                    .await
                    .map_err(|e| {
                        crate::error::BBFWritingError::ArrayPartitionPruningIndexWriteFailure(
                            Box::new(e),
                        )
                    })?;
                pruning_offset = pruning_offset.saturating_add(index_size);
            }

            self.metadata.arrays.insert(array_name, artifact.metadata);
        }

        blob_writer.flush().await.map_err(|e| {
            crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
        })?;
        blob_writer.shutdown().await.map_err(|e| {
            crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
        })?;

        if let Some(mut writer) = pruning_writer {
            writer.flush().await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionPruningIndexWriteFailure(Box::new(e))
            })?;
            writer.shutdown().await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionPruningIndexWriteFailure(Box::new(e))
            })?;
        }

        // Persist optional entry mask alongside the partition.
        if let Some(deleted) = self.entry_mask {
            if deleted.len() != self.metadata.num_entries {
                return Err(BBFWritingError::EntryMaskLengthMismatch {
                    expected: self.metadata.num_entries,
                    actual: deleted.len(),
                }
                .into());
            }
            let (mut mask_file, mask_size, mask_hash) =
                encode_deleted_mask_to_tempfile(&deleted).await?;

            resolution.objects.insert(
                mask_hash.clone(),
                ResolvedSlice {
                    offset: 0,
                    size: mask_size,
                },
            );

            mask_file.seek(SeekFrom::Start(0)).await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
            })?;

            let mut mask_writer =
                object_store::buffered::BufWriter::new(self.object_store.clone(), entry_mask_path);
            tokio::io::copy(&mut mask_file, &mut mask_writer)
                .await
                .map_err(|e| {
                    crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
                })?;
            mask_writer.flush().await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
            })?;
            mask_writer.shutdown().await.map_err(|e| {
                crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))
            })?;

            self.metadata.entry_mask_hash = Some(mask_hash);
        }

        let resolution_path = self.path.child(PARTITION_RESOLUTION_FILE);
        let resolution_payload = serde_json::to_vec(&resolution).map_err(|e| {
            crate::error::BBFWritingError::CollectionMetadataWriteFailure(Box::new(e))
        })?;
        self.object_store
            .put(
                &resolution_path,
                PutPayload::from_bytes(Bytes::from(resolution_payload)),
            )
            .await
            .map_err(|e| {
                crate::error::BBFWritingError::CollectionMetadataWriteFailure(Box::new(e))
            })?;

        // Build partition schema
        let mut fields = Vec::new();
        for (array_name, array_metadata) in &self.metadata.arrays {
            let field = Field::new(array_name.clone(), array_metadata.data_type.clone(), true);
            fields.push(field);
        }
        self.metadata.partition_schema = Arc::new(arrow::datatypes::Schema::new(fields));

        self.metadata.byte_size = total_byte_size;
        self.metadata.num_elements = total_num_elements;

        Ok(self.metadata)
    }
}

/// Persist an entry deletion mask for an existing partition, updating
/// `partition_metadata.entry_mask_hash` and `resolution.json`.
pub async fn apply_entry_mask_deleted(
    object_store: Arc<dyn ObjectStore>,
    partition_path: object_store::path::Path,
    partition_metadata: &mut CollectionPartitionMetadata,
    deleted: &[bool],
) -> BBFResult<()> {
    if deleted.len() != partition_metadata.num_entries {
        return Err(BBFWritingError::EntryMaskLengthMismatch {
            expected: partition_metadata.num_entries,
            actual: deleted.len(),
        }
        .into());
    }
    let (mut mask_file, mask_size, mask_hash) = encode_deleted_mask_to_tempfile(deleted).await?;

    // Update / create entry_mask.bbem.
    let mask_path = partition_path.child(PARTITION_ENTRY_MASK_FILE);
    mask_file
        .seek(SeekFrom::Start(0))
        .await
        .map_err(|e| crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
    let mut mask_writer =
        object_store::buffered::BufWriter::new(object_store.clone(), mask_path.clone());
    tokio::io::copy(&mut mask_file, &mut mask_writer)
        .await
        .map_err(|e| crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
    mask_writer
        .flush()
        .await
        .map_err(|e| crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
    mask_writer
        .shutdown()
        .await
        .map_err(|e| crate::error::BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

    // Load/patch resolution.json.
    let resolution_path = partition_path.child(PARTITION_RESOLUTION_FILE);
    let resolution_bytes = object_store
        .get(&resolution_path)
        .await
        .map_err(|source| BBFReadingError::PartitionResolutionFetch {
            meta_path: resolution_path.to_string(),
            source,
        })?
        .bytes()
        .await
        .map_err(|source| BBFReadingError::PartitionResolutionFetch {
            meta_path: resolution_path.to_string(),
            source,
        })?;
    let mut resolution: PartitionResolution =
        serde_json::from_slice(&resolution_bytes).map_err(|e| {
            BBFReadingError::PartitionResolutionDecode {
                meta_path: resolution_path.to_string(),
                reason: e.to_string(),
            }
        })?;

    resolution.objects.insert(
        mask_hash.clone(),
        ResolvedSlice {
            offset: 0,
            size: mask_size,
        },
    );

    let payload = serde_json::to_vec(&resolution)
        .map_err(|e| crate::error::BBFWritingError::CollectionMetadataWriteFailure(Box::new(e)))?;
    object_store
        .put(
            &resolution_path,
            PutPayload::from_bytes(Bytes::from(payload)),
        )
        .await
        .map_err(|e| crate::error::BBFWritingError::CollectionMetadataWriteFailure(Box::new(e)))?;

    partition_metadata.entry_mask_hash = Some(mask_hash);
    Ok(())
}

/// Mark entries as logically deleted by matching on the `__entry_key` value.
///
/// This reads only the `__entry_key` array to map keys to entry indices, then
/// writes an updated `entry_mask.bbem` + `resolution.json`.
pub async fn apply_entry_mask_deleted_by_entry_keys(
    object_store: Arc<dyn ObjectStore>,
    partition_path: object_store::path::Path,
    partition_metadata: &mut CollectionPartitionMetadata,
    entry_keys_to_delete: impl IntoIterator<Item = String>,
) -> BBFResult<()> {
    let keys: HashSet<String> = entry_keys_to_delete.into_iter().collect();
    if keys.is_empty() {
        return Ok(());
    }

    let entry_key_meta = partition_metadata
        .arrays
        .get("__entry_key")
        .ok_or_else(|| {
            BBFReadingError::ArrayGroupReadFailure(
                "missing __entry_key partition metadata".to_string(),
                Box::new(std::io::Error::other("missing __entry_key")),
            )
        })?
        .clone();

    // Load resolution.json.
    let resolution_path = partition_path.child(PARTITION_RESOLUTION_FILE);
    let resolution_bytes = object_store
        .get(&resolution_path)
        .await
        .map_err(|source| BBFReadingError::PartitionResolutionFetch {
            meta_path: resolution_path.to_string(),
            source,
        })?
        .bytes()
        .await
        .map_err(|source| BBFReadingError::PartitionResolutionFetch {
            meta_path: resolution_path.to_string(),
            source,
        })?;
    let resolution: PartitionResolution =
        serde_json::from_slice(&resolution_bytes).map_err(|e| {
            BBFReadingError::PartitionResolutionDecode {
                meta_path: resolution_path.to_string(),
                reason: e.to_string(),
            }
        })?;

    // Build an ArrayPartitionReader for __entry_key only.
    let blob_path = partition_path.child(PARTITION_BLOB_FILE);
    let slice = *resolution
        .objects
        .get(&entry_key_meta.hash)
        .ok_or_else(|| BBFReadingError::PartitionResolutionMissing {
            meta_path: resolution_path.to_string(),
            hash: entry_key_meta.hash.clone(),
        })?;

    let entry_key_reader = ArrayPartitionReader::new(
        object_store.clone(),
        "__entry_key".to_string(),
        blob_path,
        slice,
        None,
        entry_key_meta,
        io_cache::ArrayIoCache::new(1024 * 1024),
    )
    .await?;

    // Start with current mask (or all-false).
    let mut deleted = if let Some(mask_hash) = partition_metadata.entry_mask_hash.as_ref() {
        let slice = *resolution.objects.get(mask_hash).ok_or_else(|| {
            BBFReadingError::PartitionResolutionMissing {
                meta_path: resolution_path.to_string(),
                hash: mask_hash.clone(),
            }
        })?;
        let mask_path = partition_path.child(PARTITION_ENTRY_MASK_FILE);
        let bytes = object_store
            .get_range(&mask_path, slice.offset..slice.offset + slice.size)
            .await
            .map_err(|source| BBFReadingError::PartitionBytesFetch {
                partition_path: mask_path.to_string(),
                source,
            })?;

        let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(|e| {
            BBFReadingError::EntryMaskDecode {
                reason: e.to_string(),
            }
        })?;
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
            BBFReadingError::EntryMaskDecode {
                reason: e.to_string(),
            }
        })?;

        let batch = if batches.is_empty() {
            return Err(BBFReadingError::EntryMaskLengthMismatch {
                expected: partition_metadata.num_entries,
                actual: 0,
            }
            .into());
        } else if batches.len() == 1 {
            batches.into_iter().next().expect("batch")
        } else {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                BBFReadingError::EntryMaskDecode {
                    reason: e.to_string(),
                }
            })?
        };
        let existing = decode_deleted_mask(&batch)?;
        if existing.len() != partition_metadata.num_entries {
            return Err(BBFReadingError::EntryMaskLengthMismatch {
                expected: partition_metadata.num_entries,
                actual: existing.len(),
            }
            .into());
        }
        existing
    } else {
        vec![false; partition_metadata.num_entries]
    };

    // Mark keys.
    for idx in 0..partition_metadata.num_entries {
        if deleted[idx] {
            continue;
        }
        let Some(nd) = entry_key_reader.read_array(idx).await? else {
            continue;
        };
        let arr = nd.as_arrow_array();
        if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
            if s.len() > 0 && keys.contains(s.value(0)) {
                deleted[idx] = true;
            }
        } else if let Some(s) = arr.as_any().downcast_ref::<LargeStringArray>() {
            if s.len() > 0 && keys.contains(s.value(0)) {
                deleted[idx] = true;
            }
        } else {
            return Err(BBFReadingError::ArrayGroupReadFailure(
                "__entry_key is not a string array".to_string(),
                Box::new(std::io::Error::other("invalid __entry_key type")),
            )
            .into());
        }
    }

    apply_entry_mask_deleted(object_store, partition_path, partition_metadata, &deleted).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{StreamExt, stream};
    use indexmap::IndexMap;
    use nd_arrow_array::dimensions::{Dimension, Dimensions};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc as StdArc;

    /// Create a 1D integer `NdArrowArray` used by the fixture builders.
    fn scalar_int32(values: &[i32]) -> NdArrowArray {
        let array: ArrayRef = StdArc::new(Int32Array::from(values.to_vec()));
        let dimension = Dimension {
            name: "dim0".to_string(),
            size: values.len(),
        };
        NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
            .expect("nd array creation")
    }

    /// Verifies that writing entries tracks every array plus the synthetic
    /// `__entry_key` column used for joins.
    #[tokio::test]
    async fn write_entry_records_arrays_and_entry_keys() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let path = Path::from("collection/test");
        let mut writer = CollectionPartitionWriter::new(
            path,
            store,
            "test-partition".to_string(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: FieldRef = StdArc::new(Field::new("temp", DataType::Int32, true));
        let sal_field: FieldRef = StdArc::new(Field::new("sal", DataType::Int32, true));

        writer
            .write_entry(
                "entry-1",
                stream::iter(vec![
                    (temp_field.clone(), scalar_int32(&[1, 2])),
                    (sal_field.clone(), scalar_int32(&[7])),
                ]),
            )
            .await
            .expect("write entry 1");

        writer
            .write_entry(
                "entry-2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[3]))]),
            )
            .await
            .expect("write entry 2");

        let metadata = writer.finish().await.expect("finish success");

        assert_eq!(metadata.num_entries, 2);
        assert!(metadata.byte_size > 0);
        assert_eq!(metadata.arrays.len(), 3);

        let temp_meta = metadata.arrays.get("temp").expect("temp metadata");
        assert_eq!(temp_meta.num_elements, 3);
        assert_eq!(temp_meta.partition_offset, 0);

        let sal_meta = metadata.arrays.get("sal").expect("sal metadata");
        assert_eq!(sal_meta.num_elements, 1);
        let sal_group = sal_meta.groups.values().next().expect("sal group metadata");
        assert_eq!(sal_group.num_chunks, 2, "null entry was recorded");

        let entry_key_meta = metadata
            .arrays
            .get("__entry_key")
            .expect("entry key metadata");
        assert_eq!(entry_key_meta.num_elements, 2);
    }

    /// Builds an in-memory reader with two entries to simplify test setup.
    async fn build_reader_fixture() -> CollectionPartitionReader {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let collection_root = Path::from("collection/read");
        let partition_name = "test-partition".to_string();
        let mut writer = CollectionPartitionWriter::new(
            collection_root.clone(),
            store.clone(),
            partition_name.clone(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: FieldRef = StdArc::new(Field::new("temp", DataType::Int32, true));
        let sal_field: FieldRef = StdArc::new(Field::new("sal", DataType::Int32, true));

        writer
            .write_entry(
                "entry-1",
                stream::iter(vec![
                    (temp_field.clone(), scalar_int32(&[1, 2])),
                    (sal_field.clone(), scalar_int32(&[7])),
                ]),
            )
            .await
            .expect("write entry 1");

        writer
            .write_entry(
                "entry-2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[3]))]),
            )
            .await
            .expect("write entry 2");

        let mut metadata = writer.finish().await.expect("finish success");
        metadata.partition_schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Int32, true),
            Field::new("sal", DataType::Int32, true),
            Field::new("__entry_key", DataType::Utf8, false),
        ]));

        CollectionPartitionReader::new(
            collection_root
                .child(PARTITIONS_DIR.to_string())
                .child(partition_name),
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        )
    }

    async fn build_reader_fixture_with_entry_mask(deleted: Vec<bool>) -> CollectionPartitionReader {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let collection_root = Path::from("collection/read-mask");
        let partition_name = "test-partition".to_string();
        let mut writer = CollectionPartitionWriter::new(
            collection_root.clone(),
            store.clone(),
            partition_name.clone(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: FieldRef = StdArc::new(Field::new("temp", DataType::Int32, true));
        let sal_field: FieldRef = StdArc::new(Field::new("sal", DataType::Int32, true));

        writer
            .write_entry(
                "entry-1",
                stream::iter(vec![
                    (temp_field.clone(), scalar_int32(&[1, 2])),
                    (sal_field.clone(), scalar_int32(&[7])),
                ]),
            )
            .await
            .expect("write entry 1");

        writer
            .write_entry(
                "entry-2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[3]))]),
            )
            .await
            .expect("write entry 2");

        writer.set_entry_mask_deleted(deleted);

        let mut metadata = writer.finish().await.expect("finish success");
        metadata.partition_schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Int32, true),
            Field::new("sal", DataType::Int32, true),
            Field::new("__entry_key", DataType::Utf8, false),
        ]));

        CollectionPartitionReader::new(
            collection_root
                .child(PARTITIONS_DIR.to_string())
                .child(partition_name),
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        )
    }

    async fn build_partition_fixture() -> (
        StdArc<dyn ObjectStore>,
        Path,
        String,
        CollectionPartitionMetadata,
    ) {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let collection_root = Path::from("collection/apply-mask");
        let partition_name = "test-partition".to_string();
        let mut writer = CollectionPartitionWriter::new(
            collection_root.clone(),
            store.clone(),
            partition_name.clone(),
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: FieldRef = StdArc::new(Field::new("temp", DataType::Int32, true));
        let sal_field: FieldRef = StdArc::new(Field::new("sal", DataType::Int32, true));

        writer
            .write_entry(
                "entry-1",
                stream::iter(vec![
                    (temp_field.clone(), scalar_int32(&[1, 2])),
                    (sal_field.clone(), scalar_int32(&[7])),
                ]),
            )
            .await
            .expect("write entry 1");

        writer
            .write_entry(
                "entry-2",
                stream::iter(vec![(temp_field.clone(), scalar_int32(&[3]))]),
            )
            .await
            .expect("write entry 2");

        let mut metadata = writer.finish().await.expect("finish success");
        metadata.partition_schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Int32, true),
            Field::new("sal", DataType::Int32, true),
            Field::new("__entry_key", DataType::Utf8, false),
        ]));

        (store, collection_root, partition_name, metadata)
    }

    /// Transform an Arrow `Int32Array` into optional scalars to ease
    /// assertions about null propagation.
    fn collect_optional_ints(array: &Int32Array) -> Vec<Option<i32>> {
        (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    None
                } else {
                    Some(array.value(idx))
                }
            })
            .collect()
    }

    /// Ensures readers yield one batch per entry and surface nulls when an
    /// array was missing for a given entry.
    #[tokio::test]
    async fn read_returns_batches_for_all_arrays() {
        let reader = build_reader_fixture().await;
        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), reader.metadata.num_entries);

        let mut observed: IndexMap<String, (Vec<Option<i32>>, Vec<Option<i32>>)> = IndexMap::new();

        for batch_result in batches {
            let batch = batch_result.expect("batch success");
            let schema = batch.schema();
            let arrays = batch.arrays();

            let mut entry_key = None;
            let mut temp = None;
            let mut sal = None;

            for (field, nd_array) in schema.fields().iter().zip(arrays.iter()) {
                match field.name().as_str() {
                    "temp" => {
                        let arr = nd_array
                            .as_arrow_array()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("temp int array");
                        temp = Some(collect_optional_ints(arr));
                    }
                    "sal" => {
                        let arr = nd_array
                            .as_arrow_array()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("sal int array");
                        sal = Some(collect_optional_ints(arr));
                    }
                    "__entry_key" => {
                        let arr = nd_array
                            .as_arrow_array()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("entry key array");
                        entry_key = Some(arr.value(0).to_string());
                    }
                    _ => {}
                }
            }

            let entry_key = entry_key.expect("entry key present");
            observed.insert(
                entry_key,
                (
                    temp.expect("temp data present"),
                    sal.expect("sal data present"),
                ),
            );
        }

        let entry_one = observed.get("entry-1").expect("entry-1 present");
        assert_eq!(entry_one.0, vec![Some(1), Some(2)]);
        assert_eq!(entry_one.1, vec![Some(7)]);

        let entry_two = observed.get("entry-2").expect("entry-2 present");
        assert_eq!(entry_two.0, vec![Some(3)]);
        assert_eq!(entry_two.1, vec![None]);
    }

    /// Confirms field projection only materializes requested arrays while
    /// still emitting every logical entry.
    #[tokio::test]
    async fn read_respects_projection() {
        let reader = build_reader_fixture().await;
        let projection: Arc<[String]> =
            Arc::from(vec!["temp".to_string(), "__entry_key".to_string()].into_boxed_slice());

        let scheduler = reader
            .read(
                Some(projection),
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 1,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), reader.metadata.num_entries);

        for batch_result in batches {
            let batch = batch_result.expect("batch success");
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "temp");
            assert_eq!(schema.field(1).name(), "__entry_key");

            let arrays = batch.arrays();
            let temp_arr = arrays[0]
                .as_arrow_array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("temp int array");
            let entry_key_arr = arrays[1]
                .as_arrow_array()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("entry key array");

            match entry_key_arr.value(0) {
                "entry-1" => {
                    assert_eq!(collect_optional_ints(temp_arr), vec![Some(1), Some(2)]);
                }
                "entry-2" => {
                    assert_eq!(collect_optional_ints(temp_arr), vec![Some(3)]);
                }
                other => panic!("unexpected entry key {other}"),
            }
        }
    }

    #[tokio::test]
    async fn read_skips_deleted_entries_when_mask_is_present() {
        let reader = build_reader_fixture_with_entry_mask(vec![false, true]).await;
        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);

        let batch = batches[0].as_ref().expect("batch success");
        let schema = batch.schema();
        let arrays = batch.arrays();
        let entry_key_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "__entry_key")
            .expect("entry key column");
        let entry_key_arr = arrays[entry_key_idx]
            .as_arrow_array()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entry key array");
        assert_eq!(entry_key_arr.value(0), "entry-1");
    }

    #[tokio::test]
    async fn apply_entry_mask_deleted_updates_reader_behavior() {
        let (store, collection_root, partition_name, mut metadata) =
            build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        apply_entry_mask_deleted(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            &[true, false],
        )
        .await
        .expect("apply mask");

        let reader = CollectionPartitionReader::new(
            partition_path,
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        );

        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().expect("batch success");
        let schema = batch.schema();
        let arrays = batch.arrays();
        let entry_key_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "__entry_key")
            .expect("entry key column");
        let entry_key_arr = arrays[entry_key_idx]
            .as_arrow_array()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entry key array");
        assert_eq!(entry_key_arr.value(0), "entry-2");
    }

    #[tokio::test]
    async fn apply_entry_mask_deleted_by_entry_key_removes_matching_entries() {
        let (store, collection_root, partition_name, mut metadata) =
            build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        apply_entry_mask_deleted_by_entry_keys(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            vec!["entry-2".to_string()],
        )
        .await
        .expect("apply key-based mask");

        let reader = CollectionPartitionReader::new(
            partition_path,
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        );

        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().expect("batch success");
        let schema = batch.schema();
        let arrays = batch.arrays();
        let entry_key_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "__entry_key")
            .expect("entry key column");
        let entry_key_arr = arrays[entry_key_idx]
            .as_arrow_array()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entry key array");
        assert_eq!(entry_key_arr.value(0), "entry-1");
    }

    #[tokio::test]
    async fn apply_entry_mask_deleted_by_entry_key_is_noop_for_empty_keys() {
        let (store, collection_root, partition_name, metadata) = build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        let reader = CollectionPartitionReader::new(
            partition_path,
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        );

        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn apply_entry_mask_deleted_by_entry_key_ignores_unknown_keys() {
        let (store, collection_root, partition_name, mut metadata) =
            build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        apply_entry_mask_deleted_by_entry_keys(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            vec!["does-not-exist".to_string()],
        )
        .await
        .expect("apply key-based mask");

        let reader = CollectionPartitionReader::new(
            partition_path,
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        );

        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn apply_entry_mask_deleted_by_entry_key_merges_with_existing_mask() {
        let (store, collection_root, partition_name, mut metadata) =
            build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        // First delete entry-1 via raw mask.
        apply_entry_mask_deleted(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            &[true, false],
        )
        .await
        .expect("apply mask");

        // Then delete entry-2 via key match.
        apply_entry_mask_deleted_by_entry_keys(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            vec!["entry-2".to_string()],
        )
        .await
        .expect("apply key-based mask");

        let reader = CollectionPartitionReader::new(
            partition_path,
            store,
            metadata,
            io_cache::ArrayIoCache::new(1024 * 1024),
        );

        let scheduler = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: None,
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn finish_errors_on_entry_mask_length_mismatch() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let collection_root = Path::from("collection/finish-mask-mismatch");
        let partition_name = "test-partition".to_string();
        let mut writer = CollectionPartitionWriter::new(
            collection_root.clone(),
            store,
            partition_name,
            WriterOptions {
                max_group_size: usize::MAX,
            },
        );

        let temp_field: FieldRef = StdArc::new(Field::new("temp", DataType::Int32, true));

        writer
            .write_entry(
                "entry-1",
                stream::iter(vec![(temp_field, scalar_int32(&[1]))]),
            )
            .await
            .expect("write entry");

        // num_entries = 1 but mask length is 2.
        writer.set_entry_mask_deleted(vec![false, true]);
        let err = writer.finish().await.expect_err("expected mismatch");
        assert!(matches!(
            err,
            BBFError::Writing(BBFWritingError::EntryMaskLengthMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn apply_errors_on_entry_mask_length_mismatch() {
        let (store, collection_root, partition_name, mut metadata) =
            build_partition_fixture().await;
        let partition_path = collection_root
            .child(PARTITIONS_DIR.to_string())
            .child(partition_name.clone());

        let err = apply_entry_mask_deleted(
            store.clone(),
            partition_path.clone(),
            &mut metadata,
            &[true],
        )
        .await
        .expect_err("expected length mismatch");

        assert!(matches!(
            err,
            BBFError::Writing(BBFWritingError::EntryMaskLengthMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn read_pruning_index_respects_projection_and_returns_expected_stats() {
        let reader = build_reader_fixture().await;
        let projection: Arc<[String]> = Arc::from(vec!["temp".to_string()].into_boxed_slice());

        let batch = reader
            .read_pruning_index(Some(projection))
            .await
            .expect("read pruning index")
            .expect("expected pruning index batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "temp:min");
        assert_eq!(batch.schema().field(1).name(), "temp:max");
        assert_eq!(batch.schema().field(2).name(), "temp:null_count");
        assert_eq!(batch.schema().field(3).name(), "temp:row_count");

        let min = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("min int32");
        let max = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("max int32");
        let null_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("null_count u64");
        let row_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row_count u64");

        assert_eq!(min.value(0), 1);
        assert_eq!(max.value(0), 2);
        assert_eq!(null_count.value(0), 0);
        assert_eq!(row_count.value(0), 2);

        assert_eq!(min.value(1), 3);
        assert_eq!(max.value(1), 3);
        assert_eq!(null_count.value(1), 0);
        assert_eq!(row_count.value(1), 1);
    }

    #[tokio::test]
    async fn read_pruning_index_returns_combined_batch_for_multiple_columns() {
        let reader = build_reader_fixture().await;
        let projection: Arc<[String]> =
            Arc::from(vec!["temp".to_string(), "sal".to_string()].into_boxed_slice());

        let batch = reader
            .read_pruning_index(Some(projection))
            .await
            .expect("read pruning index")
            .expect("expected pruning index batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8);

        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert!(names.contains(&"temp:min".to_string()));
        assert!(names.contains(&"temp:max".to_string()));
        assert!(names.contains(&"temp:null_count".to_string()));
        assert!(names.contains(&"temp:row_count".to_string()));
        assert!(names.contains(&"sal:min".to_string()));
        assert!(names.contains(&"sal:max".to_string()));
        assert!(names.contains(&"sal:null_count".to_string()));
        assert!(names.contains(&"sal:row_count".to_string()));
    }

    #[tokio::test]
    async fn read_respects_entry_selection_mask() {
        let reader = build_reader_fixture().await;
        let projection: Arc<[String]> =
            Arc::from(vec!["__entry_key".to_string()].into_boxed_slice());

        let scheduler = reader
            .read(
                Some(projection),
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 2,
                    entry_selection: Some(vec![false, true]),
                },
            )
            .await
            .expect("scheduler");
        let stream = scheduler.shared_pollable_stream_ref().await;
        let batches = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().expect("batch success");
        let schema = batch.schema();
        let arrays = batch.arrays();
        let entry_key_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "__entry_key")
            .expect("entry key column");
        let entry_key_arr = arrays[entry_key_idx]
            .as_arrow_array()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entry key array");
        assert_eq!(entry_key_arr.value(0), "entry-2");
    }

    #[tokio::test]
    async fn read_errors_on_entry_selection_length_mismatch() {
        let reader = build_reader_fixture().await;
        let err = reader
            .read(
                None,
                CollectionPartitionReadOptions {
                    max_concurrent_reads: 1,
                    entry_selection: Some(vec![true]),
                },
            )
            .await
            .expect_err("expected mismatch");

        assert!(matches!(
            err,
            BBFError::Reading(BBFReadingError::EntrySelectionLengthMismatch { .. })
        ));
    }
}
