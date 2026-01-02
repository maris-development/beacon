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

use arrow::array::StringArray;
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
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::OnceCell;

use crate::array_partition::ArrayPartitionReader;
use crate::error::BBFError;
use crate::error::BBFReadingError;
use crate::io_cache;
use crate::layout::{
    PARTITION_BLOB_FILE, PARTITION_PRUNING_INDEX_FILE, PARTITION_RESOLUTION_FILE, PARTITIONS_DIR,
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
}

/// Tunable options that control how collection partitions are read.
///
/// These options primarily influence concurrency when the reader builds a
/// future per entry. Keeping the value modest helps avoid overwhelming the
/// backing object store.
pub struct CollectionPartitionReadOptions {
    /// Maximum number of concurrent entry read tasks.
    pub max_concurrent_reads: usize,
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
        let mut futures = Vec::new();
        for index in 0..self.metadata.num_entries {
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
        let mut futures = Vec::new();
        for index in 0..self.metadata.num_entries {
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
        };

        Self {
            metadata,
            path: collection_root
                .child(PARTITIONS_DIR.to_string())
                .child(partition_name.clone()),
            object_store,
            array_writers: IndexMap::new(),
            write_options: options,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
