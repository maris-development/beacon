use std::pin;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow_schema::FieldRef;
use futures::Stream;
use futures::StreamExt;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use nd_arrow_array::batch::NdRecordBatch;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::array_partition::ArrayPartitionReader;
use crate::error::BBFError;
use crate::error::BBFReadingError;
use crate::io_cache;
use crate::stream::AsyncStreamScheduler;
use crate::{
    array_partition::{ArrayPartitionMetadata, ArrayPartitionWriter},
    error::BBFResult,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPartitionMetadata {
    pub byte_size: usize,
    pub num_elements: usize,
    pub num_entries: usize,
    pub partition_schema: Arc<arrow::datatypes::Schema>,
    pub arrays: IndexMap<String, ArrayPartitionMetadata>,
}

pub struct CollectionPartitionReader {
    pub metadata: CollectionPartitionMetadata,
    pub path: object_store::path::Path,
    pub object_store: Arc<dyn ObjectStore>,
    pub io_cache: io_cache::ArrayIoCache,
}

pub struct CollectionPartitionReadOptions {
    pub max_concurrent_reads: usize,
}

impl CollectionPartitionReader {
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
        }
    }

    pub async fn read(
        &self,
        projection: Option<Arc<[String]>>,
        options: CollectionPartitionReadOptions,
    ) -> BBFResult<AsyncStreamScheduler<BBFResult<NdRecordBatch>>> {
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

        let mut array_readers = IndexMap::new();
        for (array_name, array_metadata) in &arrays_to_read {
            let array_partition_reader = ArrayPartitionReader::new(
                self.object_store.clone(),
                array_name.clone(),
                self.path.child(array_name.clone()),
                array_metadata.clone(),
                self.io_cache.clone(),
            )
            .await?;
            array_readers.insert(array_name.clone(), array_partition_reader);
        }
        let shared_readers = Arc::new(array_readers);
        let mut projected_fields = Vec::new();
        for (array_name, array_metadata) in &arrays_to_read {
            let field = self
                .metadata
                .partition_schema
                .field_with_name(array_name)
                .expect("field exists")
                .clone();
            projected_fields.push(field);
        }
        let projected_schema = Arc::new(arrow::datatypes::Schema::new(projected_fields.clone()));

        let mut futures = Vec::new();
        for index in 0..self.metadata.num_entries {
            // For each entry, read arrays
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
}

pub struct CollectionPartitionWriter {
    pub metadata: CollectionPartitionMetadata,
    pub path: object_store::path::Path,
    pub object_store: Arc<dyn ObjectStore>,
    pub array_writers: IndexMap<String, ArrayPartitionWriter>,
    pub write_options: WriterOptions,
}

pub struct WriterOptions {
    pub max_group_size: usize,
}

impl CollectionPartitionWriter {
    pub fn new(
        path: object_store::path::Path,
        object_store: Arc<dyn ObjectStore>,
        options: WriterOptions,
    ) -> Self {
        let metadata = CollectionPartitionMetadata {
            byte_size: 0,
            num_elements: 0,
            partition_schema: Arc::new(arrow::datatypes::Schema::empty()),
            arrays: IndexMap::new(),
            num_entries: 0,
        };

        Self {
            metadata,
            path,
            object_store,
            array_writers: IndexMap::new(),
            write_options: options,
        }
    }

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
                let path = self.path.child(field.name().to_string());
                let array_partition_writer = ArrayPartitionWriter::new(
                    self.object_store.clone(),
                    path,
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
            let path = self.path.child("__entry_key".to_string());
            let array_partition_writer = ArrayPartitionWriter::new(
                self.object_store.clone(),
                path,
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

    pub async fn finish(mut self) -> BBFResult<CollectionPartitionMetadata> {
        let mut total_byte_size = 0;
        let mut total_num_elements = 0;
        for (array_name, array_writer) in self.array_writers {
            let array_metadata = array_writer.finish().await?;
            total_byte_size += array_metadata.partition_byte_size;
            total_num_elements += array_metadata.num_elements;
            self.metadata.arrays.insert(array_name, array_metadata);
        }

        self.metadata.byte_size = total_byte_size;
        self.metadata.num_elements = total_num_elements;

        Ok(self.metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field};
    use futures::stream;
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

    #[tokio::test]
    async fn write_entry_records_arrays_and_entry_keys() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let path = Path::from("collection/test");
        let mut writer = CollectionPartitionWriter::new(
            path,
            store,
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
}
