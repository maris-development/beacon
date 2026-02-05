use std::{
    fs::File,
    io::{Seek, SeekFrom},
    sync::Arc,
};

use arrow::ipc::writer::FileWriter;
use beacon_nd_arrow::{NdArrowArray, extension::nd_column_data_type};
use futures::StreamExt;
use object_store::ObjectStore;

use crate::{
    IPC_WRITE_OPTS,
    array::{
        Array, ChunkStore,
        layout::{ArrayLayout, ArrayLayouts},
        pruning::{self, ChunkStatistics, PruningArrayWriter},
    },
    consts, util,
};

/// Writes chunked ND Arrow arrays to object storage using buffered IPC batches.
pub struct ArrayWriter<S: ObjectStore + Clone> {
    array_datatype: arrow::datatypes::DataType,
    store: S,
    path: object_store::path::Path,
    temp_writer: FileWriter<File>,
    num_batches: usize,
    flush_size: usize, // Number of bytes of all the arrays to buffer before flushing to a batch and thus disk
    current_buffer_size: usize, // Current number of bytes buffered
    buffer_arrays: Vec<NdArrowArray>,

    // Layouts of the arrays written so far.
    layouts: Vec<ArrayLayout>,

    // Pruning Array
    pruning_writer: PruningArrayWriter<S>,
}

impl<S: ObjectStore + Clone> ArrayWriter<S> {
    const DEFAULT_FLUSH_SIZE: usize = 8 * 1024 * 1024; // 8 MB

    /// Create a new chunked array writer.
    pub fn new(
        store: S,
        path: object_store::path::Path,
        array_datatype: arrow::datatypes::DataType,
    ) -> Self {
        let field = arrow::datatypes::Field::new(
            "array",
            nd_column_data_type(array_datatype.clone()),
            true,
        );
        let schema = arrow::datatypes::Schema::new(vec![field]);
        let temp_writer = FileWriter::try_new_with_options(
            tempfile::tempfile().unwrap(),
            &schema,
            IPC_WRITE_OPTS.clone(),
        )
        .expect("Failed to create IPC file writer");

        let pruning_writer = PruningArrayWriter::new(
            store.clone(),
            path.child("pruning.arrow"),
            array_datatype.clone(),
        );

        Self {
            array_datatype,
            store,
            path,
            flush_size: Self::DEFAULT_FLUSH_SIZE,
            temp_writer,
            current_buffer_size: 0,
            num_batches: 0,
            buffer_arrays: Vec::new(),
            layouts: Vec::new(),
            pruning_writer,
        }
    }

    pub fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    /// Flush remaining batches and upload the IPC file to object storage.
    pub async fn finalize(mut self) -> anyhow::Result<()> {
        // Flush any remaining data
        self.flush().await?;
        self.pruning_writer.finish().await?;

        self.temp_writer.finish()?;

        // Upload the temp file to object store
        let mut temp_file = self.temp_writer.into_inner()?;
        temp_file.seek(SeekFrom::Start(0))?;

        util::stream_file_to_store::<S>(
            &self.store,
            &self.path.child("array.arrow"),
            &mut temp_file,
            consts::STREAM_CHUNK_SIZE,
        )
        .await?;

        // Create a layout file
        let layout = ArrayLayouts::new(self.layouts);
        let layout_path = self.path.child("layout.arrow");
        layout.save::<S>(self.store, layout_path).await?;

        Ok(())
    }

    pub async fn append_null(&mut self) -> anyhow::Result<()> {
        self.pruning_writer.append(None)?;
        Ok(())
    }

    /// Append a stream of chunked arrays for a dataset index.
    pub async fn append_array<C: ChunkStore>(
        &mut self,
        dataset_index: u32,
        array: Array<C>,
    ) -> anyhow::Result<()> {
        // Check if datatype aligns with writer
        if array.array_datatype != self.array_datatype {
            return Err(anyhow::anyhow!(
                "Array datatype does not match writer datatype"
            ));
        }

        let mut file_array_indices = Vec::new();
        let mut chunk_indices: Vec<Vec<u32>> = Vec::new();
        let chunk_shape: Vec<u32> = array.chunk_shape.iter().map(|d| *d as u32).collect();
        let array_shape: Vec<u32> = array.array_shape.iter().map(|d| *d as u32).collect();

        let mut chunks = array.chunks();
        while let Some(part) = chunks.next().await {
            let part = part?;
            let array_indice = [self.num_batches as u32, self.buffer_arrays.len() as u32];

            self.current_buffer_size += part.array.values().get_array_memory_size();
            let chunk_statistics = Self::generate_statistics(&part.array);
            self.buffer_arrays.push(part.array);
            self.pruning_writer.append(chunk_statistics)?;

            // Check if we need to flush
            if self.current_buffer_size >= self.flush_size {
                self.flush().await?;
            }

            file_array_indices.push(array_indice);
            chunk_indices.push(part.chunk_index.iter().map(|d| *d as u32).collect());
        }

        self.layouts.push(ArrayLayout {
            dataset_index,
            chunk_shape,
            array_shape,
            array_indexes: file_array_indices,
            chunk_indexes: chunk_indices,
        });

        Ok(())
    }

    fn generate_statistics(nd_array: &NdArrowArray) -> Option<ChunkStatistics> {
        let values = nd_array.values();

        Some(ChunkStatistics {
            max: pruning::compute_max_scalar(values).ok().flatten(),
            min: pruning::compute_min_scalar(values).ok().flatten(),
            null_count: values.null_count(),
            row_count: values.len(),
        })
    }

    /// Flush any remaining buffered data to the temp file.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        // If there is any buffered data, write it as a batch
        if !self.buffer_arrays.is_empty() {
            let column_array = beacon_nd_arrow::column::NdArrowArrayColumn::from_rows(
                std::mem::take(&mut self.buffer_arrays),
            )?;

            // Create the schema for the batch
            let field = arrow::datatypes::Field::new(
                "array",
                nd_column_data_type(self.array_datatype.clone()),
                true,
            );

            let schema = arrow::datatypes::Schema::new(vec![field]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(column_array.into_array_ref())],
            )?;

            self.buffer_arrays.clear();

            // Write the batch to the temp file
            self.temp_writer.write(&batch)?;
            self.num_batches += 1;
            self.current_buffer_size = 0;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array as ArrowArray, Int32Array, UInt64Array};
    use arrow::datatypes::DataType;
    use beacon_nd_arrow::NdArrowArray;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use futures::{StreamExt, TryStreamExt};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::array::pruning::PruningArrayReader;
    use crate::array::reader::ArrayReader;
    use crate::array::{Array as AtlasArray, ArrayPart, ChunkStore};

    use super::ArrayWriter;

    #[derive(Debug, Clone)]
    struct TestChunkStore {
        parts: Vec<ArrayPart>,
        index: HashMap<Vec<usize>, ArrayPart>,
    }

    impl TestChunkStore {
        fn new(parts: Vec<ArrayPart>) -> Self {
            let index = parts
                .iter()
                .cloned()
                .map(|part| (part.chunk_index.clone(), part))
                .collect();
            Self { parts, index }
        }
    }

    #[async_trait::async_trait]
    impl ChunkStore for TestChunkStore {
        fn chunks(&self) -> futures::stream::BoxStream<'static, anyhow::Result<ArrayPart>> {
            let parts = self.parts.clone();
            futures::stream::iter(parts.into_iter().map(Ok)).boxed()
        }

        async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
            Ok(self.index.get(&chunk_index).cloned())
        }
    }

    fn make_array(values: Vec<i32>, shape: Vec<usize>, names: Vec<&str>) -> NdArrowArray {
        let dims = names
            .into_iter()
            .zip(shape.into_iter())
            .map(|(name, size)| Dimension::try_new(name, size).unwrap())
            .collect::<Vec<_>>();
        NdArrowArray::new(Arc::new(Int32Array::from(values)), Dimensions::new(dims)).unwrap()
    }

    fn array_values(array: &NdArrowArray) -> Vec<i32> {
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    #[tokio::test]
    async fn writer_and_reader_roundtrip() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("array-roundtrip");

        let parts = vec![
            ArrayPart {
                array: make_array(vec![1, 2], vec![2], vec!["x"]),
                chunk_index: vec![0],
            },
            ArrayPart {
                array: make_array(vec![3, 4], vec![2], vec!["x"]),
                chunk_index: vec![1],
            },
        ];

        let array = AtlasArray {
            array_datatype: DataType::Int32,
            chunk_shape: vec![2],
            array_shape: vec![4],
            chunk_provider: TestChunkStore::new(parts),
        };

        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32);
        writer.append_array(0, array).await?;
        writer.finalize().await?;

        let reader = ArrayReader::new(store.clone(), path.clone()).await?;
        let read_array = reader.read_dataset_array(0).expect("array exists");
        let chunks = read_array.chunks().try_collect::<Vec<_>>().await?;
        assert_eq!(chunks.len(), 2);
        assert_eq!(array_values(&chunks[0].array), vec![1, 2]);
        assert_eq!(array_values(&chunks[1].array), vec![3, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn pruning_array_records_nulls() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("array-pruning");

        let parts = vec![
            ArrayPart {
                array: make_array(vec![1, 2], vec![2], vec!["x"]),
                chunk_index: vec![0],
            },
            ArrayPart {
                array: make_array(vec![3, 4], vec![2], vec!["x"]),
                chunk_index: vec![1],
            },
        ];

        let array = AtlasArray {
            array_datatype: DataType::Int32,
            chunk_shape: vec![2],
            array_shape: vec![4],
            chunk_provider: TestChunkStore::new(parts),
        };

        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32);
        writer.append_array(0, array).await?;
        writer.append_null().await?;
        writer.finalize().await?;

        let pruning = PruningArrayReader::new(store.clone(), path.child("pruning.arrow")).await?;
        let min_array = pruning.min_array()?;
        let min = min_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 min array");
        let max_array = pruning.max_array()?;
        let max = max_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 max array");
        let null_count_array = pruning.null_count_array()?;
        let null_count = null_count_array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("null count array");
        let row_count_array = pruning.row_count_array()?;
        let row_count = row_count_array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row count array");

        assert_eq!(min.len(), 3);
        assert_eq!(max.len(), 3);
        assert_eq!(row_count.len(), 3);
        assert_eq!(row_count.value(0), 2);
        assert_eq!(row_count.value(1), 2);
        assert_eq!(row_count.value(2), 0);
        assert_eq!(null_count.value(0), 0);
        assert_eq!(null_count.value(1), 0);
        assert_eq!(null_count.value(2), 0);
        assert_eq!(min.value(0), 1);
        assert_eq!(min.value(1), 3);
        assert_eq!(max.value(0), 2);
        assert_eq!(max.value(1), 4);
        assert!(min.is_null(2));
        assert!(max.is_null(2));

        Ok(())
    }
}
