use std::{
    fs::File,
    io::{Seek, SeekFrom},
    sync::Arc,
};

use arrow::{array::ArrayRef, ipc::writer::FileWriter};
use futures::StreamExt;
use object_store::ObjectStore;

use crate::{
    IPC_WRITE_OPTS,
    array::{
        Array, ChunkStore,
        layout::{ArrayLayout, ArrayLayouts},
    },
    consts, util,
};

/// Writes chunked ND Arrow arrays to object storage using buffered IPC batches.
///
/// The writer stores array values in `array.arrow` and emits the corresponding
/// dataset layouts in `layout.arrow` within the provided object store path.
pub struct ArrayWriter<S: ObjectStore + Clone> {
    array_datatype: arrow::datatypes::DataType,
    store: S,
    array_path: object_store::path::Path,
    layout_path: object_store::path::Path,
    temp_writer: FileWriter<File>,
    row_count: usize,  // Total number of rows flushed to the IPC file
    chunk_size: usize, // Target number of rows per buffered batch

    current_buffer_size: usize,   // Current number of rows buffered
    buffer_arrays: Vec<ArrayRef>, // Buffered arrays to write in the next batch

    // Layouts of the arrays written so far.
    layouts: Vec<ArrayLayout>,
}

impl<S: ObjectStore + Clone> ArrayWriter<S> {
    /// Create a new chunked array writer.
    ///
    /// `chunk_size` controls when buffered arrays are flushed to the temp IPC file.
    pub fn new(
        store: S,
        array_path: object_store::path::Path,
        layout_path: object_store::path::Path,
        array_datatype: arrow::datatypes::DataType,
        chunk_size: usize,
    ) -> Self {
        let field = arrow::datatypes::Field::new("array", array_datatype.clone(), true);
        let schema = arrow::datatypes::Schema::new(vec![field]);
        let temp_writer = FileWriter::try_new_with_options(
            tempfile::tempfile().unwrap(),
            &schema,
            IPC_WRITE_OPTS.clone(),
        )
        .expect("Failed to create IPC file writer");

        Self {
            array_datatype,
            store,
            array_path,
            layout_path,
            temp_writer,
            chunk_size,
            row_count: 0,
            current_buffer_size: 0,
            buffer_arrays: Vec::new(),
            layouts: Vec::new(),
        }
    }

    pub fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    /// Flush remaining batches and upload the IPC file to object storage.
    ///
    /// Writes `array.arrow` (values) and `layout.arrow` (layout metadata)
    /// under the configured path.
    pub async fn finalize(mut self) -> anyhow::Result<()> {
        // Flush any remaining data
        self.flush().await?;

        self.temp_writer.finish()?;

        // Upload the temp file to object store
        let mut temp_file = self.temp_writer.into_inner()?;
        temp_file.seek(SeekFrom::Start(0))?;

        util::stream_file_to_store::<S>(
            &self.store,
            &self.array_path,
            &mut temp_file,
            consts::STREAM_CHUNK_SIZE,
        )
        .await?;

        // Create a layout file
        let layout = ArrayLayouts::new(self.layouts);
        layout.save::<S>(self.store, self.layout_path).await?;

        Ok(())
    }

    pub async fn append_null(&mut self) -> anyhow::Result<()> {
        // Placeholder for future support of null datasets.
        Ok(())
    }

    /// Append a stream of chunked arrays for a dataset index.
    ///
    /// The layout entry is recorded immediately using the current write offset,
    /// which includes buffered (not-yet-flushed) rows.
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

        let array_shape = array
            .array_shape
            .iter()
            .map(|d| *d as u32)
            .collect::<Vec<_>>();

        let array_start =
            self.row_count
                .checked_add(self.current_buffer_size)
                .ok_or_else(|| anyhow::anyhow!("array offset overflow"))? as u64;

        let mut chunks = array.as_chunked_arrow_stream();
        while let Some(part) = chunks.next().await {
            let part = part?;
            self.current_buffer_size += part.len();
            self.buffer_arrays.push(part);

            // Check if we need to flush
            if self.current_buffer_size >= self.chunk_size {
                self.flush().await?;
            }
        }

        let array_len = array.array_shape.iter().product::<usize>() as u64;

        self.layouts.push(ArrayLayout {
            dataset_index,
            dimensions: array.dimensions.clone(),
            array_len,
            array_shape,
            array_start,
        });

        Ok(())
    }

    /// Flush any remaining buffered data to the temp file.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        // If there is any buffered data, write it as a batch
        if !self.buffer_arrays.is_empty() {
            let column_arrays = std::mem::take(&mut self.buffer_arrays);
            // concatenate the buffered arrays into a single column array
            let column_array = arrow::compute::concat(
                &column_arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            )?;

            // Create the schema for the batch
            let field = arrow::datatypes::Field::new("array", self.array_datatype.clone(), true);

            let schema = arrow::datatypes::Schema::new(vec![field]);
            let batch =
                arrow::record_batch::RecordBatch::try_new(Arc::new(schema), vec![column_array])?;

            self.buffer_arrays.clear();

            // Write the batch to the temp file
            self.temp_writer.write(&batch)?;
            self.row_count = self
                .row_count
                .checked_add(batch.num_rows())
                .ok_or_else(|| anyhow::anyhow!("row count overflow"))?;
            self.current_buffer_size = 0;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayWriter;
    use crate::array::{Array, layout::ArrayLayouts, store::InMemoryChunkStore};
    use crate::arrow_object_store::ArrowObjectStoreReader;
    use arrow::array::{ArrayRef, Float32Array, Int32Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::DataType;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn chunk(values: Vec<i32>) -> ArrayRef {
        Arc::new(Int32Array::from(values)) as ArrayRef
    }

    fn chunk_f32(values: Vec<f32>) -> ArrayRef {
        Arc::new(Float32Array::from(values)) as ArrayRef
    }

    fn build_array(
        chunks: Vec<Vec<i32>>,
        shape: Vec<usize>,
        dims: Vec<&str>,
    ) -> Array<InMemoryChunkStore> {
        let arrays = chunks.into_iter().map(chunk).collect::<Vec<_>>();
        Array {
            array_datatype: DataType::Int32,
            array_shape: shape,
            dimensions: dims.into_iter().map(|d| d.to_string()).collect(),
            chunk_provider: InMemoryChunkStore::new(arrays),
        }
    }

    async fn read_all_rows(store: Arc<dyn ObjectStore>, path: &Path) -> anyhow::Result<Vec<i32>> {
        let reader = ArrowObjectStoreReader::new(store, path.clone()).await?;
        let mut batches = Vec::new();
        for idx in 0..reader.num_batches() {
            if let Some(batch) = reader.read_batch(idx).await? {
                batches.push(batch);
            }
        }
        let batch = concat_batches(&reader.schema(), &batches)?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        Ok((0..values.len()).map(|i| values.value(i)).collect())
    }

    #[tokio::test]
    async fn writes_array_and_layout() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("writer/basic");
        let array_path = path.child("array.arrow");
        let layout_path = path.child("layout.arrow");
        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            DataType::Int32,
            3,
        );

        let array = build_array(vec![vec![1, 2], vec![3, 4, 5]], vec![5], vec!["x"]);
        writer.append_array(7, array).await?;
        writer.finalize().await?;

        let values = read_all_rows(store.clone(), &array_path).await?;
        assert_eq!(values, vec![1, 2, 3, 4, 5]);

        let layout = ArrayLayouts::from_object(store.clone(), layout_path).await?;
        let entry = layout.find_dataset_array_layout(7).expect("layout entry");
        assert_eq!(entry.array_start, 0);
        assert_eq!(entry.array_len, 5);
        assert_eq!(entry.array_shape, vec![5]);
        assert_eq!(entry.dimensions, vec!["x".to_string()]);

        Ok(())
    }

    #[tokio::test]
    async fn tracks_offsets_with_buffered_rows() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("writer/buffered");
        let array_path = path.child("array.arrow");
        let layout_path = path.child("layout.arrow");
        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path,
            layout_path.clone(),
            DataType::Int32,
            10,
        );

        let first = build_array(vec![vec![1, 2, 3]], vec![3], vec!["x"]);
        let second = build_array(vec![vec![4, 5]], vec![2], vec!["x"]);

        writer.append_array(1, first).await?;
        writer.append_array(2, second).await?;
        writer.finalize().await?;

        let layout = ArrayLayouts::from_object(store.clone(), layout_path).await?;
        let first_entry = layout.find_dataset_array_layout(1).expect("first entry");
        let second_entry = layout.find_dataset_array_layout(2).expect("second entry");

        assert_eq!(first_entry.array_start, 0);
        assert_eq!(first_entry.array_len, 3);
        assert_eq!(second_entry.array_start, 3);
        assert_eq!(second_entry.array_len, 2);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_mismatched_datatype() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("writer/mismatch");
        let array_path = path.child("array.arrow");
        let layout_path = path.child("layout.arrow");
        let mut writer = ArrayWriter::new(store, array_path, layout_path, DataType::Int32, 4);

        let arrays = vec![chunk_f32(vec![1.0, 2.0])];
        let array = Array {
            array_datatype: DataType::Float32,
            array_shape: vec![2],
            dimensions: vec!["x".to_string()],
            chunk_provider: InMemoryChunkStore::new(arrays),
        };

        let err = writer
            .append_array(1, array)
            .await
            .expect_err("expected error");
        assert!(err.to_string().contains("datatype"));
        Ok(())
    }
}
