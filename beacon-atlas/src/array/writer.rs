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
