//! Chunked array implementations for Beacon Atlas.
//!
//! This module provides a streaming writer for ND Arrow arrays that are
//! chunked and uploaded to object storage.
//!

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use arrow::ipc::writer::FileWriter;
use beacon_nd_arrow::{NdArrowArray, extension::nd_column_data_type};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::BoxStream};
use object_store::ObjectStore;

use crate::{
    IPC_WRITE_OPTS,
    arrow_object_store::ArrowObjectStoreReader,
    layout::{ArrayLayouts, DatasetArrayLayout},
};

pub trait ChunkedArrayProvider: Send + Sync {
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ChunkedArrayPart>>;
    fn fetch_chunk(
        &self,
        chunk_index: Vec<usize>,
    ) -> BoxFuture<'static, anyhow::Result<Option<ChunkedArrayPart>>>;
}

impl<T> ChunkedArrayProvider for Arc<T>
where
    T: ChunkedArrayProvider + ?Sized,
{
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ChunkedArrayPart>> {
        (**self).chunks()
    }

    fn fetch_chunk(
        &self,
        chunk_index: Vec<usize>,
    ) -> BoxFuture<'static, anyhow::Result<Option<ChunkedArrayPart>>> {
        (**self).fetch_chunk(chunk_index)
    }
}

struct LazyChunkedArrayProvider<S: ObjectStore + Send + Sync> {
    layout: Arc<DatasetArrayLayout>,
    reader: Arc<ArrowObjectStoreReader<S>>,
}

impl<S: ObjectStore + Send + Sync> ChunkedArrayProvider for LazyChunkedArrayProvider<S> {
    fn chunks(&self) -> BoxStream<'static, anyhow::Result<ChunkedArrayPart>> {
        unimplemented!()
    }

    fn fetch_chunk(
        &self,
        chunk_index: Vec<usize>,
    ) -> BoxFuture<'static, anyhow::Result<Option<ChunkedArrayPart>>> {
        // Find the index of the chunk in the layout
        let layout = self.layout.clone();

        let fut = async move {
            let index = layout.chunk_indexes.iter().position(|idxs| {
                idxs.iter()
                    .zip(chunk_index.iter())
                    .all(|(a, b)| *a as usize == *b)
            });

            if let Some(index) = index {
                let array_index = layout.array_indexes[index];
                let batch_index = array_index[0] as usize;
                let array_in_batch_index = array_index[1] as usize;

                let batch = self.reader.read_batch(batch_index).await?;
                if let Some(batch) = batch {
                    let column = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<beacon_nd_arrow::column::NdArrowArrayColumn>()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Failed to downcast to NdArrowArrayColumn")
                        })?;

                    let arrays = column.to_rows()?;
                    if array_in_batch_index < arrays.len() {
                        let array = arrays[array_in_batch_index].clone();
                        return Ok(Some(ChunkedArrayPart { array, chunk_index }));
                    } else {
                        return Ok(None);
                    }
                }
            }

            Ok(None)
        };

        fut.boxed()
    }
}

/// A stream of chunked ND arrays with a shared element type and chunk shape.
pub struct ChunkedArray<S: ChunkedArrayProvider + Send + Sync> {
    pub array_datatype: arrow::datatypes::DataType,
    pub chunk_shape: Vec<usize>,
    pub chunk_provider: S,
}

impl<S: ChunkedArrayProvider + Send + Sync> ChunkedArray<S> {
    /// Returns the next chunked array part from the stream.
    pub fn chunks(&self) -> BoxStream<'static, anyhow::Result<ChunkedArrayPart>> {
        self.chunk_provider.chunks()
    }
}

/// A single chunk and its chunk index within the overall array.
pub struct ChunkedArrayPart {
    pub array: NdArrowArray,
    pub chunk_index: Vec<usize>,
}

/// Writes chunked ND Arrow arrays to object storage using buffered IPC batches.
pub struct ChunkedArrayWriter<S: ObjectStore> {
    array_datatype: arrow::datatypes::DataType,
    store: S,
    path: object_store::path::Path,
    temp_writer: FileWriter<File>,
    num_batches: usize,
    flush_size: usize, // Number of bytes of all the arrays to buffer before flushing to a batch and thus disk
    current_buffer_size: usize, // Current number of bytes buffered
    buffer_arrays: Vec<NdArrowArray>,

    // Layouts of the arrays written so far.
    layouts: Vec<DatasetArrayLayout>,
}

impl<S: ObjectStore> ChunkedArrayWriter<S> {
    const DEFAULT_FLUSH_SIZE: usize = 8 * 1024 * 1024; // 8 MB
    const STREAM_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

    /// Create a new chunked array writer.
    pub fn new(
        array_datatype: arrow::datatypes::DataType,
        store: S,
        path: object_store::path::Path,
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

    /// Flush remaining batches and upload the IPC file to object storage.
    pub async fn finalize(mut self) -> anyhow::Result<()> {
        // Flush any remaining data
        self.flush().await?;

        self.temp_writer.finish()?;

        // Upload the temp file to object store
        let mut temp_file = self.temp_writer.into_inner()?;
        temp_file.seek(SeekFrom::Start(0))?;

        Self::stream_file_to_store(&self.store, &self.path.child("array.arrow"), &mut temp_file)
            .await?;

        // Create a layout file
        let layout = ArrayLayouts::new(self.layouts);
        let layout_path = self.path.child("layout.arrow");
        layout.save::<S>(self.store, layout_path).await?;

        Ok(())
    }

    /// Append a stream of chunked arrays for a dataset index.
    pub async fn append_chunked_array<C: ChunkedArrayProvider + Send + Sync>(
        &mut self,
        dataset_index: u32,
        array: ChunkedArray<C>,
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

        let mut chunks = array.chunks();
        while let Some(part) = chunks.next().await {
            let part = part?;
            let array_indice = [self.num_batches as u32, self.buffer_arrays.len() as u32];

            self.current_buffer_size += part.array.values().get_array_memory_size();
            self.buffer_arrays.push(part.array);

            // Check if we need to flush
            if self.current_buffer_size >= self.flush_size {
                self.flush().await?;
            }

            file_array_indices.push(array_indice);
            chunk_indices.push(part.chunk_index.iter().map(|d| *d as u32).collect());
        }

        self.layouts.push(DatasetArrayLayout {
            dataset_index,
            chunk_shape,
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

    /// Stream a file to object storage using multipart uploads.
    async fn stream_file_to_store(
        store: &S,
        path: &object_store::path::Path,
        file: &mut File,
    ) -> anyhow::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; Self::STREAM_CHUNK_SIZE];
        let mut total_read = 0usize;
        let mut uploader = store.put_multipart(path).await?;

        loop {
            let read = file.read(&mut buf)?;
            if read == 0 {
                break;
            }
            total_read += read;
            uploader
                .put_part(Bytes::copy_from_slice(&buf[..read]).into())
                .await?;
        }

        if total_read == 0 {
            store.put(path, Bytes::new().into()).await?;
        } else {
            uploader.complete().await?;
        }
        Ok(())
    }
}

pub struct ChunkedArrayReader<S: ObjectStore + Clone> {
    store: S,
    path: object_store::path::Path,
    array_reader: Arc<ArrowObjectStoreReader<S>>,
    layouts: ArrayLayouts,
    array_datatype: arrow::datatypes::DataType,
}

impl<S: ObjectStore + Clone> ChunkedArrayReader<S> {
    /// Create a new chunked array reader.
    pub async fn new(store: S, path: object_store::path::Path) -> anyhow::Result<Self> {
        let array_path = path.child("array.arrow");
        let layout_path = path.child("layout.arrow");
        let array_reader = Arc::new(ArrowObjectStoreReader::new(store.clone(), array_path).await?);

        let array_field = array_reader.schema();
        let array_datatype = beacon_nd_arrow::extension::nd_column_data_type(
            array_field.field(0).data_type().clone(),
        );

        Ok(Self {
            store: store.clone(),
            path,
            array_reader,
            layouts: ArrayLayouts::from_object(store.clone(), layout_path).await?,
            array_datatype,
        })
    }

    pub fn layouts(&self) -> &ArrayLayouts {
        &self.layouts
    }

    pub fn array_datatype(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    pub fn read_dataset_array(
        &self,
        dataset_index: u32,
    ) -> anyhow::Result<ChunkedArray<Arc<dyn ChunkedArrayProvider>>> {
        let layout = self
            .layouts
            .find_dataset_array_layout(dataset_index)
            .ok_or_else(|| anyhow::anyhow!("Dataset index {} not found in layouts", dataset_index))?
            .clone();

        let provider = Arc::new(LazyChunkedArrayProvider {
            layout: Arc::new(layout),
            reader: self.array_reader.clone(),
        });

        Ok(ChunkedArray {
            array_datatype: self.array_datatype.clone(),
            chunk_shape: provider
                .layout
                .chunk_shape
                .iter()
                .map(|d| *d as usize)
                .collect(),
            chunk_provider: provider,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;
    use arrow::ipc::reader::FileReader;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::{ChunkedArray, ChunkedArrayPart, ChunkedArrayWriter};

    fn make_part(values: Vec<i32>, chunk_index: Vec<usize>) -> ChunkedArrayPart {
        let dims = Dimensions::new(vec![Dimension::try_new("x", values.len()).unwrap()]);
        let array =
            beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(values)), dims).unwrap();
        ChunkedArrayPart { array, chunk_index }
    }

    #[tokio::test]
    async fn chunked_array_writer_streams_ipc_to_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("chunked/arrays.arrow");
        let mut writer = ChunkedArrayWriter::new(DataType::Int32, store.clone(), path.clone());

        let chunks = stream::iter(vec![
            make_part(vec![1, 2], vec![0]),
            make_part(vec![3, 4], vec![1]),
        ]);
        let chunked = ChunkedArray {
            array_datatype: DataType::Int32,
            chunk_shape: vec![2],
            chunk_provider: Box::pin(chunks),
        };

        writer.append_chunked_array(0, chunked).await.unwrap();
        writer.finalize().await.unwrap();

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let reader = FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None).unwrap();
        let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
    }
}
