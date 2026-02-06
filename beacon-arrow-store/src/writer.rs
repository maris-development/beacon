use std::{
    collections::VecDeque,
    io::{Read, Seek, SeekFrom},
};

use anyhow::Context;
use arrow::{
    compute::concat_batches, datatypes::SchemaRef, ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::ObjectStore;
use tempfile::tempfile;

const STREAM_CHUNK_SIZE: usize = 1024 * 1024;

/// Writes Arrow IPC files to an [`ObjectStore`] using a temporary file.
///
/// Batches are written to a local temp file first. On `finish`, the temp file
/// is streamed to the object store in 1 MB chunks using multipart upload.
pub struct ArrowObjectStoreWriter<S: ObjectStore + Send + Sync> {
    store: S,
    path: object_store::path::Path,
    schema: SchemaRef,
    writer: FileWriter<std::fs::File>,
    batch_size: usize,
    pending: VecDeque<RecordBatch>,
    pending_rows: usize,
}

impl<S: ObjectStore + Send + Sync> ArrowObjectStoreWriter<S> {
    /// Creates a new writer that buffers IPC output in a temporary file.
    ///
    /// # Errors
    /// Returns an error if the temp file cannot be created or if the IPC writer
    /// fails to initialize.
    pub fn new(
        store: S,
        path: object_store::path::Path,
        schema: SchemaRef,
        batch_size: usize,
    ) -> anyhow::Result<Self> {
        if batch_size == 0 {
            return Err(anyhow::anyhow!("batch_size must be greater than 0"));
        }
        let temp = tempfile().context("failed to create temporary file")?;
        let writer = FileWriter::try_new(temp, &schema)
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to create IPC writer")?;

        Ok(Self {
            store,
            path,
            schema,
            writer,
            batch_size,
            pending: VecDeque::new(),
            pending_rows: 0,
        })
    }

    /// Returns the IPC schema for this writer.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the object store path for this writer.
    pub fn path(&self) -> &object_store::path::Path {
        &self.path
    }

    /// Writes a record batch to the temporary IPC file.
    ///
    /// # Errors
    /// Returns an error if the record batch cannot be written.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.pending.push_back(batch.clone());
        self.pending_rows += batch.num_rows();

        while self.pending_rows >= self.batch_size {
            let aligned = self.next_aligned_batch()?;
            self.writer
                .write(&aligned)
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to write IPC record batch")?;
        }

        Ok(())
    }

    fn next_aligned_batch(&mut self) -> anyhow::Result<RecordBatch> {
        let mut remaining = self.batch_size;
        let mut parts: Vec<RecordBatch> = Vec::new();

        while remaining > 0 {
            let Some(batch) = self.pending.pop_front() else {
                break;
            };
            let rows = batch.num_rows();
            if rows <= remaining {
                remaining -= rows;
                self.pending_rows -= rows;
                parts.push(batch);
            } else {
                let take = batch.slice(0, remaining);
                let rest = batch.slice(remaining, rows - remaining);
                self.pending_rows -= remaining;
                parts.push(take);
                self.pending.push_front(rest);
                remaining = 0;
            }
        }

        if parts.is_empty() {
            return Err(anyhow::anyhow!("no pending batches available"));
        }

        if parts.len() == 1 {
            Ok(parts.remove(0))
        } else {
            concat_batches(&self.schema, &parts)
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to concatenate record batches")
        }
    }

    /// Finalizes the IPC file and streams it to the object store in 1 MB chunks.
    ///
    /// Returns the underlying store for further access (e.g. tests).
    ///
    /// # Errors
    /// Returns an error if the IPC file cannot be finalized or if streaming fails.
    pub async fn finish(mut self) -> anyhow::Result<S> {
        if self.pending_rows > 0 {
            let parts: Vec<RecordBatch> = self.pending.drain(..).collect();
            let pending_batch = if parts.len() == 1 {
                parts.into_iter().next().unwrap()
            } else {
                concat_batches(&self.schema, &parts)
                    .map_err(|err| anyhow::anyhow!(err))
                    .context("failed to concatenate final record batch")?
            };
            self.writer
                .write(&pending_batch)
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to write final IPC record batch")?;
        }

        self.writer
            .finish()
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to finalize IPC writer")?;

        let mut file = self
            .writer
            .into_inner()
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to extract temp file")?;

        file.seek(SeekFrom::Start(0))
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to rewind temp file")?;

        let mut uploader = self
            .store
            .put_multipart(&self.path)
            .await
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to start multipart upload")?;

        let mut buffer = vec![0u8; STREAM_CHUNK_SIZE];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to read temp file")?;
            if read == 0 {
                break;
            }
            let bytes = Bytes::copy_from_slice(&buffer[..read]);
            uploader
                .put_part(bytes.into())
                .await
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to upload multipart chunk")?;
        }

        uploader
            .complete()
            .await
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to complete multipart upload")?;

        Ok(self.store)
    }
}

#[cfg(test)]
mod tests {
    use super::ArrowObjectStoreWriter;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        ipc::reader::FileReader,
        record_batch::RecordBatch,
    };
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::{io::Cursor, sync::Arc};

    fn make_batch() -> anyhow::Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .map_err(|err| anyhow::anyhow!(err))
    }

    #[tokio::test]
    async fn writes_to_object_store_in_chunks() -> anyhow::Result<()> {
        let batch = make_batch()?;
        let store = InMemory::new();
        let path = Path::from("data.arrow");

        let mut writer = ArrowObjectStoreWriter::new(store, path.clone(), batch.schema(), 1024)?;
        writer.write_batch(&batch)?;
        let store = writer.finish().await?;

        let size = store.head(&path).await?.size;
        let data = store.get_range(&path, 0..size).await?;
        let reader = FileReader::try_new(Cursor::new(data), None)?;
        let read_batch = reader.into_iter().next().expect("batch exists")?;

        assert_eq!(read_batch.num_columns(), 2);
        assert_eq!(read_batch.num_rows(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn aligns_batches_to_batch_size() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
                Arc::new(Int32Array::from(vec![8, 9, 10, 11, 12, 13, 14])),
            ],
        )
        .map_err(|err| anyhow::anyhow!(err))?;

        let store = InMemory::new();
        let path = Path::from("aligned.arrow");

        let mut writer = ArrowObjectStoreWriter::new(store, path.clone(), schema, 3)?;
        writer.write_batch(&batch)?;
        let store = writer.finish().await?;

        let size = store.head(&path).await?.size;
        let data = store.get_range(&path, 0..size).await?;
        let reader = FileReader::try_new(Cursor::new(data), None)?;
        let sizes: Vec<usize> = reader
            .into_iter()
            .map(|batch| batch.map(|b| b.num_rows()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow::anyhow!(err))?;

        assert_eq!(sizes, vec![3, 3, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn aligns_across_multiple_input_batches() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch_a = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .map_err(|err| anyhow::anyhow!(err))?;

        let batch_b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![5, 6, 7, 8])),
                Arc::new(Int32Array::from(vec![9, 10, 11, 12])),
            ],
        )
        .map_err(|err| anyhow::anyhow!(err))?;

        let store = InMemory::new();
        let path = Path::from("aligned_multi.arrow");

        let mut writer = ArrowObjectStoreWriter::new(store, path.clone(), schema, 3)?;
        writer.write_batch(&batch_a)?;
        writer.write_batch(&batch_b)?;
        let store = writer.finish().await?;

        let size = store.head(&path).await?.size;
        let data = store.get_range(&path, 0..size).await?;
        let reader = FileReader::try_new(Cursor::new(data), None)?;
        let sizes: Vec<usize> = reader
            .into_iter()
            .map(|batch| batch.map(|b| b.num_rows()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow::anyhow!(err))?;

        assert_eq!(sizes, vec![3, 3]);
        Ok(())
    }

    #[test]
    fn rejects_zero_batch_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let store = InMemory::new();
        let path = Path::from("invalid.arrow");

        let err = ArrowObjectStoreWriter::new(store, path, schema, 0)
            .err()
            .expect("expected error");
        assert!(
            err.to_string()
                .contains("batch_size must be greater than 0")
        );
    }
}
