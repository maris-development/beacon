use std::sync::Arc;

use anyhow::Context;
use arrow::{
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{
        Block,
        convert::fb_to_schema,
        reader::{FileDecoder, read_footer_length},
        root_as_footer,
    },
};
use object_store::ObjectStore;

/// Reads Arrow IPC files from an [`ObjectStore`] using range requests.
///
/// This reader only loads the IPC footer and dictionaries during construction,
/// and fetches record batch blocks on demand.
#[derive(Debug)]
pub struct ArrowObjectStoreReader<S: ObjectStore + Send + Sync> {
    store: S,
    path: object_store::path::Path,
    decoder: arrow::ipc::reader::FileDecoder,
    schema: Arc<arrow::datatypes::Schema>,
    batches: Vec<Block>,
}

impl<S: ObjectStore + Send + Sync> ArrowObjectStoreReader<S> {
    /// Creates a new reader by loading the IPC footer and dictionaries.
    ///
    /// # Errors
    /// Returns an error when the object is too small, has an invalid footer,
    /// or when range reads fail.
    pub async fn new(store: S, path: object_store::path::Path) -> anyhow::Result<Self> {
        let size = store
            .head(&path)
            .await
            .map_err(|err| anyhow::anyhow!(err))?
            .size;

        if size < 10 {
            return Err(anyhow::anyhow!("IPC file too small for footer"));
        }

        let size_usize = usize::try_from(size).map_err(|err| anyhow::anyhow!(err))?;
        let trailer_start = size_usize - 10;
        let trailer_start_u64 = u64::try_from(trailer_start).map_err(|err| anyhow::anyhow!(err))?;

        // Read the 10-byte IPC trailer with the footer length and magic.
        let trailer_bytes = store
            .get_range(&path, trailer_start_u64..size)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let trailer: [u8; 10] = trailer_bytes
            .as_ref()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid IPC trailer length"))?;

        let footer_len = read_footer_length(trailer).map_err(|err| anyhow::anyhow!(err))?;
        let footer_start = trailer_start
            .checked_sub(footer_len)
            .ok_or_else(|| anyhow::anyhow!("IPC footer length underflow"))?;
        if footer_start > trailer_start {
            return Err(anyhow::anyhow!("IPC footer length overflow"));
        }
        let footer_start_u64 = u64::try_from(footer_start).map_err(|err| anyhow::anyhow!(err))?;
        // Read the footer using the length from the trailer.
        let footer_bytes = store
            .get_range(&path, footer_start_u64..trailer_start_u64)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let footer = root_as_footer(footer_bytes.as_ref())
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to parse IPC footer")?;
        let schema: SchemaRef = fb_to_schema(
            footer
                .schema()
                .ok_or_else(|| anyhow::anyhow!("missing IPC schema"))?,
        )
        .into();

        // Build a decoder without reading dictionaries yet.
        let mut decoder = FileDecoder::new(schema.clone(), footer.version());
        let dictionary_blocks: Vec<Block> = footer
            .dictionaries()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        // Read all dictionary blocks up-front so record batch reads are ready.
        for block in &dictionary_blocks {
            let block_len = (block.bodyLength() as usize) + (block.metaDataLength() as usize);
            let offset = block.offset() as u64;
            let block_len_u64 = u64::try_from(block_len).map_err(|err| anyhow::anyhow!(err))?;
            let data = store
                .get_range(&path, offset..(offset + block_len_u64))
                .await
                .map_err(|err: object_store::Error| anyhow::anyhow!(err))
                .context("failed to read dictionary block")?;
            let buffer = Buffer::from(data);
            decoder
                .read_dictionary(block, &buffer)
                .map_err(|err| anyhow::anyhow!(err))
                .context("failed to decode dictionary")?;
        }

        Ok(Self {
            store,
            path,
            decoder,
            schema: schema.clone(),
            batches: footer
                .recordBatches()
                .map(|b| b.iter().copied().collect())
                .unwrap_or_default(),
        })
    }

    /// Creates a reader that projects the given column indices.
    ///
    /// # Errors
    /// Returns an error if the projection is out of bounds.
    pub fn with_projection(self, projection: Vec<usize>) -> anyhow::Result<Self> {
        let projected_schema = self.schema.project(&projection)?;
        Ok(Self {
            store: self.store,
            batches: self.batches,
            decoder: self.decoder.with_projection(projection),
            path: self.path,
            schema: projected_schema.into(),
        })
    }

    /// Returns the number of record batches in this reader.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Returns the IPC schema for this reader.
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    /// Returns the object store path for this reader.
    pub fn path(&self) -> &object_store::path::Path {
        &self.path
    }

    /// Reads a record batch by index.
    ///
    /// Returns `Ok(None)` when the index is out of range.
    ///
    /// # Errors
    /// Returns an error when the block cannot be fetched or decoded.
    pub async fn read_batch(
        &self,
        index: usize,
    ) -> anyhow::Result<Option<arrow::record_batch::RecordBatch>> {
        let batch_block = match self.batches.get(index) {
            Some(block) => block,
            None => return Ok(None),
        };

        let block_len =
            (batch_block.bodyLength() as usize) + (batch_block.metaDataLength() as usize);
        let offset = batch_block.offset() as u64;
        let block_len_u64 = u64::try_from(block_len).map_err(|err| anyhow::anyhow!(err))?;
        let data = self
            .store
            .get_range(&self.path, offset..(offset + block_len_u64))
            .await
            .map_err(|err: object_store::Error| anyhow::anyhow!(err))
            .context("failed to read record batch block")?;
        let buffer = Buffer::from(data);
        let batch = self
            .decoder
            .read_record_batch(batch_block, &buffer)
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to decode record batch")?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::ArrowObjectStoreReader;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        ipc::writer::FileWriter,
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::{io::Cursor, sync::Arc};

    fn make_ipc_bytes() -> anyhow::Result<Vec<u8>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?;

        let mut cursor = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new(&mut cursor, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok(cursor.into_inner())
    }

    #[tokio::test]
    async fn reads_schema_and_batch() -> anyhow::Result<()> {
        let bytes = make_ipc_bytes()?;
        let store = InMemory::new();
        let path = Path::from("data.arrow");
        store.put(&path, Bytes::from(bytes).into()).await?;

        let reader = ArrowObjectStoreReader::new(store, path).await?;
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);

        let batch = reader.read_batch(0).await?.expect("batch exists");
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn projection_limits_columns() -> anyhow::Result<()> {
        let bytes = make_ipc_bytes()?;
        let store = InMemory::new();
        let path = Path::from("data.arrow");
        store.put(&path, Bytes::from(bytes).into()).await?;

        let reader = ArrowObjectStoreReader::new(store, path).await?;
        let projected = reader.with_projection(vec![1])?;
        let schema = projected.schema();
        assert_eq!(schema.fields().len(), 1);

        let batch = projected.read_batch(0).await?.expect("batch exists");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_too_small_files() {
        let store = InMemory::new();
        let path = Path::from("small.arrow");
        store
            .put(&path, Bytes::from_static(&[0u8; 5]).into())
            .await
            .expect("put bytes");

        let err = ArrowObjectStoreReader::new(store, path).await.unwrap_err();
        assert!(err.to_string().contains("IPC file too small"));
    }
}
