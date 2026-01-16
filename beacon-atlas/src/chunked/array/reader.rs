use std::sync::Arc;

use arrow_ipc::{
    Block,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
    root_as_footer,
};
use object_store::ObjectStore;

use crate::chunked::array::CHUNKED_ARRAY_FILE_NAME;

pub struct ChunkedArrayReader<S: ObjectStore> {
    pub ipc_decoder: ArrowStoreReader<S>,
    pub array_shape: Vec<usize>,
    pub chunk_shape: Vec<usize>,
    pub data_type: arrow::datatypes::DataType,
}

impl<S: ObjectStore> ChunkedArrayReader<S> {
    pub fn new(
        store: std::sync::Arc<S>,
        variable_path: object_store::path::Path,
        array_shape: Vec<usize>,
        chunk_shape: Vec<usize>,
        data_type: arrow::datatypes::DataType,
    ) -> Self {
        let array_path = variable_path.child(CHUNKED_ARRAY_FILE_NAME);

        Self {
            store,
            array_path,
            array_shape,
            chunk_shape,
            data_type,
        }
    }
}

struct ArrowStoreReader<S: ObjectStore> {
    store: std::sync::Arc<S>,
    /// Decoder that reads Arrays that refers to the underlying buffers
    decoder: FileDecoder,
    /// Location of the batches within the buffer
    batches: Vec<Block>,
}

impl<S: ObjectStore> ArrowStoreReader<S> {
    pub const PREFETCH_LEN: u64 = 256 * 1024; // 256 KB
    async fn new(store: Arc<S>, path: object_store::path::Path) -> Self {
        let object_meta = store.head(&path).await.unwrap();
        let size = object_meta.size as usize;
        let prefetch_start = if size > Self::PREFETCH_LEN as usize {
            size - Self::PREFETCH_LEN as usize
        } else {
            0
        };

        let prefetch_buffer = store
            .get_range(&path, prefetch_start as u64..size as u64)
            .await
            .unwrap();

        let trailer_start = prefetch_buffer.len() - 10;
        let footer_len =
            read_footer_length(prefetch_buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer =
            root_as_footer(&prefetch_buffer[trailer_start - footer_len..trailer_start]).unwrap();

        let schema = fb_to_schema(footer.schema().unwrap());

        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());

        // Read dictionaries
        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);
            decoder.read_dictionary(block, &data).unwrap();
        }

        // convert to Vec from the flatbuffers Vector to avoid having a direct dependency on flatbuffers
        let batches = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        Self {
            buffer,
            decoder,
            batches,
        }
    }

    /// Return the number of [`RecordBatch`]es in this buffer
    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Return the [`RecordBatch`] at message index `i`.
    ///
    /// This may return `None` if the IPC message was None
    fn get_batch(&self, i: usize) -> Result<Option<RecordBatch>> {
        let block = &self.batches[i];
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = self
            .buffer
            .slice_with_length(block.offset() as _, block_len);
        self.decoder.read_record_batch(block, &data)
    }
}
