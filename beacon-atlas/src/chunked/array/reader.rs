use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_ipc::{
    Block,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
    root_as_footer,
};
use bytes::BytesMut;
use object_store::ObjectStore;

use crate::chunked::array::CHUNKED_ARRAY_FILE_NAME;

pub struct ChunkedArrayReader<S: ObjectStore> {
    pub ipc_decoder: ArrowStoreReader<S>,
    pub array_path: object_store::path::Path,
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

        // Self {
        //     store,
        //     array_path,
        //     array_shape,
        //     chunk_shape,
        //     data_type,
        // }
        todo!()
    }
}

struct ArrowStoreReader<S: ObjectStore> {
    store: std::sync::Arc<S>,
    array_path: object_store::path::Path,
    /// Decoder that reads Arrays that refers to the underlying buffers
    decoder: parking_lot::Mutex<FileDecoder>,
    /// Location of the batches within the buffer
    batches: Vec<Block>,
    /// Dictionaries Blocks
    dictionary_batches: Option<Vec<Block>>,
}

impl<S: ObjectStore> ArrowStoreReader<S> {
    pub const PREFETCH_LEN: u64 = 256 * 1024; // 256 KB
    async fn new(store: Arc<S>, array_path: object_store::path::Path) -> Self {
        let object_meta = store.head(&array_path).await.unwrap();

        let trailer_start = object_meta.size - 10;
        let footer_len_buf = store
            .get_range(&array_path, trailer_start..object_meta.size)
            .await
            .unwrap();
        let footer_len = read_footer_length(footer_len_buf.as_ref().try_into().unwrap()).unwrap();

        let footer_start = trailer_start - footer_len as u64;
        let footer_buffer = store
            .get_range(&array_path, footer_start..object_meta.size)
            .await
            .unwrap();

        let footer = root_as_footer(&footer_buffer[..]).unwrap();

        let schema = fb_to_schema(footer.schema().unwrap());

        let decoder =
            FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(false);

        // Read dictionaries and record batch blocks
        let (dictionary_blocks, batch_blocks): (_, Vec<Block>) =
            match (footer.dictionaries(), footer.recordBatches()) {
                (Some(dict_blocks), Some(batch_blocks)) => {
                    let dicts = dict_blocks.iter().cloned().collect();
                    let batches = batch_blocks.iter().cloned().collect();
                    (Some(dicts), batches)
                }
                (None, Some(batch_blocks)) => (None, batch_blocks.iter().cloned().collect()),
                _ => (None, Vec::new()),
            };

        Self {
            array_path,
            store,
            decoder: parking_lot::Mutex::new(decoder),
            batches: batch_blocks,
            dictionary_batches: dictionary_blocks,
        }
    }

    /// Return the number of [`RecordBatch`]es in this buffer
    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Return the [`RecordBatch`] at message index `i`.
    ///
    /// This may return `None` if the IPC message was None
    async fn get_batch(&self, i: usize) -> Result<Option<RecordBatch>, object_store::Error> {
        if self.batches.len() <= i {
            return Ok(None);
        }

        // Init dictionary decoder if necessary
        if let Some(dict_blocks) = &self.dictionary_batches {
            let dict_block = dict_blocks.get(i).unwrap();
            let range = dict_block.offset() as u64
                ..(dict_block.offset()
                    + dict_block.metaDataLength() as i64
                    + dict_block.bodyLength()) as u64;
            let dict_buffer = self.store.get_range(&self.array_path, range).await?;
            let arrow_buffer = arrow::buffer::Buffer::from(dict_buffer);
            let decoder = self.decoder.lock();
            decoder
                .read_record_batch(dict_block, &arrow_buffer)
                .unwrap();
        }

        let batch_block = self.batches.get(i).unwrap();
        let range = batch_block.offset() as u64
            ..(batch_block.offset()
                + batch_block.metaDataLength() as i64
                + batch_block.bodyLength()) as u64;
        let batch_buffer = self.store.get_range(&self.array_path, range).await?;
        let arrow_buffer = arrow::buffer::Buffer::from(batch_buffer);
        let decoder = self.decoder.lock();
        let record_batch = decoder
            .read_record_batch(batch_block, &arrow_buffer)
            .unwrap();

        Ok(record_batch)
    }
}
