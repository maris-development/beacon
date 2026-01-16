use std::{io::Read, sync::Arc};

use beacon_nd_arrow::NdArrowArray;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};
use tempfile::tempfile;

use crate::chunked::array::{CHUNKED_ARRAY_FILE_NAME, util};

pub struct ChunkedArrayWriter<S: ObjectStore> {
    // fields and methods would go here
    pub store: Arc<S>,
    pub array_path: object_store::path::Path,
    pub data_type: arrow::datatypes::DataType,
    pub chunk_shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub array_shape: Vec<usize>,
    pub temp_array_writer: arrow_ipc::writer::FileWriter<std::fs::File>,
    pub temp_array_chunk_index_order: Vec<usize>,
}

impl<S: ObjectStore> ChunkedArrayWriter<S> {
    pub fn new(
        store: Arc<S>,
        variable_path: object_store::path::Path,
        array_type: arrow::datatypes::DataType,
        chunk_shape: Vec<usize>,
        dimensions: Vec<String>,
        array_shape: Vec<usize>,
    ) -> Self {
        let array_path = variable_path.child(CHUNKED_ARRAY_FILE_NAME);
        let temp_file = tempfile().expect("failed to create temp file for chunked array");
        let field = arrow::datatypes::Field::new("data", array_type.clone(), true);
        let schema = arrow::datatypes::Schema::new(vec![field]);
        let temp_array_writer = arrow_ipc::writer::FileWriter::try_new(temp_file, &schema)
            .expect("failed to create temp arrow ipc writer for chunked array");
        Self {
            store,
            array_path,
            chunk_shape,
            dimensions,
            array_shape,
            temp_array_writer,
            data_type: array_type,
            temp_array_chunk_index_order: Vec::new(),
        }
    }

    pub async fn write_chunk(
        &mut self,
        chunk_index: Vec<usize>,
        array: NdArrowArray,
    ) -> object_store::Result<()> {
        let single_value_chunk_index =
            util::chunk_index_to_id(&self.array_shape, &self.chunk_shape, &chunk_index);
        let values_array = array.values();
        // Implementation for writing the chunked array to the object store
        let chunk_batch = arrow::record_batch::RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "data",
                self.data_type.clone(),
                true,
            )])
            .into(),
            vec![values_array.clone()],
        )
        .unwrap();

        self.temp_array_writer
            .write(&chunk_batch)
            .expect("failed to write chunk batch to temp arrow ipc writer");

        self.temp_array_chunk_index_order
            .push(single_value_chunk_index);

        Ok(())
    }

    pub async fn finalize(mut self) -> object_store::Result<()> {
        // Rewrite the temp arrow ipc file to the object store in chunk index order
        self.temp_array_writer
            .finish()
            .expect("failed to finish temp arrow ipc writer");
        let file = self.temp_array_writer.into_inner().unwrap();

        let mut arrow_reader = arrow_ipc::reader::FileReader::try_new(file, None).unwrap();

        // Create a new arrow ipc writer to write ordered chunks
        let temp_file_ordered =
            tempfile().expect("failed to create temp file for ordered chunked array");
        let mut ordered_array_writer =
            arrow_ipc::writer::FileWriter::try_new(temp_file_ordered, &arrow_reader.schema())
                .expect("failed to create temp arrow ipc writer for ordered chunked array");

        // Write chunks to object store in correct order
        let mut ordered_index = vec![];
        for (i, index) in self.temp_array_chunk_index_order.iter().enumerate() {
            ordered_index.push((i, *index));
        }
        // Sort by chunk index
        ordered_index.sort_by_key(|&(_, index)| index);

        // Write each chunk in order
        for (i, _) in ordered_index {
            arrow_reader.set_index(i).unwrap();
            let batch = arrow_reader
                .next()
                .expect("failed to get record batch from arrow reader")
                .expect("no more record batches");
            ordered_array_writer
                .write(&batch)
                .expect("failed to write ordered batch to temp arrow ipc writer");
        }

        // Finalize the ordered writer
        ordered_array_writer
            .finish()
            .expect("failed to finish ordered arrow ipc writer");

        // Upload the ordered arrow ipc file to the object store using multipart upload
        let mut multi_part_upload = self.store.put_multipart(&self.array_path).await.unwrap();

        // Read the ordered temp file in 1MB parts and upload
        let ordered_file = ordered_array_writer.into_inner().unwrap();
        let mut reader = std::io::BufReader::new(ordered_file);
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let n = reader.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            multi_part_upload
                .put_part(PutPayload::from_bytes(Bytes::copy_from_slice(&buffer[..n])))
                .await
                .unwrap();
        }

        Ok(())
    }
}
