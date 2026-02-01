//! Chunked array implementations for Beacon Atlas.
//!
//!
//!
//!
//!
//!

use std::fs::File;

use arrow::ipc::writer::FileWriter;
use beacon_nd_arrow::{NdArrowArray, extension::nd_column_data_type};
use futures::stream::BoxStream;
use object_store::ObjectStore;

use crate::{IPC_WRITE_OPTS, layout::DatasetArrayLayout};

pub struct ChunkedArrayWriter<S: ObjectStore> {
    array_datatype: arrow::datatypes::DataType,
    store: S,
    temp_writer: FileWriter<File>,
    buffer_arrays: Vec<Option<NdArrowArray>>,

    // Layouts of the arrays written so far.
    layouts: Vec<DatasetArrayLayout>,
}

pub struct ChunkedArray {
    pub array_datatype: arrow::datatypes::DataType,
    pub chunk_shape: Vec<usize>,
    pub chunks: BoxStream<'static, ChunkedArrayPart>,
}

pub struct ChunkedArrayPart {
    pub array: NdArrowArray,
    pub chunk_index: Vec<usize>,
}

impl<S: ObjectStore> ChunkedArrayWriter<S> {
    pub fn new(array_datatype: arrow::datatypes::DataType, store: S) -> Self {
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
            temp_writer,
            buffer_arrays: Vec::new(),
            layouts: Vec::new(),
        }
    }

    pub async fn append_chunked_array(&mut self, array: ChunkedArray) -> anyhow::Result<()> {
        unimplemented!()
    }
}
