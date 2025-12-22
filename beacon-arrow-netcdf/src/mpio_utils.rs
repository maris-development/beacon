use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Command {
    ReadArrowSchema {
        request_id: u32,
        path: String,
    },
    ReadFile {
        request_id: u32,
        path: String,
        projection: Option<Vec<usize>>,
        chunk_size: usize,
        stream_size: usize,
    },
    Exit,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CommandReponse {
    Error {
        request_id: u32,
        message: String,
    },
    ArrowSchema {
        request_id: u32,
        schema: arrow::datatypes::Schema,
    },
    BatchesStream {
        request_id: u32,
        length: usize,
        has_more: bool,
    },
}
