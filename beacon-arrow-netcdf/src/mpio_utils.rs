//! Serializable command/response types for MPIO request handling.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

/// Commands accepted by the MPIO bridge.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ReadCommand {
    /// Read and return only the Arrow schema for a file.
    ReadArrowSchema {
        /// Correlation id echoed in responses.
        request_id: u32,
        /// Path to the NetCDF source file.
        path: String,
    },
    /// Read file contents as streamed Arrow bytes.
    ReadFile {
        /// Correlation id echoed in responses.
        request_id: u32,
        /// Path to the NetCDF source file.
        path: String,
        /// Optional projected column indices.
        projection: Option<Vec<usize>>,
        /// Preferred row chunk size.
        chunk_size: usize,
        /// Number of bytes to emit per stream message.
        stream_size: usize,
    },
    /// Stop command processing.
    Exit,
}

/// Responses emitted by the MPIO bridge.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CommandResponse {
    /// Request failed.
    Error {
        /// Correlation id.
        request_id: u32,
        /// Human-readable error message.
        message: String,
    },
    /// Arrow schema response.
    ArrowSchema {
        /// Correlation id.
        request_id: u32,
        /// Derived Arrow schema.
        schema: arrow::datatypes::Schema,
    },
    /// Chunk of Arrow IPC bytes.
    ArrowBytes {
        /// Correlation id.
        request_id: u32,
        /// Number of bytes in this chunk.
        length: usize,
    },
    /// End-of-stream marker for a request.
    ArrowStreamEnd {
        /// Correlation id.
        request_id: u32,
    },
}
