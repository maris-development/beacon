use arrow::array::ArrayRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ArrayEncoding {
    NDimensional,        // Standard N-Dimensional Arrow Array
    ChunkedNDimensional, // Chunked N-Dimensional Array
    RleDict,             // Run-Length Encoded Dictionary
}

pub enum EncodedArray {
    RleDict(ArrayRef),
}
