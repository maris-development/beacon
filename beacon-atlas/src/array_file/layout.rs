use std::collections::BTreeMap;

use crate::scalar::Scalar;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct FileArrayFooter {
    pub num_chunks: usize,
    pub chunk_offsets: Vec<u64>, // Byte offsets for each chunk in the file
    pub lookup_table: BTreeMap<u32, ArrayEntry>, // Maps entry index to chunk index and range of that entry
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct ArrayEntry {
    pub shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub fill_value: Option<Scalar>,
    pub lookup: ArrayLookup,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum ArrayLookup {
    Flat(SingleArrayLookup), // The entire array is stored as a single chunk
    Chunked {
        map: Box<BTreeMap<u32, SingleArrayLookup>>, // Maps chunk index to range of that entry in the chunk
    },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct SingleArrayLookup {
    pub chunk_index: u32,
    pub range: std::ops::Range<u32>,
    pub shape: Vec<usize>,
}

#[allow(dead_code)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum FileArrayChunk {
    CompressedZSTD {
        uncompressed_size: usize,
        bytes: Vec<u8>,
    },
}
