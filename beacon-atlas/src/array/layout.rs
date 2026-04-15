use std::collections::BTreeMap;

use crate::{scalar::Scalar, schema::_type::AtlasDataType};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct FileArrayFooter {
    pub datatype: AtlasDataType,
    pub num_chunks: usize,
    pub chunk_offsets: Vec<u64>, // Byte offsets for each chunk in the file
    pub lookup_table: BTreeMap<u32, ArrayEntry>, // Maps entry index to chunk index and range of that entry
    pub statistics: Option<FileArrayStatistics>, // Maps entry index to statistics for that entry
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct FileArrayStatistics {
    pub min: FileArrayChunk,
    pub max: FileArrayChunk,
    pub null_count: FileArrayChunk,
    pub row_count: FileArrayChunk,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct ArrayEntry {
    pub shape: Vec<usize>,
    pub chunk_shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub fill_value: Option<Scalar>,
    pub lookup: ArrayLookup,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum ArrayLookup {
    Flat(SingleArrayLookup), // The entire array is stored as a single chunk
    Chunked {
        map: Box<BTreeMap<ChunkMapIndex, SingleArrayLookup>>, // Maps chunk index to range of that entry in the chunk
    },
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[repr(transparent)]
#[rkyv(attr(derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)))]
pub struct ChunkMapIndex(Vec<u32>);

impl ChunkMapIndex {
    pub fn new(index: Vec<u32>) -> Self {
        Self(index)
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<u32> {
        self.0
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct SingleArrayLookup {
    pub file_chunk_index: u32,
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
