use std::{
    collections::BTreeMap,
    io::{Seek, Write},
};

use object_store::{ObjectStore, buffered::DEFAULT_BUFFER_SIZE};
use tempfile::NamedTempFile;

use crate::{array::_type::ArrayType, util::stream_file_to_store};

pub struct ArrayFileReader<S: ObjectStore + Clone> {
    pub store: S,
    pub path: object_store::path::Path,
    pub footer: FileArrayFooter,
}

impl<S: ObjectStore + Clone> ArrayFileReader<S> {
    pub async fn open(store: S, path: object_store::path::Path) -> anyhow::Result<Self> {
        todo!()
    }

    pub async fn read_entry_range(
        &self,
        entry_index: u32,
        range: std::ops::Range<usize>,
    ) -> anyhow::Result<VersionedArray> {
        todo!()
    }
}

pub struct ArrayFileWriter<S: ObjectStore + Clone> {
    pub store: S,
    pub path: object_store::path::Path,
    pub footer: FileArrayFooter,
    pub array_type: ArrayType,
    pub chunk_builder: ArrayV1Builder,
    pub chunk_size: usize,
    temp_file: NamedTempFile,
}

impl<S: ObjectStore + Clone> ArrayFileWriter<S> {
    pub fn new(
        array_type: ArrayType,
        store: S,
        path: object_store::path::Path,
        chunk_size: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            store,
            path,
            footer: FileArrayFooter {
                num_chunks: 0,
                chunk_offsets: Vec::new(),
                ranges: BTreeMap::new(),
            },
            array_type,
            chunk_builder: ArrayV1Builder::new(array_type),
            chunk_size,
            temp_file: NamedTempFile::new()
                .map_err(|e| anyhow::anyhow!("Failed to create temporary file: {}", e))?,
        })
    }

    pub fn append<A: Into<ArrayV1>, I: futures::stream::Stream<Item = anyhow::Result<A>>>(
        &mut self,
        entry_index: u32,
        values_stream: I,
    ) -> anyhow::Result<()> {
        todo!()
    }

    pub fn flush_chunk(&mut self) -> anyhow::Result<()> {
        if self.chunk_builder.len() == 0 {
            return Ok(()); // Nothing to flush
        }
        let builder = std::mem::replace(
            &mut self.chunk_builder,
            ArrayV1Builder::new(self.array_type),
        );
        // Serialize the chunk using rkyv
        let array = VersionedArray::V1(builder.array.clone());
        let serialized_array = rkyv::to_bytes::<rkyv::rancor::Error>(&array)
            .map_err(|e| anyhow::anyhow!("Failed to serialize array: {}", e))?;
        let compressed = zstd::bulk::compress(&serialized_array, 3)?;

        // Serialize the chunk
        let chunk = FileArrayChunk::CompressedZSTD {
            uncompressed_size: serialized_array.len(),
            bytes: compressed,
        };
        let serialized_chunk = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)
            .map_err(|e| anyhow::anyhow!("Failed to serialize chunk: {}", e))?;

        // Write the chunk to the temporary file
        use std::io::Write;
        let offset = self.temp_file.as_file().seek(std::io::SeekFrom::End(0))?;
        self.temp_file.as_file_mut().write_all(&serialized_chunk)?;

        // Update footer information
        self.footer.chunk_offsets.push(offset);
        self.footer.num_chunks += 1;

        // Reset the chunk builder for the next chunk
        self.chunk_builder = ArrayV1Builder::new(self.array_type);

        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        // Flush any remaining data as a final chunk
        self.flush_chunk()?;

        // Serialize the footer
        let serialized_footer = rkyv::to_bytes::<rkyv::rancor::Error>(&self.footer)
            .map_err(|e| anyhow::anyhow!("Failed to serialize footer: {}", e))?;

        // Write the footer at the end of the file
        self.temp_file.as_file_mut().write_all(&serialized_footer)?;
        // Write the footer size at the very end of the file (8 bytes for u64)
        let footer_size = serialized_footer.len() as u64;
        self.temp_file
            .as_file_mut()
            .write_all(&footer_size.to_le_bytes())?;

        // Upload the file to object storage
        stream_file_to_store(
            &self.store,
            &self.path,
            self.temp_file.as_file_mut(),
            DEFAULT_BUFFER_SIZE,
        )
        .await?;

        Ok(())
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct FileArrayFooter {
    pub num_chunks: usize,
    pub chunk_offsets: Vec<u64>, // Byte offsets for each chunk in the file
    pub ranges: BTreeMap<u32, std::ops::Range<usize>>, // Maps entry index to chunk index and range of that entry
}

#[allow(dead_code)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum FileArrayChunk {
    CompressedZSTD {
        uncompressed_size: usize,
        bytes: Vec<u8>,
    },
    Uncompressed(VersionedArray),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum VersionedArray {
    V1(ArrayV1),
}

impl VersionedArray {
    pub fn version(&self) -> u8 {
        match self {
            VersionedArray::V1(_) => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            VersionedArray::V1(array) => match array {
                ArrayV1::Bool(vec) => vec.len(),
                ArrayV1::I8(vec) => vec.len(),
                ArrayV1::I16(vec) => vec.len(),
                ArrayV1::I32(vec) => vec.len(),
                ArrayV1::I64(vec) => vec.len(),
                ArrayV1::U8(vec) => vec.len(),
                ArrayV1::U16(vec) => vec.len(),
                ArrayV1::U32(vec) => vec.len(),
                ArrayV1::U64(vec) => vec.len(),
                ArrayV1::F32(vec) => vec.len(),
                ArrayV1::F64(vec) => vec.len(),
                ArrayV1::Timestamp(vec) => vec.len(),
                ArrayV1::Binary(vec) => vec.len(),
                ArrayV1::String(vec) => vec.len(),
            },
        }
    }

    pub fn as_bool(&self) -> Option<&Vec<bool>> {
        match self {
            VersionedArray::V1(ArrayV1::Bool(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_i8(&self) -> Option<&Vec<i8>> {
        match self {
            VersionedArray::V1(ArrayV1::I8(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<&Vec<i16>> {
        match self {
            VersionedArray::V1(ArrayV1::I16(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<&Vec<i32>> {
        match self {
            VersionedArray::V1(ArrayV1::I32(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<&Vec<i64>> {
        match self {
            VersionedArray::V1(ArrayV1::I64(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<&Vec<u8>> {
        match self {
            VersionedArray::V1(ArrayV1::U8(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<&Vec<u16>> {
        match self {
            VersionedArray::V1(ArrayV1::U16(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<&Vec<u32>> {
        match self {
            VersionedArray::V1(ArrayV1::U32(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<&Vec<u64>> {
        match self {
            VersionedArray::V1(ArrayV1::U64(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<&Vec<f32>> {
        match self {
            VersionedArray::V1(ArrayV1::F32(vec)) => Some(vec),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<&Vec<f64>> {
        match self {
            VersionedArray::V1(ArrayV1::F64(vec)) => Some(vec),
            _ => None,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub enum ArrayV1 {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Timestamp(Vec<i64>),  // Unix timestamp in nanoseconds
    Binary(Vec<Vec<u8>>), // Variable-length binary data
    String(Vec<String>),  // Variable-length UTF-8 strings
}

impl ArrayV1 {
    pub fn array_type(&self) -> ArrayType {
        match self {
            ArrayV1::Bool(_) => ArrayType::Bool,
            ArrayV1::I8(_) => ArrayType::I8,
            ArrayV1::I16(_) => ArrayType::I16,
            ArrayV1::I32(_) => ArrayType::I32,
            ArrayV1::I64(_) => ArrayType::I64,
            ArrayV1::U8(_) => ArrayType::U8,
            ArrayV1::U16(_) => ArrayType::U16,
            ArrayV1::U32(_) => ArrayType::U32,
            ArrayV1::U64(_) => ArrayType::U64,
            ArrayV1::F32(_) => ArrayType::F32,
            ArrayV1::F64(_) => ArrayType::F64,
            ArrayV1::Timestamp(_) => ArrayType::Timestamp,
            ArrayV1::Binary(_) => ArrayType::Binary,
            ArrayV1::String(_) => ArrayType::String,
        }
    }
}

pub struct ArrayV1Builder {
    array: ArrayV1,
}

impl ArrayV1Builder {
    pub fn new(array_type: ArrayType) -> Self {
        let array = match array_type {
            ArrayType::Bool => ArrayV1::Bool(Vec::new()),
            ArrayType::I8 => ArrayV1::I8(Vec::new()),
            ArrayType::I16 => ArrayV1::I16(Vec::new()),
            ArrayType::I32 => ArrayV1::I32(Vec::new()),
            ArrayType::I64 => ArrayV1::I64(Vec::new()),
            ArrayType::U8 => ArrayV1::U8(Vec::new()),
            ArrayType::U16 => ArrayV1::U16(Vec::new()),
            ArrayType::U32 => ArrayV1::U32(Vec::new()),
            ArrayType::U64 => ArrayV1::U64(Vec::new()),
            ArrayType::F32 => ArrayV1::F32(Vec::new()),
            ArrayType::F64 => ArrayV1::F64(Vec::new()),
            ArrayType::Timestamp => ArrayV1::Timestamp(Vec::new()),
            ArrayType::Binary => ArrayV1::Binary(Vec::new()),
            ArrayType::String => ArrayV1::String(Vec::new()),
        };
        Self { array }
    }

    pub fn len(&self) -> usize {
        match &self.array {
            ArrayV1::Bool(vec) => vec.len(),
            ArrayV1::I8(vec) => vec.len(),
            ArrayV1::I16(vec) => vec.len(),
            ArrayV1::I32(vec) => vec.len(),
            ArrayV1::I64(vec) => vec.len(),
            ArrayV1::U8(vec) => vec.len(),
            ArrayV1::U16(vec) => vec.len(),
            ArrayV1::U32(vec) => vec.len(),
            ArrayV1::U64(vec) => vec.len(),
            ArrayV1::F32(vec) => vec.len(),
            ArrayV1::F64(vec) => vec.len(),
            ArrayV1::Timestamp(vec) => vec.len(),
            ArrayV1::Binary(vec) => vec.len(),
            ArrayV1::String(vec) => vec.len(),
        }
    }

    pub fn try_append(&mut self, values: impl Into<ArrayV1>) -> anyhow::Result<()> {
        let values_converted: ArrayV1 = values.into();
        match (&mut self.array, values_converted) {
            (ArrayV1::Bool(vec), ArrayV1::Bool(new_vec)) => vec.extend(new_vec),
            (ArrayV1::I8(vec), ArrayV1::I8(new_vec)) => vec.extend(new_vec),
            (ArrayV1::I16(vec), ArrayV1::I16(new_vec)) => vec.extend(new_vec),
            (ArrayV1::I32(vec), ArrayV1::I32(new_vec)) => vec.extend(new_vec),
            (ArrayV1::I64(vec), ArrayV1::I64(new_vec)) => vec.extend(new_vec),
            (ArrayV1::U8(vec), ArrayV1::U8(new_vec)) => vec.extend(new_vec),
            (ArrayV1::U16(vec), ArrayV1::U16(new_vec)) => vec.extend(new_vec),
            (ArrayV1::U32(vec), ArrayV1::U32(new_vec)) => vec.extend(new_vec),
            (ArrayV1::U64(vec), ArrayV1::U64(new_vec)) => vec.extend(new_vec),
            (ArrayV1::F32(vec), ArrayV1::F32(new_vec)) => vec.extend(new_vec),
            (ArrayV1::F64(vec), ArrayV1::F64(new_vec)) => vec.extend(new_vec),
            (ArrayV1::Timestamp(vec), ArrayV1::Timestamp(new_vec)) => vec.extend(new_vec),
            (ArrayV1::Binary(vec), ArrayV1::Binary(new_vec)) => vec.extend(new_vec),
            (ArrayV1::String(vec), ArrayV1::String(new_vec)) => vec.extend(new_vec),
            (left, right) => {
                anyhow::bail!(
                    "Type mismatch: cannot append {:?} to {:?}",
                    right.array_type(),
                    left.array_type()
                )
            }
        };
        Ok(())
    }
}
