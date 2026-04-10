use std::{
    collections::BTreeMap,
    io::{Seek, Write},
};

use object_store::{ObjectStore, buffered::DEFAULT_BUFFER_SIZE};
use tempfile::NamedTempFile;

// use crate::{array::_type::ArrayType, util::stream_file_to_store};

// pub struct ArrayFileReader<S: ObjectStore + Clone> {
//     pub store: S,
//     pub path: object_store::path::Path,
//     pub footer: FileArrayFooter,
// }

// impl<S: ObjectStore + Clone> ArrayFileReader<S> {
//     pub async fn open(store: S, path: object_store::path::Path) -> anyhow::Result<Self> {
//         todo!()
//     }

//     pub async fn read_entry_range(
//         &self,
//         entry_index: u32,
//         range: std::ops::Range<usize>,
//     ) -> anyhow::Result<VersionedArray> {
//         todo!()
//     }
// }

// pub struct ArrayFileWriter<S: ObjectStore + Clone> {
//     pub store: S,
//     pub path: object_store::path::Path,
//     pub footer: FileArrayFooter,
//     pub array_type: ArrayType,
//     pub chunk_builder: ArrayV1Builder,
//     pub chunk_size: usize,
//     temp_file: NamedTempFile,
// }

// impl<S: ObjectStore + Clone> ArrayFileWriter<S> {
//     pub fn new(
//         array_type: ArrayType,
//         store: S,
//         path: object_store::path::Path,
//         chunk_size: usize,
//     ) -> anyhow::Result<Self> {
//         Ok(Self {
//             store,
//             path,
//             footer: FileArrayFooter {
//                 num_chunks: 0,
//                 chunk_offsets: Vec::new(),
//                 ranges: BTreeMap::new(),
//             },
//             array_type,
//             chunk_builder: ArrayV1Builder::new(array_type),
//             chunk_size,
//             temp_file: NamedTempFile::new()
//                 .map_err(|e| anyhow::anyhow!("Failed to create temporary file: {}", e))?,
//         })
//     }

//     pub fn append<A: Into<ArrayV1>, I: futures::stream::Stream<Item = anyhow::Result<A>>>(
//         &mut self,
//         entry_index: u32,
//         values_stream: I,
//     ) -> anyhow::Result<()> {
//         todo!()
//     }

//     pub fn flush_chunk(&mut self) -> anyhow::Result<()> {
//         if self.chunk_builder.len() == 0 {
//             return Ok(()); // Nothing to flush
//         }
//         let builder = std::mem::replace(
//             &mut self.chunk_builder,
//             ArrayV1Builder::new(self.array_type),
//         );
//         // Serialize the chunk using rkyv
//         let array = VersionedArray::V1(builder.array.clone());
//         let serialized_array = rkyv::to_bytes::<rkyv::rancor::Error>(&array)
//             .map_err(|e| anyhow::anyhow!("Failed to serialize array: {}", e))?;
//         let compressed = zstd::bulk::compress(&serialized_array, 3)?;

//         // Serialize the chunk
//         let chunk = FileArrayChunk::CompressedZSTD {
//             uncompressed_size: serialized_array.len(),
//             bytes: compressed,
//         };
//         let serialized_chunk = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)
//             .map_err(|e| anyhow::anyhow!("Failed to serialize chunk: {}", e))?;

//         // Write the chunk to the temporary file
//         use std::io::Write;
//         let offset = self.temp_file.as_file().seek(std::io::SeekFrom::End(0))?;
//         self.temp_file.as_file_mut().write_all(&serialized_chunk)?;

//         // Update footer information
//         self.footer.chunk_offsets.push(offset);
//         self.footer.num_chunks += 1;

//         // Reset the chunk builder for the next chunk
//         self.chunk_builder = ArrayV1Builder::new(self.array_type);

//         Ok(())
//     }

//     pub async fn finish(mut self) -> anyhow::Result<()> {
//         // Flush any remaining data as a final chunk
//         self.flush_chunk()?;

//         // Serialize the footer
//         let serialized_footer = rkyv::to_bytes::<rkyv::rancor::Error>(&self.footer)
//             .map_err(|e| anyhow::anyhow!("Failed to serialize footer: {}", e))?;

//         // Write the footer at the end of the file
//         self.temp_file.as_file_mut().write_all(&serialized_footer)?;
//         // Write the footer size at the very end of the file (8 bytes for u64)
//         let footer_size = serialized_footer.len() as u64;
//         self.temp_file
//             .as_file_mut()
//             .write_all(&footer_size.to_le_bytes())?;

//         // Upload the file to object storage
//         stream_file_to_store(
//             &self.store,
//             &self.path,
//             self.temp_file.as_file_mut(),
//             DEFAULT_BUFFER_SIZE,
//         )
//         .await?;

//         Ok(())
//     }
// }

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
pub struct FileArrayFooter {
    pub num_chunks: usize,
    pub chunk_offsets: Vec<u64>, // Byte offsets for each chunk in the file
    pub ranges: BTreeMap<u32, std::ops::Range<usize>>, // Maps entry index to chunk index and range of that entry
}

// #[allow(dead_code)]
// #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
// pub enum FileArrayChunk {
//     CompressedZSTD {
//         uncompressed_size: usize,
//         bytes: Vec<u8>,
//     },
//     Uncompressed(VersionedArray),
// }
