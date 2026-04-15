use beacon_nd_arrow::NdArray;
use object_store::ObjectStore;
use rkyv::util::AlignedVec;
use std::sync::Arc;

use crate::{
    array::{
        backend::AtlasArrayFileBackend,
        layout::{ArrayEntry, FileArrayChunk, FileArrayFooter},
    },
    cache::VecCache,
    schema::_type::AtlasType,
    typed_vec::TypedVec,
};

#[derive(Debug)]
pub struct ArrayFileReader<S: ObjectStore + Clone> {
    store: S,
    path: object_store::path::Path,
    footer: FileArrayFooter,
    footer_start: u64,
    cache: Option<Arc<VecCache>>,
}

impl<S: ObjectStore + Clone + Send + Sync + 'static> ArrayFileReader<S> {
    pub async fn open(
        store: S,
        path: object_store::path::Path,
        cache: Option<Arc<VecCache>>,
    ) -> anyhow::Result<Self> {
        let file_size = store
            .head(&path)
            .await
            .map_err(|err| anyhow::anyhow!(err))?
            .size;

        anyhow::ensure!(
            file_size >= 8,
            "Array file is too small to contain footer size trailer"
        );

        let footer_size_offset = file_size - 8;
        let footer_size_bytes = store
            .get_range(&path, footer_size_offset..file_size)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        anyhow::ensure!(
            footer_size_bytes.len() == 8,
            "Invalid footer size trailer length: expected 8, got {}",
            footer_size_bytes.len()
        );

        let mut footer_size_array = [0_u8; 8];
        footer_size_array.copy_from_slice(footer_size_bytes.as_ref());
        let footer_size = u64::from_le_bytes(footer_size_array);
        anyhow::ensure!(
            footer_size <= footer_size_offset,
            "Footer size {} exceeds file content before trailer {}",
            footer_size,
            footer_size_offset
        );

        let footer_start = footer_size_offset - footer_size;
        let footer_bytes = store
            .get_range(&path, footer_start..footer_size_offset)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let mut aligned_footer: AlignedVec<16> = AlignedVec::with_capacity(footer_bytes.len());
        aligned_footer.extend_from_slice(footer_bytes.as_ref());
        let footer = rkyv::from_bytes::<FileArrayFooter, rkyv::rancor::Error>(&aligned_footer)
            .map_err(|err| anyhow::anyhow!("Failed to deserialize array file footer: {err}"))?;

        anyhow::ensure!(
            footer.num_chunks == footer.chunk_offsets.len(),
            "Footer num_chunks {} does not match chunk_offsets length {}",
            footer.num_chunks,
            footer.chunk_offsets.len()
        );

        for window in footer.chunk_offsets.windows(2) {
            anyhow::ensure!(
                window[0] < window[1],
                "Footer chunk offsets must be strictly increasing"
            );
        }

        if let Some(last_offset) = footer.chunk_offsets.last() {
            anyhow::ensure!(
                *last_offset < footer_start,
                "Last chunk offset {} must be before footer start {}",
                last_offset,
                footer_start
            );
        }

        Ok(Self {
            store,
            path,
            footer,
            footer_start,
            cache,
        })
    }

    pub fn footer(&self) -> &FileArrayFooter {
        &self.footer
    }

    pub fn entry(&self, entry_index: u32) -> Option<&ArrayEntry> {
        self.footer.lookup_table.get(&entry_index)
    }

    pub fn footer_start(&self) -> u64 {
        self.footer_start
    }

    pub async fn read_file_chunk_typed<T: AtlasType>(
        &self,
        file_chunk_index: u32,
    ) -> anyhow::Result<Vec<T>> {
        let chunk = self.read_file_chunk(file_chunk_index).await?;
        T::try_from_vec(chunk)
    }

    pub async fn read_file_chunk(&self, file_chunk_index: u32) -> anyhow::Result<TypedVec> {
        let chunk_index = file_chunk_index as usize;
        anyhow::ensure!(
            chunk_index < self.footer.chunk_offsets.len(),
            "Chunk index {} out of bounds for {} chunks",
            file_chunk_index,
            self.footer.chunk_offsets.len()
        );

        let chunk_start = self.footer.chunk_offsets[chunk_index];
        let chunk_end = if chunk_index + 1 < self.footer.chunk_offsets.len() {
            self.footer.chunk_offsets[chunk_index + 1]
        } else {
            self.footer_start
        };

        anyhow::ensure!(
            chunk_start < chunk_end,
            "Invalid chunk bounds for chunk {}: {}..{}",
            file_chunk_index,
            chunk_start,
            chunk_end
        );
        anyhow::ensure!(
            chunk_end <= self.footer_start,
            "Chunk {} end {} exceeds footer start {}",
            file_chunk_index,
            chunk_end,
            self.footer_start
        );

        let cloned_store = self.store.clone();
        let fut = |(path, _chunk_index)| async move {
            let chunk_bytes = cloned_store
                .get_range(&path, chunk_start..chunk_end)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to resolve chunk bytes: {}", e))?;

            let mut aligned_chunk: AlignedVec<64> = AlignedVec::with_capacity(chunk_bytes.len());
            aligned_chunk.extend_from_slice(chunk_bytes.as_ref());
            let chunk = rkyv::from_bytes::<FileArrayChunk, rkyv::rancor::Error>(&aligned_chunk)
                .map_err(|err| anyhow::anyhow!("Failed to deserialize file chunk: {err}"))?;

            match chunk {
                FileArrayChunk::CompressedZSTD {
                    uncompressed_size,
                    bytes,
                } => {
                    let decompressed = zstd::bulk::decompress(&bytes, uncompressed_size)?;
                    let mut aligned_values: AlignedVec<64> =
                        AlignedVec::with_capacity(decompressed.len());
                    aligned_values.extend_from_slice(decompressed.as_slice());
                    rkyv::from_bytes::<TypedVec, rkyv::rancor::Error>(&aligned_values).map_err(
                        |err| anyhow::anyhow!("Failed to deserialize typed chunk payload: {err}"),
                    )
                }
            }
        };

        match &self.cache {
            Some(cache) => {
                // Access cache with try get resolvable future
                cache
                    .try_get_or_insert((self.path.clone(), chunk_index as u32), fut)
                    .await
            }
            None => {
                // Resolve future
                fut((self.path.clone(), chunk_index as u32)).await
            }
        }
    }

    pub fn read_entry_array<T: AtlasType>(
        self: &Arc<Self>,
        entry_index: u32,
    ) -> anyhow::Result<NdArray<T>> {
        let backend = AtlasArrayFileBackend::<S, T>::new(Arc::clone(self), entry_index)?;
        NdArray::new_with_backend(backend)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::ArrayFileReader;
    use crate::{
        array::layout::{
            ArrayEntry, ArrayLookup, FileArrayChunk, FileArrayFooter, SingleArrayLookup,
        },
        schema::_type::AtlasDataType,
        typed_vec::TypedVec,
    };

    fn encode_file(
        chunks: Vec<TypedVec>,
        lookup_table: BTreeMap<u32, ArrayEntry>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut file_bytes = Vec::new();
        let mut chunk_offsets = Vec::with_capacity(chunks.len());

        for chunk_values in chunks {
            let chunk_offset = u64::try_from(file_bytes.len())?;
            chunk_offsets.push(chunk_offset);

            let serialized_array = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk_values)?;
            let compressed = zstd::bulk::compress(&serialized_array, 3)?;
            let chunk = FileArrayChunk::CompressedZSTD {
                uncompressed_size: serialized_array.len(),
                bytes: compressed,
            };
            let serialized_chunk = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)?;
            file_bytes.extend_from_slice(&serialized_chunk);
        }

        let footer = FileArrayFooter {
            datatype: AtlasDataType::I32,
            num_chunks: chunk_offsets.len(),
            chunk_offsets,
            lookup_table,
            statistics: None,
        };
        let serialized_footer = rkyv::to_bytes::<rkyv::rancor::Error>(&footer)?;
        file_bytes.extend_from_slice(&serialized_footer);
        file_bytes.extend_from_slice(&(serialized_footer.len() as u64).to_le_bytes());

        Ok(file_bytes)
    }

    #[tokio::test]
    async fn open_reads_footer_and_chunk_payloads() -> anyhow::Result<()> {
        let mut lookup_table = BTreeMap::new();
        lookup_table.insert(
            1,
            ArrayEntry {
                shape: vec![3],
                chunk_shape: vec![3],
                dimensions: vec!["x".to_string()],
                fill_value: None,
                lookup: ArrayLookup::Flat(SingleArrayLookup {
                    file_chunk_index: 0,
                    range: 0..3,
                    shape: vec![3],
                }),
            },
        );

        let file_bytes = encode_file(
            vec![TypedVec::I32(vec![1, 2]), TypedVec::I32(vec![3, 4, 5])],
            lookup_table,
        )?;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("array-file.bin");
        store.put(&path, Bytes::from(file_bytes).into()).await?;

        let reader = ArrayFileReader::open(store.clone(), path, None).await?;
        assert_eq!(reader.footer().num_chunks, 2);

        let first = reader.read_file_chunk(0).await?;
        assert_eq!(first, TypedVec::I32(vec![1, 2]));

        let second = reader.read_file_chunk(1).await?;
        assert_eq!(second, TypedVec::I32(vec![3, 4, 5]));

        Ok(())
    }

    #[tokio::test]
    async fn open_rejects_too_small_file() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("too-small.bin");
        store
            .put(&path, Bytes::from(vec![1_u8, 2, 3]).into())
            .await?;

        let err = ArrayFileReader::open(store, path, None)
            .await
            .expect_err("reader should fail for too-small file");
        assert!(
            err.to_string()
                .contains("Array file is too small to contain footer size trailer")
        );

        Ok(())
    }
}
