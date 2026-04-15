use std::{
    collections::BTreeMap,
    io::{Seek, SeekFrom, Write},
};

use beacon_nd_arrow::{NdArray, array::subset::ArraySubset};
use object_store::{ObjectStore, buffered::DEFAULT_BUFFER_SIZE};
use tempfile::NamedTempFile;

use crate::{
    array::layout::{
        ArrayEntry, ArrayLookup, ChunkMapIndex, FileArrayChunk, FileArrayFooter, SingleArrayLookup,
    },
    schema::_type::{AtlasDataType, AtlasType},
    util::stream_file_to_store,
};

pub struct ArrayFileWriter<T: AtlasType, S: ObjectStore + Clone> {
    pub store: S,
    pub path: object_store::path::Path,
    pub footer: FileArrayFooter,
    pub current_chunk: Vec<T>,
    pub chunk_size: usize,
    temp_file: NamedTempFile,
}

impl<T: AtlasType, S: ObjectStore + Clone> ArrayFileWriter<T, S> {
    pub fn new(
        store: S,
        path: object_store::path::Path,
        chunk_size: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(chunk_size > 0, "chunk_size must be greater than zero");

        Ok(Self {
            store,
            path,
            footer: FileArrayFooter {
                datatype: T::atlas_datatype(),
                num_chunks: 0,
                chunk_offsets: Vec::new(),
                lookup_table: BTreeMap::new(),
                statistics: None, // For now we don't compute statistics, but we can add this later if needed
            },
            current_chunk: Vec::new(),
            chunk_size,
            temp_file: NamedTempFile::new()
                .map_err(|e| anyhow::anyhow!("Failed to create temporary file: {}", e))?,
        })
    }

    pub async fn append(&mut self, entry_index: u32, array: NdArray<T>) -> anyhow::Result<()> {
        let shape = array.shape();
        let chunk_shape = array.chunk_shape();
        anyhow::ensure!(
            shape.len() == chunk_shape.len(),
            "Chunk shape rank {} does not match array shape rank {}",
            chunk_shape.len(),
            shape.len()
        );

        let dimensions = array.dimensions();

        if chunk_shape == shape {
            let values = array.into_raw_vec().await;
            let lookup = self.append_values_with_lookup(values, shape.clone())?;
            self.footer.lookup_table.insert(
                entry_index,
                ArrayEntry {
                    chunk_shape: shape.clone(),
                    shape,
                    dimensions,
                    fill_value: None,
                    lookup: ArrayLookup::Flat(lookup),
                },
            );

            if self.current_chunk.len() >= self.chunk_size {
                self.flush_chunk()?;
            }
        } else {
            let chunk_counts = Self::chunk_counts(&shape, &chunk_shape)?;
            let mut chunk_map = BTreeMap::new();

            if !chunk_counts.contains(&0) {
                let mut chunk_index = vec![0usize; shape.len()];

                loop {
                    let chunk_start: Vec<usize> = chunk_index
                        .iter()
                        .zip(chunk_shape.iter())
                        .map(|(index, size)| index * size)
                        .collect();

                    let chunk_extent: Vec<usize> = chunk_start
                        .iter()
                        .zip(shape.iter())
                        .zip(chunk_shape.iter())
                        .map(|((start, full_size), chunk_size)| {
                            (*full_size - *start).min(*chunk_size)
                        })
                        .collect();

                    let subset = array
                        .subset(ArraySubset::new(chunk_start, chunk_extent.clone()))
                        .await?;
                    let values = subset.into_raw_vec().await;
                    let lookup = self.append_values_with_lookup(values, chunk_extent)?;

                    let key = ChunkMapIndex::new(
                        chunk_index
                            .iter()
                            .map(|index| {
                                u32::try_from(*index).map_err(|_| {
                                    anyhow::anyhow!("Chunk index {} cannot fit into u32", index)
                                })
                            })
                            .collect::<anyhow::Result<Vec<u32>>>()?,
                    );
                    chunk_map.insert(key, lookup);

                    if self.current_chunk.len() >= self.chunk_size {
                        self.flush_chunk()?;
                    }

                    if !Self::advance_chunk_index(&mut chunk_index, &chunk_counts) {
                        break;
                    }
                }
            }

            self.footer.lookup_table.insert(
                entry_index,
                ArrayEntry {
                    shape,
                    dimensions,
                    chunk_shape,
                    fill_value: None,
                    lookup: ArrayLookup::Chunked {
                        map: Box::new(chunk_map),
                    },
                },
            );
        }

        Ok(())
    }

    pub fn flush_chunk(&mut self) -> anyhow::Result<()> {
        if self.current_chunk.is_empty() {
            return Ok(());
        }

        let chunk_data = std::mem::replace(&mut self.current_chunk, Vec::new());
        let typed_vec = T::into_vec(chunk_data);
        let serialized_array = rkyv::to_bytes::<rkyv::rancor::Error>(&typed_vec)
            .map_err(|e| anyhow::anyhow!("Failed to serialize array: {}", e))?;
        let compressed = zstd::bulk::compress(&serialized_array, 3)?;

        let chunk = FileArrayChunk::CompressedZSTD {
            uncompressed_size: serialized_array.len(),
            bytes: compressed,
        };
        let serialized_chunk = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)
            .map_err(|e| anyhow::anyhow!("Failed to serialize chunk: {}", e))?;

        let offset = self.temp_file.as_file_mut().seek(SeekFrom::End(0))?;
        self.temp_file.as_file_mut().write_all(&serialized_chunk)?;

        self.footer.chunk_offsets.push(offset);
        self.footer.num_chunks += 1;

        Ok(())
    }

    fn append_values_with_lookup(
        &mut self,
        values: Vec<T>,
        lookup_shape: Vec<usize>,
    ) -> anyhow::Result<SingleArrayLookup> {
        let start = self.current_chunk.len();
        self.current_chunk.extend(values);
        let end = self.current_chunk.len();

        let file_chunk_index = u32::try_from(self.footer.num_chunks).map_err(|_| {
            anyhow::anyhow!("Chunk index {} cannot fit into u32", self.footer.num_chunks)
        })?;
        let start_u32 = u32::try_from(start)
            .map_err(|_| anyhow::anyhow!("Chunk range start {} cannot fit into u32", start))?;
        let end_u32 = u32::try_from(end)
            .map_err(|_| anyhow::anyhow!("Chunk range end {} cannot fit into u32", end))?;

        Ok(SingleArrayLookup {
            file_chunk_index,
            range: start_u32..end_u32,
            shape: lookup_shape,
        })
    }

    fn chunk_counts(shape: &[usize], chunk_shape: &[usize]) -> anyhow::Result<Vec<usize>> {
        shape
            .iter()
            .zip(chunk_shape.iter())
            .map(|(full_size, chunk_size)| {
                anyhow::ensure!(
                    *chunk_size > 0,
                    "Chunk shape dimensions must be greater than zero"
                );
                Ok(full_size.div_ceil(*chunk_size))
            })
            .collect()
    }

    fn advance_chunk_index(chunk_index: &mut [usize], chunk_counts: &[usize]) -> bool {
        for axis in (0..chunk_index.len()).rev() {
            chunk_index[axis] += 1;
            if chunk_index[axis] < chunk_counts[axis] {
                return true;
            }
            chunk_index[axis] = 0;
        }

        false
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

    pub fn datatype(&self) -> AtlasDataType {
        T::atlas_datatype()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_nd_arrow::{
        NdArray,
        array::{
            backend::{ArrayBackend, mem::InMemoryArrayBackend},
            subset::ArraySubset,
        },
    };
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::ArrayFileWriter;
    use crate::{
        array::{layout::ArrayLookup, reader::ArrayFileReader},
        schema::_type::AtlasDataType,
    };

    #[derive(Debug, Clone)]
    struct ChunkedTestBackend {
        inner: InMemoryArrayBackend<i32>,
        chunk_shape: Vec<usize>,
    }

    impl ChunkedTestBackend {
        fn new(
            values: ndarray::ArrayD<i32>,
            shape: Vec<usize>,
            dimensions: Vec<String>,
            chunk_shape: Vec<usize>,
        ) -> Self {
            Self {
                inner: InMemoryArrayBackend::new(values, shape, dimensions, None),
                chunk_shape,
            }
        }
    }

    #[async_trait::async_trait]
    impl ArrayBackend<i32> for ChunkedTestBackend {
        fn len(&self) -> usize {
            self.inner.len()
        }

        fn shape(&self) -> Vec<usize> {
            self.inner.shape()
        }

        fn chunk_shape(&self) -> Vec<usize> {
            self.chunk_shape.clone()
        }

        fn dimensions(&self) -> Vec<String> {
            self.inner.dimensions()
        }

        fn fill_value(&self) -> Option<i32> {
            self.inner.fill_value()
        }

        async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<i32>> {
            self.inner.read_subset(subset).await
        }
    }

    #[tokio::test]
    async fn writer_roundtrip_flat_and_chunked_entries() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("writer-roundtrip.bin");

        let mut writer = ArrayFileWriter::<i32, _>::new(store.clone(), path.clone(), 4)?;

        let flat = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![10, 11, 12, 13],
            vec![4],
            vec!["x".to_string()],
            None,
        )?;
        writer.append(1, flat).await?;

        let chunked_values = ndarray::ArrayD::from_shape_vec(
            ndarray::IxDyn(&[3, 3]),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
        )?;
        let chunked_backend = ChunkedTestBackend::new(
            chunked_values,
            vec![3, 3],
            vec!["y".to_string(), "x".to_string()],
            vec![2, 2],
        );
        let chunked = NdArray::new_with_backend(chunked_backend)?;
        writer.append(2, chunked).await?;

        writer.finish().await?;

        let reader = Arc::new(ArrayFileReader::open(store, path, None).await?);
        assert_eq!(reader.footer().lookup_table.len(), 2);

        let flat_entry = reader.entry(1).expect("flat entry should exist");
        assert!(matches!(flat_entry.lookup, ArrayLookup::Flat(_)));

        let chunked_entry = reader.entry(2).expect("chunked entry should exist");
        let chunk_map = match &chunked_entry.lookup {
            ArrayLookup::Chunked { map } => map,
            _ => panic!("expected chunked lookup for entry 2"),
        };
        assert_eq!(chunk_map.len(), 4);

        let chunk_shapes: Vec<Vec<usize>> = chunk_map
            .values()
            .map(|lookup| lookup.shape.clone())
            .collect();
        assert_eq!(
            chunk_shapes,
            vec![vec![2, 2], vec![2, 1], vec![1, 2], vec![1, 1]]
        );

        let flat_roundtrip = reader.read_entry_array::<i32>(1)?.into_raw_vec().await;
        assert_eq!(flat_roundtrip, vec![10, 11, 12, 13]);

        let chunked_roundtrip = reader.read_entry_array::<i32>(2)?.into_raw_vec().await;
        assert_eq!(chunked_roundtrip, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        let chunked_subset = reader
            .read_entry_array::<i32>(2)?
            .subset(ArraySubset::new(vec![1, 1], vec![2, 2]))
            .await?;
        assert_eq!(chunked_subset.into_raw_vec().await, vec![5, 6, 8, 9]);

        Ok(())
    }
}
