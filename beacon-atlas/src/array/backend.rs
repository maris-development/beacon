use std::{ops::Range, sync::Arc};

use beacon_nd_arrow::array::{backend::ArrayBackend, subset::ArraySubset};
use object_store::ObjectStore;

use crate::{
    array::{
        layout::{ArrayLookup, SingleArrayLookup},
        reader::ArrayFileReader,
    },
    schema::_type::AtlasType,
    typed_vec::TypedVec,
};

#[derive(Debug)]
pub struct AtlasArrayFileBackend<S: ObjectStore + Clone, T: AtlasType> {
    _marker: std::marker::PhantomData<T>,
    reader: Arc<ArrayFileReader<S>>,
    entry_index: u32,
    shape: Vec<usize>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
    dimensions: Vec<String>,
    lookups: ArrayLookup,
}

#[derive(Debug)]
struct ChunkOverlap {
    chunk_local_start: Vec<usize>,
    output_local_start: Vec<usize>,
    overlap_shape: Vec<usize>,
}

#[async_trait::async_trait]
impl<S: ObjectStore + Clone + Send + Sync + 'static, T: AtlasType> ArrayBackend<T>
    for AtlasArrayFileBackend<S, T>
{
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        self.validate_subset(&subset)?;

        // Fast path for non-chunked array: just read the single chunk for the entire array
        match &self.lookups {
            ArrayLookup::Flat(lookup) => {
                let values = self.read_values_for_lookup(lookup).await?;
                let array = ndarray::ArrayD::from_shape_vec(self.shape.clone(), values)?;
                // Slice the array according to the subset
                let sliced = array.slice_each_axis(|axis| {
                    let start = subset.start[axis.axis.index()];
                    let end = start + subset.shape[axis.axis.index()];
                    ndarray::Slice::from(start..end)
                });
                return Ok(sliced.to_owned());
            }
            ArrayLookup::Chunked { map } => {
                // For chunked arrays, assemble the requested subset by copying intersecting
                // regions from each chunk. `lookup.shape` is authoritative and may be smaller
                // than `self.chunk_shape` on remainder chunks.
                let mut mutable_array = Self::create_subset_output(&subset)?;
                let subset_end = Self::subset_end(&subset)?;

                for (chunk_index, lookup) in map.iter() {
                    let Some(overlap) = self.compute_chunk_overlap(
                        &subset,
                        &subset_end,
                        chunk_index.as_slice(),
                        lookup,
                    )?
                    else {
                        continue;
                    };

                    self.copy_overlap_from_chunk(&mut mutable_array, lookup, &overlap)
                        .await?;
                }

                Ok(mutable_array)
            }
        }
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }
}

impl<S: ObjectStore + Clone + Send + Sync + 'static, T: AtlasType> AtlasArrayFileBackend<S, T> {
    pub fn new(reader: Arc<ArrayFileReader<S>>, entry_index: u32) -> anyhow::Result<Self> {
        let entry = reader.entry(entry_index).cloned().ok_or_else(|| {
            anyhow::anyhow!("Entry index {} not found in array footer", entry_index)
        })?;

        let fill_value = entry
            .fill_value
            .as_ref()
            .map(T::try_from_scalar)
            .transpose()?;

        Ok(Self {
            _marker: std::marker::PhantomData,
            reader,
            entry_index,
            shape: entry.shape,
            chunk_shape: entry.chunk_shape,
            fill_value,
            dimensions: entry.dimensions,
            lookups: entry.lookup,
        })
    }

    async fn read_values_for_lookup(&self, lookup: &SingleArrayLookup) -> anyhow::Result<Vec<T>> {
        let file_chunk = self.reader.read_file_chunk(lookup.file_chunk_index).await?;

        let start = usize::try_from(lookup.range.start).map_err(|_| {
            anyhow::anyhow!(
                "Lookup range start {} does not fit into usize",
                lookup.range.start
            )
        })?;
        let end = usize::try_from(lookup.range.end).map_err(|_| {
            anyhow::anyhow!(
                "Lookup range end {} does not fit into usize",
                lookup.range.end
            )
        })?;
        let slice = Self::slice_typed_vec(&file_chunk, start..end)?;

        T::try_from_vec(slice).map_err(|err| {
            anyhow::anyhow!(
                "Failed to convert typed lookup values for entry {}: {err}",
                self.entry_index
            )
        })
    }

    fn create_subset_output(subset: &ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        let output_len: usize = subset.shape.iter().product();
        if output_len == 0 {
            return ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&subset.shape), Vec::new())
                .map_err(|err| anyhow::anyhow!("Failed to build empty subset array: {err}"));
        }

        ndarray::ArrayD::from_shape_vec(
            ndarray::IxDyn(&subset.shape),
            vec![T::default(); output_len],
        )
        .map_err(|err| anyhow::anyhow!("Failed to build subset output array: {err}"))
    }

    fn subset_end(subset: &ArraySubset) -> anyhow::Result<Vec<usize>> {
        subset
            .start
            .iter()
            .zip(subset.shape.iter())
            .map(|(start, len)| {
                start.checked_add(*len).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Subset axis bound overflow for start {} and length {}",
                        start,
                        len
                    )
                })
            })
            .collect::<anyhow::Result<_>>()
    }

    fn compute_chunk_overlap(
        &self,
        subset: &ArraySubset,
        subset_end: &[usize],
        chunk_coords: &[u32],
        lookup: &SingleArrayLookup,
    ) -> anyhow::Result<Option<ChunkOverlap>> {
        anyhow::ensure!(
            chunk_coords.len() == subset.start.len(),
            "Chunk index dimension {} does not match subset dimension {}",
            chunk_coords.len(),
            subset.start.len()
        );
        anyhow::ensure!(
            lookup.shape.len() == subset.start.len(),
            "Chunk lookup shape dimension {} does not match subset dimension {}",
            lookup.shape.len(),
            subset.start.len()
        );

        let mut chunk_local_start = Vec::with_capacity(subset.start.len());
        let mut output_local_start = Vec::with_capacity(subset.start.len());
        let mut overlap_shape = Vec::with_capacity(subset.start.len());

        for axis in 0..subset.start.len() {
            let chunk_coord = usize::try_from(chunk_coords[axis]).map_err(|_| {
                anyhow::anyhow!(
                    "Chunk coordinate {} does not fit in usize",
                    chunk_coords[axis]
                )
            })?;
            let chunk_start = chunk_coord
                .checked_mul(self.chunk_shape[axis])
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Chunk start overflow for axis {} with coord {} and chunk size {}",
                        axis,
                        chunk_coord,
                        self.chunk_shape[axis]
                    )
                })?;
            let chunk_end = chunk_start.checked_add(lookup.shape[axis]).ok_or_else(|| {
                anyhow::anyhow!(
                    "Chunk end overflow for axis {} with start {} and extent {}",
                    axis,
                    chunk_start,
                    lookup.shape[axis]
                )
            })?;

            let overlap_start = subset.start[axis].max(chunk_start);
            let overlap_end = subset_end[axis].min(chunk_end);

            if overlap_start >= overlap_end {
                return Ok(None);
            }

            chunk_local_start.push(overlap_start - chunk_start);
            output_local_start.push(overlap_start - subset.start[axis]);
            overlap_shape.push(overlap_end - overlap_start);
        }

        Ok(Some(ChunkOverlap {
            chunk_local_start,
            output_local_start,
            overlap_shape,
        }))
    }

    async fn copy_overlap_from_chunk(
        &self,
        mutable_array: &mut ndarray::ArrayD<T>,
        lookup: &SingleArrayLookup,
        overlap: &ChunkOverlap,
    ) -> anyhow::Result<()> {
        let chunk_values = self.read_values_for_lookup(lookup).await?;
        let chunk_array =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&lookup.shape), chunk_values).map_err(
                |err| {
                    anyhow::anyhow!(
                        "Failed to build chunk array for entry {} chunk {}: {err}",
                        self.entry_index,
                        lookup.file_chunk_index
                    )
                },
            )?;

        let chunk_view = chunk_array.slice_each_axis(|axis| {
            let i = axis.axis.index();
            let start = overlap.chunk_local_start[i];
            let end = start + overlap.overlap_shape[i];
            ndarray::Slice::from(start..end)
        });

        let mut output_view = mutable_array.slice_each_axis_mut(|axis| {
            let i = axis.axis.index();
            let start = overlap.output_local_start[i];
            let end = start + overlap.overlap_shape[i];
            ndarray::Slice::from(start..end)
        });
        output_view.assign(&chunk_view);

        Ok(())
    }

    fn slice_typed_vec(values: &TypedVec, range: Range<usize>) -> anyhow::Result<TypedVec> {
        anyhow::ensure!(
            range.start <= range.end,
            "Invalid typed vector slice range {}..{}",
            range.start,
            range.end
        );
        anyhow::ensure!(
            range.end <= values.len(),
            "Slice range end {} exceeds typed vector length {}",
            range.end,
            values.len()
        );

        Ok(match values {
            TypedVec::Bool(vec) => TypedVec::Bool(vec[range.clone()].to_vec()),
            TypedVec::I8(vec) => TypedVec::I8(vec[range.clone()].to_vec()),
            TypedVec::I16(vec) => TypedVec::I16(vec[range.clone()].to_vec()),
            TypedVec::I32(vec) => TypedVec::I32(vec[range.clone()].to_vec()),
            TypedVec::I64(vec) => TypedVec::I64(vec[range.clone()].to_vec()),
            TypedVec::U8(vec) => TypedVec::U8(vec[range.clone()].to_vec()),
            TypedVec::U16(vec) => TypedVec::U16(vec[range.clone()].to_vec()),
            TypedVec::U32(vec) => TypedVec::U32(vec[range.clone()].to_vec()),
            TypedVec::U64(vec) => TypedVec::U64(vec[range.clone()].to_vec()),
            TypedVec::F32(vec) => TypedVec::F32(vec[range.clone()].to_vec()),
            TypedVec::F64(vec) => TypedVec::F64(vec[range.clone()].to_vec()),
            TypedVec::Timestamp(vec) => TypedVec::Timestamp(vec[range.clone()].to_vec()),
            TypedVec::Binary(vec) => TypedVec::Binary(vec[range.clone()].to_vec()),
            TypedVec::String(vec) => TypedVec::String(vec[range].to_vec()),
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{collections::BTreeMap, sync::Arc};

//     use beacon_nd_arrow::array::{backend::ArrayBackend, subset::ArraySubset};
//     use bytes::Bytes;
//     use object_store::{ObjectStore, memory::InMemory, path::Path};

//     use super::AtlasArrayFileBackend;
//     use crate::{
//         array::{
//             layout::{
//                 ArrayEntry, ArrayLookup, ChunkMapIndex, FileArrayChunk, FileArrayFooter,
//                 SingleArrayLookup,
//             },
//             reader::ArrayFileReader,
//         },
//         typed_vec::TypedVec,
//     };

//     fn encode_file(
//         chunks: Vec<TypedVec>,
//         lookup_table: BTreeMap<u32, ArrayEntry>,
//     ) -> anyhow::Result<Vec<u8>> {
//         let mut file_bytes = Vec::new();
//         let mut chunk_offsets = Vec::with_capacity(chunks.len());

//         for chunk_values in chunks {
//             let chunk_offset = u64::try_from(file_bytes.len())?;
//             chunk_offsets.push(chunk_offset);

//             let serialized_array = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk_values)?;
//             let compressed = zstd::bulk::compress(&serialized_array, 3)?;
//             let chunk = FileArrayChunk::CompressedZSTD {
//                 uncompressed_size: serialized_array.len(),
//                 bytes: compressed,
//             };
//             let serialized_chunk = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)?;
//             file_bytes.extend_from_slice(&serialized_chunk);
//         }

//         let footer = FileArrayFooter {
//             num_chunks: chunk_offsets.len(),
//             chunk_offsets,
//             lookup_table,
//             statistics: None,
//         };

//         let serialized_footer = rkyv::to_bytes::<rkyv::rancor::Error>(&footer)?;
//         file_bytes.extend_from_slice(&serialized_footer);
//         file_bytes.extend_from_slice(&(serialized_footer.len() as u64).to_le_bytes());

//         Ok(file_bytes)
//     }

//     async fn open_reader(
//         path: &str,
//         chunks: Vec<TypedVec>,
//         lookup_table: BTreeMap<u32, ArrayEntry>,
//     ) -> anyhow::Result<Arc<ArrayFileReader<Arc<dyn ObjectStore>>>> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let path = Path::from(path);
//         let file_bytes = encode_file(chunks, lookup_table)?;
//         store.put(&path, Bytes::from(file_bytes).into()).await?;

//         Ok(Arc::new(ArrayFileReader::open(store, path, None).await?))
//     }

//     #[tokio::test]
//     async fn lazy_backend_reads_subset_from_flat_entry() -> anyhow::Result<()> {
//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             1,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![4],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Flat(SingleArrayLookup {
//                     file_chunk_index: 0,
//                     range: 0..4,
//                     shape: vec![4],
//                 }),
//             },
//         );

//         let reader = open_reader(
//             "flat-entry.bin",
//             vec![TypedVec::I32(vec![10, 11, 12, 13])],
//             lookup_table,
//         )
//         .await?;
//         let backend = AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader, 1)?;

//         let subset = backend
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;

//         assert_eq!(subset.iter().copied().collect::<Vec<_>>(), vec![11, 12]);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn lazy_backend_reads_subset_across_chunked_entry() -> anyhow::Result<()> {
//         let mut chunk_map = BTreeMap::new();
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![0]),
//             SingleArrayLookup {
//                 file_chunk_index: 0,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![1]),
//             SingleArrayLookup {
//                 file_chunk_index: 1,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );

//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             42,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![2],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Chunked {
//                     map: Box::new(chunk_map),
//                 },
//             },
//         );

//         let reader = open_reader(
//             "chunked-entry.bin",
//             vec![TypedVec::I32(vec![1, 2]), TypedVec::I32(vec![3, 4])],
//             lookup_table,
//         )
//         .await?;
//         let backend = AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader.clone(), 42)?;

//         let subset = backend
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;
//         assert_eq!(subset.iter().copied().collect::<Vec<_>>(), vec![2, 3]);

//         let lazy_array = reader.read_entry_array::<i32>(42)?;
//         let subset_from_array = lazy_array
//             .subset(ArraySubset {
//                 start: vec![2],
//                 shape: vec![2],
//             })
//             .await?;
//         assert_eq!(subset_from_array.into_raw_vec().await, vec![3, 4]);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn lazy_backend_handles_remainder_chunk_shape() -> anyhow::Result<()> {
//         let mut chunk_map = BTreeMap::new();
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![0]),
//             SingleArrayLookup {
//                 file_chunk_index: 0,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![1]),
//             SingleArrayLookup {
//                 file_chunk_index: 1,
//                 range: 0..1,
//                 shape: vec![1],
//             },
//         );

//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             8,
//             ArrayEntry {
//                 shape: vec![3],
//                 chunk_shape: vec![2],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Chunked {
//                     map: Box::new(chunk_map),
//                 },
//             },
//         );

//         let reader = open_reader(
//             "remainder-entry.bin",
//             vec![TypedVec::I32(vec![5, 6]), TypedVec::I32(vec![7])],
//             lookup_table,
//         )
//         .await?;
//         let backend = AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader, 8)?;

//         let subset = backend
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;
//         assert_eq!(subset.iter().copied().collect::<Vec<_>>(), vec![6, 7]);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn lazy_backend_reads_2d_subset_across_chunk_axes() -> anyhow::Result<()> {
//         let mut chunk_map = BTreeMap::new();
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![0, 0]),
//             SingleArrayLookup {
//                 file_chunk_index: 0,
//                 range: 0..4,
//                 shape: vec![2, 2],
//             },
//         );
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![0, 1]),
//             SingleArrayLookup {
//                 file_chunk_index: 1,
//                 range: 0..2,
//                 shape: vec![2, 1],
//             },
//         );
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![1, 0]),
//             SingleArrayLookup {
//                 file_chunk_index: 2,
//                 range: 0..2,
//                 shape: vec![1, 2],
//             },
//         );
//         chunk_map.insert(
//             ChunkMapIndex::new(vec![1, 1]),
//             SingleArrayLookup {
//                 file_chunk_index: 3,
//                 range: 0..1,
//                 shape: vec![1, 1],
//             },
//         );

//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             99,
//             ArrayEntry {
//                 shape: vec![3, 3],
//                 chunk_shape: vec![2, 2],
//                 dimensions: vec!["y".to_string(), "x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Chunked {
//                     map: Box::new(chunk_map),
//                 },
//             },
//         );

//         let reader = open_reader(
//             "chunked-2d-entry.bin",
//             vec![
//                 TypedVec::I32(vec![1, 2, 4, 5]),
//                 TypedVec::I32(vec![3, 6]),
//                 TypedVec::I32(vec![7, 8]),
//                 TypedVec::I32(vec![9]),
//             ],
//             lookup_table,
//         )
//         .await?;
//         let backend = AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader, 99)?;

//         let subset = backend
//             .read_subset(ArraySubset {
//                 start: vec![1, 1],
//                 shape: vec![2, 2],
//             })
//             .await?;

//         assert_eq!(subset.into_raw_vec_and_offset().0, vec![5, 6, 8, 9]);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn lazy_backend_reads_multiple_flat_entries() -> anyhow::Result<()> {
//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             1,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![4],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Flat(SingleArrayLookup {
//                     file_chunk_index: 0,
//                     range: 0..4,
//                     shape: vec![4],
//                 }),
//             },
//         );
//         lookup_table.insert(
//             2,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![4],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Flat(SingleArrayLookup {
//                     file_chunk_index: 1,
//                     range: 0..4,
//                     shape: vec![4],
//                 }),
//             },
//         );

//         let reader = open_reader(
//             "multi-flat-entry.bin",
//             vec![
//                 TypedVec::I32(vec![10, 11, 12, 13]),
//                 TypedVec::I32(vec![20, 21, 22, 23]),
//             ],
//             lookup_table,
//         )
//         .await?;

//         let backend_one =
//             AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader.clone(), 1)?;
//         let backend_two =
//             AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader.clone(), 2)?;

//         let subset_one = backend_one
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;
//         let subset_two = backend_two
//             .read_subset(ArraySubset {
//                 start: vec![0],
//                 shape: vec![3],
//             })
//             .await?;

//         assert_eq!(subset_one.iter().copied().collect::<Vec<_>>(), vec![11, 12]);
//         assert_eq!(
//             subset_two.iter().copied().collect::<Vec<_>>(),
//             vec![20, 21, 22]
//         );

//         let lazy_one = reader.read_entry_array::<i32>(1)?;
//         let lazy_two = reader.read_entry_array::<i32>(2)?;

//         assert_eq!(lazy_one.into_raw_vec().await, vec![10, 11, 12, 13]);
//         assert_eq!(lazy_two.into_raw_vec().await, vec![20, 21, 22, 23]);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn lazy_backend_reads_multiple_chunked_entries() -> anyhow::Result<()> {
//         let mut chunk_map_one = BTreeMap::new();
//         chunk_map_one.insert(
//             ChunkMapIndex::new(vec![0]),
//             SingleArrayLookup {
//                 file_chunk_index: 0,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );
//         chunk_map_one.insert(
//             ChunkMapIndex::new(vec![1]),
//             SingleArrayLookup {
//                 file_chunk_index: 1,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );

//         let mut chunk_map_two = BTreeMap::new();
//         chunk_map_two.insert(
//             ChunkMapIndex::new(vec![0]),
//             SingleArrayLookup {
//                 file_chunk_index: 2,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );
//         chunk_map_two.insert(
//             ChunkMapIndex::new(vec![1]),
//             SingleArrayLookup {
//                 file_chunk_index: 3,
//                 range: 0..2,
//                 shape: vec![2],
//             },
//         );

//         let mut lookup_table = BTreeMap::new();
//         lookup_table.insert(
//             11,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![2],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Chunked {
//                     map: Box::new(chunk_map_one),
//                 },
//             },
//         );
//         lookup_table.insert(
//             22,
//             ArrayEntry {
//                 shape: vec![4],
//                 chunk_shape: vec![2],
//                 dimensions: vec!["x".to_string()],
//                 fill_value: None,
//                 lookup: ArrayLookup::Chunked {
//                     map: Box::new(chunk_map_two),
//                 },
//             },
//         );

//         let reader = open_reader(
//             "multi-chunked-entry.bin",
//             vec![
//                 TypedVec::I32(vec![1, 2]),
//                 TypedVec::I32(vec![3, 4]),
//                 TypedVec::I32(vec![10, 11]),
//                 TypedVec::I32(vec![12, 13]),
//             ],
//             lookup_table,
//         )
//         .await?;

//         let backend_one =
//             AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader.clone(), 11)?;
//         let backend_two =
//             AtlasArrayFileBackend::<Arc<dyn ObjectStore>, i32>::new(reader.clone(), 22)?;

//         let subset_one = backend_one
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;
//         let subset_two = backend_two
//             .read_subset(ArraySubset {
//                 start: vec![1],
//                 shape: vec![2],
//             })
//             .await?;

//         assert_eq!(subset_one.iter().copied().collect::<Vec<_>>(), vec![2, 3]);
//         assert_eq!(subset_two.iter().copied().collect::<Vec<_>>(), vec![11, 12]);

//         let lazy_one = reader
//             .read_entry_array::<i32>(11)?
//             .subset(ArraySubset {
//                 start: vec![0],
//                 shape: vec![4],
//             })
//             .await?;
//         let lazy_two = reader
//             .read_entry_array::<i32>(22)?
//             .subset(ArraySubset {
//                 start: vec![0],
//                 shape: vec![4],
//             })
//             .await?;

//         assert_eq!(lazy_one.into_raw_vec().await, vec![1, 2, 3, 4]);
//         assert_eq!(lazy_two.into_raw_vec().await, vec![10, 11, 12, 13]);

//         Ok(())
//     }
// }
