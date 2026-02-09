use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::BoxStream;

use crate::array::{chunk::ArrayChunk, nd::NdArray, store::ChunkStore};

pub mod buffer;
pub mod chunk;
pub mod data_type;
pub mod io_cache;
pub mod layout;
pub mod nd;
pub mod pruning;
pub mod reader;
pub mod store;
pub mod writer;

/// A stream of chunked ND arrays with a shared element type and chunk shape.
#[derive(Debug, Clone)]
pub struct Array<S: ChunkStore + Send + Sync> {
    pub array_datatype: arrow::datatypes::DataType,
    pub chunk_shape: Vec<usize>,
    pub array_shape: Vec<usize>,
    pub chunk_provider: S,
}

#[derive(Debug, Clone)]
pub struct ArraySubset {
    start: Vec<usize>,
    shape: Vec<usize>,
}

impl<S: ChunkStore + Send + Sync> Array<S> {
    /// Returns the next chunked array part from the stream.
    pub fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayChunk>> {
        self.chunk_provider.chunks()
    }

    /// Returns true if the array is fully chunked (i.e., each chunk covers the entire array).
    pub fn is_single_chunk(&self) -> bool {
        self.chunk_shape == self.array_shape
    }

    /// Fetches a chunk by its logical chunk index.
    pub async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayChunk>> {
        self.chunk_provider.fetch_chunk(chunk_index).await
    }

    pub fn chunk_subsets(&self) -> Vec<ArraySubset> {
        if self.array_shape.len() != self.chunk_shape.len() {
            return Vec::new();
        }
        if self.array_shape.is_empty() {
            return vec![ArraySubset {
                start: Vec::new(),
                shape: Vec::new(),
            }];
        }
        if self.chunk_shape.contains(&0) {
            return Vec::new();
        }

        let chunk_counts: Vec<usize> = self
            .array_shape
            .iter()
            .zip(self.chunk_shape.iter())
            .map(|(array_dim, chunk_dim)| array_dim.div_ceil(*chunk_dim))
            .collect();

        let mut indices = vec![0usize; chunk_counts.len()];
        let mut subsets = Vec::new();

        loop {
            let mut start = Vec::with_capacity(indices.len());
            let mut shape = Vec::with_capacity(indices.len());
            for ((idx, chunk_dim), array_dim) in indices
                .iter()
                .zip(self.chunk_shape.iter())
                .zip(self.array_shape.iter())
            {
                let offset = idx * chunk_dim;
                if offset >= *array_dim {
                    shape.push(0);
                } else {
                    shape.push((*array_dim - offset).min(*chunk_dim));
                }
                start.push(offset);
            }
            subsets.push(ArraySubset { start, shape });

            let mut dim = indices.len();
            while dim > 0 {
                dim -= 1;
                if indices[dim] + 1 < chunk_counts[dim] {
                    indices[dim] += 1;
                    for reset in dim + 1..indices.len() {
                        indices[reset] = 0;
                    }
                    break;
                }
                if dim == 0 {
                    return subsets;
                }
            }
        }
    }

    pub fn determine_chunk_indices(&self, subset: ArraySubset) -> anyhow::Result<Vec<Vec<usize>>> {
        let num_dims = subset.start.len();
        if num_dims != subset.shape.len()
            || num_dims != self.chunk_shape.len()
            || num_dims != self.array_shape.len()
        {
            return Err(anyhow!("subset dimensionality does not match array"));
        }
        if num_dims == 0 {
            return Ok(vec![Vec::new()]);
        }

        let mut chunk_ranges = Vec::with_capacity(num_dims);
        for dim in 0..num_dims {
            let start = subset.start[dim];
            let len = subset.shape[dim];
            let array_dim = self.array_shape[dim];
            let chunk_dim = self.chunk_shape[dim];

            if chunk_dim == 0 {
                return Err(anyhow!("chunk dimension cannot be zero"));
            }
            if len == 0 || start.saturating_add(len) > array_dim {
                return Err(anyhow!("subset out of bounds"));
            }

            let first = start / chunk_dim;
            let last = (start + len - 1) / chunk_dim;
            chunk_ranges.push((first, last));
        }

        let mut indices = Vec::with_capacity(num_dims);
        for (start, _end) in &chunk_ranges {
            indices.push(*start);
        }
        let mut chunk_indices = Vec::new();
        loop {
            chunk_indices.push(indices.clone());
            let mut dim = num_dims;
            let mut carried = true;
            while dim > 0 && carried {
                dim -= 1;
                let (start, end) = chunk_ranges[dim];
                if indices[dim] < end {
                    indices[dim] += 1;
                    for reset in dim + 1..num_dims {
                        indices[reset] = chunk_ranges[reset].0;
                    }
                    carried = false;
                } else {
                    indices[dim] = start;
                }
            }
            if carried {
                break;
            }
        }
        Ok(chunk_indices)
    }

    /// Fetches a subset of the array that is fully contained within a single chunk. Returns an error if failed to retreived. None if the chunk is missing.
    pub async fn subset_within_chunk(
        &self,
        chunk_index: Vec<usize>,
        subset: ArraySubset,
    ) -> anyhow::Result<Option<Arc<dyn NdArray>>> {
        todo!()
    }

    // pub async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Option<NdArrowArray>> {
    //     // Determine which chunk indices intersect the requested subset.
    //     let chunk_indices: Vec<Vec<usize>> = self.determine_chunk_indices(subset.clone())?;

    //     if chunk_indices.len() == 1 {
    //         // Fast path for single chunk: just fetch and slice it.
    //         return self
    //             .subset_within_chunk(chunk_indices[0].clone(), subset)
    //             .await;
    //     } else {
    //         // For multiple chunks, we need to fetch them all, stitch them together, and then slice the combined array.
    //         // This is more complex but avoids multiple slicing operations.
    //     }

    //     // Read all the chunks, stitch them together, and extract the final subset. Use the concat function to stitch together the chunks.
    //     let num_dims = subset.start.len();
    //     if num_dims == 0 {
    //         let part = self
    //             .fetch_chunk(Vec::new())
    //             .await?
    //             .ok_or_else(|| anyhow!("missing scalar chunk"))?;
    //         return Ok(Some(part.array));
    //     }

    //     let mut chunk_ranges = Vec::with_capacity(num_dims);
    //     for dim in 0..num_dims {
    //         let start = subset.start[dim];
    //         let len = subset.shape[dim];
    //         let array_dim = self.array_shape[dim];
    //         let chunk_dim = self.chunk_shape[dim];

    //         if chunk_dim == 0 {
    //             return Err(anyhow!("chunk dimension cannot be zero"));
    //         }
    //         if len == 0 || start.saturating_add(len) > array_dim {
    //             return Err(anyhow!("subset out of bounds"));
    //         }

    //         let first = start / chunk_dim;
    //         let last = (start + len - 1) / chunk_dim;
    //         chunk_ranges.push((first, last));
    //     }

    //     let mut chunks = std::collections::HashMap::new();
    //     for chunk_index in &chunk_indices {
    //         let part = self
    //             .fetch_chunk(chunk_index.clone())
    //             .await?
    //             .ok_or_else(|| anyhow!("missing chunk {:?}", chunk_index))?;
    //         chunks.insert(chunk_index.clone(), part.array);
    //     }

    //     fn build_combined(
    //         dim: usize,
    //         ranges: &[(usize, usize)],
    //         prefix: &mut Vec<usize>,
    //         chunks: &std::collections::HashMap<Vec<usize>, NdArrowArray>,
    //     ) -> anyhow::Result<NdArrowArray> {
    //         let (start, end) = ranges[dim];
    //         let mut arrays = Vec::new();
    //         for idx in start..=end {
    //             prefix.push(idx);
    //             let array = if dim + 1 == ranges.len() {
    //                 chunks
    //                     .get(prefix)
    //                     .cloned()
    //                     .ok_or_else(|| anyhow!("missing chunk {:?}", prefix))?
    //             } else {
    //                 build_combined(dim + 1, ranges, prefix, chunks)?
    //             };
    //             arrays.push(array);
    //             prefix.pop();
    //         }
    //         if arrays.len() == 1 {
    //             return Ok(arrays.remove(0));
    //         }
    //         concat_nd(&arrays, dim).map_err(|err| anyhow!(err))
    //     }

    //     let mut prefix = Vec::with_capacity(num_dims);
    //     let combined = build_combined(0, &chunk_ranges, &mut prefix, &chunks)?;

    //     let indices = chunk_ranges
    //         .iter()
    //         .enumerate()
    //         .map(|(dim, (start_chunk, _))| {
    //             let combined_start = start_chunk * self.chunk_shape[dim];
    //             let local_start = subset.start[dim].saturating_sub(combined_start);
    //             NdIndex::slice(local_start, subset.shape[dim])
    //         })
    //         .collect::<Vec<_>>();

    //     combined.slice_nd(&indices).map_err(|err| anyhow!(err))
    // }
}

// #[cfg(test)]
// mod tests {
//     use super::{Array, ArrayPart, ChunkStore};
//     use std::collections::HashMap;
//     use std::sync::Arc;

//     use arrow::array::Int32Array;
//     use beacon_nd_arrow::NdArrowArray;
//     use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
//     use futures::StreamExt;
//     use futures::executor::block_on;
//     use futures::stream::{self, BoxStream};

//     #[derive(Debug, Clone)]
//     struct TestStore {
//         parts: HashMap<Vec<usize>, ArrayPart>,
//     }

//     #[async_trait::async_trait]
//     impl ChunkStore for TestStore {
//         fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
//             let parts: Vec<ArrayPart> = self.parts.values().cloned().collect();
//             stream::iter(parts.into_iter().map(Ok)).boxed()
//         }

//         async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
//             Ok(self.parts.get(&chunk_index).cloned())
//         }
//     }

//     fn make_array(values: Vec<i32>, shape: Vec<usize>, names: Vec<&str>) -> NdArrowArray {
//         let dims = names
//             .into_iter()
//             .zip(shape.into_iter())
//             .map(|(name, size)| Dimension::try_new(name, size).unwrap())
//             .collect::<Vec<_>>();
//         NdArrowArray::new(Arc::new(Int32Array::from(values)), Dimensions::new(dims)).unwrap()
//     }

//     fn make_part(
//         chunk_index: Vec<usize>,
//         values: Vec<i32>,
//         shape: Vec<usize>,
//         names: Vec<&str>,
//         chunk_shape: &[usize],
//         array_shape: &[usize],
//     ) -> ArrayPart {
//         let start = chunk_index
//             .iter()
//             .zip(chunk_shape.iter())
//             .map(|(idx, dim)| idx * dim)
//             .collect::<Vec<_>>();
//         let shape = shape
//             .into_iter()
//             .zip(start.iter())
//             .zip(array_shape.iter())
//             .map(|((dim, start), array_dim)| (*array_dim - *start).min(dim))
//             .collect::<Vec<_>>();

//         ArrayPart {
//             array: make_array(values, shape.clone(), names),
//             chunk_index,
//             start,
//             shape,
//         }
//     }

//     #[test]
//     fn chunk_subsets_1d() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![2],
//             array_shape: vec![5],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subsets = array.chunk_subsets();
//         let starts: Vec<Vec<usize>> = subsets.iter().map(|s| s.start.clone()).collect();
//         let shapes: Vec<Vec<usize>> = subsets.iter().map(|s| s.shape.clone()).collect();

//         assert_eq!(starts, vec![vec![0], vec![2], vec![4]]);
//         assert_eq!(shapes, vec![vec![2], vec![2], vec![1]]);
//     }

//     #[test]
//     fn chunk_subsets_2d() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![2, 2],
//             array_shape: vec![3, 5],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subsets = array.chunk_subsets();
//         let starts: Vec<Vec<usize>> = subsets.iter().map(|s| s.start.clone()).collect();
//         let shapes: Vec<Vec<usize>> = subsets.iter().map(|s| s.shape.clone()).collect();

//         assert_eq!(
//             starts,
//             vec![
//                 vec![0, 0],
//                 vec![0, 2],
//                 vec![0, 4],
//                 vec![2, 0],
//                 vec![2, 2],
//                 vec![2, 4],
//             ]
//         );
//         assert_eq!(
//             shapes,
//             vec![
//                 vec![2, 2],
//                 vec![2, 2],
//                 vec![2, 1],
//                 vec![1, 2],
//                 vec![1, 2],
//                 vec![1, 1],
//             ]
//         );
//     }

//     #[test]
//     fn chunk_subsets_scalar() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![],
//             array_shape: vec![],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subsets = array.chunk_subsets();
//         assert_eq!(subsets.len(), 1);
//         assert!(subsets[0].start.is_empty());
//         assert!(subsets[0].shape.is_empty());
//     }

//     #[test]
//     fn determine_chunk_indices_1d() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![2],
//             array_shape: vec![5],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subset = super::ArraySubset {
//             start: vec![1],
//             shape: vec![3],
//         };
//         let indices = block_on(array.determine_chunk_indices(subset)).unwrap();
//         assert_eq!(indices, vec![vec![0], vec![1]]);
//     }

//     #[test]
//     fn determine_chunk_indices_2d() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![2, 2],
//             array_shape: vec![3, 5],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subset = super::ArraySubset {
//             start: vec![1, 2],
//             shape: vec![2, 3],
//         };
//         let indices = block_on(array.determine_chunk_indices(subset)).unwrap();
//         assert_eq!(
//             indices,
//             vec![vec![0, 1], vec![0, 2], vec![1, 1], vec![1, 2]]
//         );
//     }

//     #[test]
//     fn subset_1d() {
//         let chunk_shape = vec![2];
//         let array_shape = vec![5];
//         let names = vec!["x"];

//         let mut parts = HashMap::new();
//         parts.insert(
//             vec![0],
//             make_part(
//                 vec![0],
//                 vec![1, 2],
//                 vec![2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );
//         parts.insert(
//             vec![1],
//             make_part(
//                 vec![1],
//                 vec![3, 4],
//                 vec![2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );
//         parts.insert(
//             vec![2],
//             make_part(
//                 vec![2],
//                 vec![5],
//                 vec![1],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );

//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: chunk_shape.clone(),
//             array_shape: array_shape.clone(),
//             chunk_provider: TestStore { parts },
//         };

//         let subset = super::ArraySubset {
//             start: vec![1],
//             shape: vec![3],
//         };
//         let result = block_on(array.subset(subset)).unwrap();
//         let values = result
//             .values()
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .unwrap();
//         assert_eq!(values.values(), &[2, 3, 4]);
//         assert_eq!(result.dimensions().shape(), vec![3]);
//     }

//     #[test]
//     fn subset_2d() {
//         let chunk_shape = vec![2, 2];
//         let array_shape = vec![3, 4];
//         let names = vec!["y", "x"];

//         let mut parts = HashMap::new();
//         parts.insert(
//             vec![0, 0],
//             make_part(
//                 vec![0, 0],
//                 vec![1, 2, 5, 6],
//                 vec![2, 2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );
//         parts.insert(
//             vec![0, 1],
//             make_part(
//                 vec![0, 1],
//                 vec![3, 4, 7, 8],
//                 vec![2, 2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );
//         parts.insert(
//             vec![1, 0],
//             make_part(
//                 vec![1, 0],
//                 vec![9, 10],
//                 vec![1, 2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );
//         parts.insert(
//             vec![1, 1],
//             make_part(
//                 vec![1, 1],
//                 vec![11, 12],
//                 vec![1, 2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );

//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: chunk_shape.clone(),
//             array_shape: array_shape.clone(),
//             chunk_provider: TestStore { parts },
//         };

//         let subset = super::ArraySubset {
//             start: vec![1, 1],
//             shape: vec![2, 2],
//         };
//         let result = block_on(array.subset(subset)).unwrap();
//         let values = result
//             .values()
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .unwrap();
//         assert_eq!(values.values(), &[6, 7, 10, 11]);
//         assert_eq!(result.dimensions().shape(), vec![2, 2]);
//     }

//     #[test]
//     fn subset_within_chunk_ok() {
//         let chunk_shape = vec![2, 2];
//         let array_shape = vec![3, 4];
//         let names = vec!["y", "x"];

//         let mut parts = HashMap::new();
//         parts.insert(
//             vec![0, 0],
//             make_part(
//                 vec![0, 0],
//                 vec![1, 2, 5, 6],
//                 vec![2, 2],
//                 names.clone(),
//                 &chunk_shape,
//                 &array_shape,
//             ),
//         );

//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: chunk_shape.clone(),
//             array_shape: array_shape.clone(),
//             chunk_provider: TestStore { parts },
//         };

//         let subset = super::ArraySubset {
//             start: vec![0, 1],
//             shape: vec![2, 1],
//         };
//         let result = block_on(array.subset_within_chunk(vec![0, 0], subset))
//             .unwrap()
//             .unwrap();
//         let values = result
//             .values()
//             .as_any()
//             .downcast_ref::<Int32Array>()
//             .unwrap();
//         assert_eq!(values.values(), &[2, 6]);
//         assert_eq!(result.dimensions().shape(), vec![2, 1]);
//     }

//     #[test]
//     fn subset_within_chunk_missing() {
//         let array = Array {
//             array_datatype: arrow::datatypes::DataType::Int32,
//             chunk_shape: vec![2, 2],
//             array_shape: vec![3, 4],
//             chunk_provider: TestStore {
//                 parts: HashMap::new(),
//             },
//         };

//         let subset = super::ArraySubset {
//             start: vec![0, 0],
//             shape: vec![1, 1],
//         };
//         let result = block_on(array.subset_within_chunk(vec![0, 0], subset)).unwrap();
//         assert!(result.is_none());
//     }
// }
