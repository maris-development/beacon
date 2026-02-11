use std::sync::Arc;

use futures::StreamExt;

use crate::array::{nd::NdArray, store::ChunkStore};

pub mod buffer;
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
    pub array_shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub chunk_provider: S,
}

impl<S: ChunkStore + Send + Sync> Array<S> {
    pub async fn fetch(&self) -> Arc<dyn NdArray> {
        //Fetch all chunks, concat using arrow
        let all_chunks = self.chunk_provider.chunks().collect::<Vec<_>>().await;

        todo!()
    }
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
