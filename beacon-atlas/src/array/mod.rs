use std::sync::Arc;

use anyhow::anyhow;
use beacon_nd_arrow::{
    NdArrowArray, NdIndex, concat_nd,
    dimensions::{Dimension, Dimensions},
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};

use crate::{array::store::ChunkStore, config};

pub mod io_cache;
pub mod layout;
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

/// A single chunk and its chunk index within the overall array.
#[derive(Debug, Clone)]
pub struct ArrayPart {
    pub array: NdArrowArray,
    pub chunk_index: Vec<usize>,
}

impl<S: ChunkStore + Send + Sync> Array<S> {
    /// Returns the next chunked array part from the stream.
    pub fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
        self.chunk_provider.chunks()
    }

    /// Fetches a chunk by its logical chunk index.
    pub async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
        self.chunk_provider.fetch_chunk(chunk_index).await
    }

    /// Fetches a logical slice by retrieving all intersecting chunks and
    /// stitching them together, similar to zarr chunk assembly.
    ///
    /// Missing chunks are represented as null-filled arrays, and edge chunks
    /// are allowed to be smaller than the nominal `chunk_shape`.
    pub async fn fetch_sliced(
        &self,
        index: Vec<usize>, // element index to start slicing at along each dimension
        counts: Vec<usize>, // number of elements to read along each dimension
    ) -> anyhow::Result<Option<ArrayPart>> {
        // Determine which chunk indices intersect the requested logical slice.
        let ranges = compute_chunk_ranges(&index, &counts, &self.chunk_shape, &self.array_shape)?;
        let chunk_indices = expand_chunk_indices(&ranges);

        if chunk_indices.is_empty() {
            return Ok(None);
        }

        // Fast path: a single chunk can resolve the requested slice.
        if let Some(chunk_index) = single_chunk_index(&ranges) {
            return self
                .fetch_single_chunk_slice(chunk_index, &index, &counts)
                .await;
        }

        // Fetch all intersecting chunks concurrently to minimize latency.
        let fetched_parts =
            fetch_parts_concurrently(self, &chunk_indices, config::chunk_fetch_concurrency())
                .await?;
        let dim_names = resolve_dim_names(&fetched_parts, &counts);
        let dims_from_names = |shape: &[usize]| dims_from_names(&dim_names, shape);
        let slices = build_chunk_slices(
            &chunk_indices,
            fetched_parts,
            &index,
            &counts,
            &self.chunk_shape,
            &self.array_shape,
            &self.array_datatype,
            &dims_from_names,
        )?;

        let stitched = stitch_slices(slices, &ranges)?;
        let slice = rebuild_output(stitched, &dim_names, &counts)?;

        // Return the slice with the logical starting chunk index for reference.
        let start_chunk_index = ranges.iter().map(|(start, _)| *start).collect();
        Ok(Some(ArrayPart {
            array: slice,
            chunk_index: start_chunk_index,
        }))
    }

    async fn fetch_single_chunk_slice(
        &self,
        chunk_index: Vec<usize>,
        index: &[usize],
        counts: &[usize],
    ) -> anyhow::Result<Option<ArrayPart>> {
        // Fetch the single chunk and slice it directly.
        let part = self.fetch_chunk(chunk_index.clone()).await?;
        let part = match part {
            Some(part) => part,
            None => return Ok(None),
        };

        let slice_indices = build_chunk_slice_indices(
            index,
            counts,
            &self.chunk_shape,
            &self.array_shape,
            &chunk_index,
        );
        let slice_shape: Vec<usize> = slice_indices
            .iter()
            .map(|idx| match idx {
                NdIndex::Slice { len, .. } => *len,
                NdIndex::Index { .. } => 1,
            })
            .collect();
        let sliced = part
            .array
            .slice_nd(&slice_indices)
            .map_err(|err| anyhow!(err))?;

        let dim_names = part
            .array
            .dimensions()
            .as_multi_dimensional()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|d| d.name().to_string())
            .collect::<Vec<_>>();
        let dims = dims_from_names(&dim_names, &slice_shape)?;
        let normalized =
            NdArrowArray::new(sliced.values().clone(), dims).map_err(|err| anyhow!(err))?;

        Ok(Some(ArrayPart {
            array: normalized,
            chunk_index,
        }))
    }
}

/// Returns the chunk index when the request maps to a single chunk.
fn single_chunk_index(ranges: &[(usize, usize)]) -> Option<Vec<usize>> {
    if ranges.is_empty() {
        return None;
    }
    let mut chunk_index = Vec::with_capacity(ranges.len());
    for (start, end) in ranges.iter() {
        if start != end {
            return None;
        }
        chunk_index.push(*start);
    }
    Some(chunk_index)
}

async fn fetch_parts_concurrently<S: ChunkStore + Send + Sync>(
    array: &Array<S>,
    chunk_indices: &[Vec<usize>],
    concurrency: usize,
) -> anyhow::Result<Vec<Option<ArrayPart>>> {
    let fetches = stream::iter(chunk_indices.iter().cloned().enumerate())
        .map(|(pos, chunk_index)| async move {
            let part = array.fetch_chunk(chunk_index).await?;
            Ok::<_, anyhow::Error>((pos, part))
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut fetched_parts = vec![None; chunk_indices.len()];
    for result in fetches {
        let (pos, part) = result?;
        fetched_parts[pos] = part;
    }
    Ok(fetched_parts)
}

fn resolve_dim_names(fetched_parts: &[Option<ArrayPart>], counts: &[usize]) -> Vec<String> {
    if let Some(part) = fetched_parts.iter().flatten().next() {
        return part
            .array
            .dimensions()
            .as_multi_dimensional()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|d| d.name().to_string())
            .collect::<Vec<_>>();
    }

    (0..counts.len()).map(|i| format!("dim_{i}")).collect()
}

fn dims_from_names(dim_names: &[String], shape: &[usize]) -> anyhow::Result<Dimensions> {
    if shape.is_empty() {
        return Ok(Dimensions::new_scalar());
    }
    if shape.len() != dim_names.len() {
        return Err(anyhow!("dimension names do not match slice rank"));
    }
    let dims = dim_names
        .iter()
        .cloned()
        .zip(shape.iter().copied())
        .map(|(name, size)| Dimension::try_new(name, size))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| anyhow!(err))?;
    Ok(Dimensions::new(dims))
}

#[allow(clippy::too_many_arguments)]
fn build_chunk_slices(
    chunk_indices: &[Vec<usize>],
    fetched_parts: Vec<Option<ArrayPart>>,
    index: &[usize],
    counts: &[usize],
    chunk_shape: &[usize],
    array_shape: &[usize],
    array_datatype: &arrow::datatypes::DataType,
    dims_from_names: &impl Fn(&[usize]) -> anyhow::Result<Dimensions>,
) -> anyhow::Result<Vec<NdArrowArray>> {
    let mut slices: Vec<NdArrowArray> = Vec::with_capacity(chunk_indices.len());
    for (chunk_index, part) in chunk_indices.iter().zip(fetched_parts.into_iter()) {
        // Compute the slice indices for this chunk, accounting for smaller edge chunks.
        let slice_indices =
            build_chunk_slice_indices(index, counts, chunk_shape, array_shape, chunk_index);
        // Convert slice indices to an explicit shape for the chunk sub-slice.
        let slice_shape: Vec<usize> = slice_indices
            .iter()
            .map(|idx| match idx {
                NdIndex::Slice { len, .. } => *len,
                NdIndex::Index { .. } => 1,
            })
            .collect();

        // Either slice an existing chunk or synthesize a null-filled chunk.
        let sliced = match part {
            Some(part) => part
                .array
                .slice_nd(&slice_indices)
                .map_err(|err| anyhow!(err))?,
            None => {
                let len = if slice_shape.is_empty() {
                    1
                } else {
                    slice_shape.iter().product()
                };
                let values = arrow::array::new_null_array(array_datatype, len);
                let dims = dims_from_names(&slice_shape)?;
                NdArrowArray::new(values, dims).map_err(|err| anyhow!(err))?
            }
        };

        // Normalize dimensions so concatenation can assume consistent names.
        let dims = dims_from_names(&slice_shape)?;
        let normalized =
            NdArrowArray::new(sliced.values().clone(), dims).map_err(|err| anyhow!(err))?;
        slices.push(normalized);
    }
    Ok(slices)
}

fn stitch_slices(
    slices: Vec<NdArrowArray>,
    ranges: &[(usize, usize)],
) -> anyhow::Result<NdArrowArray> {
    // Stitch chunks together along each axis, starting from the last axis.
    let mut current = slices;
    let chunk_counts: Vec<usize> = ranges.iter().map(|(start, end)| end - start + 1).collect();

    for axis in (0..chunk_counts.len()).rev() {
        let group_size = chunk_counts[axis];
        let mut next = Vec::new();
        for group in current.chunks(group_size) {
            let merged = concat_nd(group, axis).map_err(|err| anyhow!(err))?;
            next.push(merged);
        }
        current = next;
    }

    current
        .pop()
        .ok_or_else(|| anyhow!("missing sliced result"))
}

fn rebuild_output(
    slice: NdArrowArray,
    dim_names: &[String],
    counts: &[usize],
) -> anyhow::Result<NdArrowArray> {
    // Rebuild the final array with the requested output shape.
    let out_shape = counts.to_vec();
    let dims = if out_shape.is_empty() {
        Dimensions::new_scalar()
    } else {
        let dims = dim_names
            .iter()
            .cloned()
            .zip(out_shape.iter().copied())
            .map(|(name, size)| Dimension::try_new(name, size))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow!(err))?;
        Dimensions::new(dims)
    };
    NdArrowArray::new(slice.values().clone(), dims).map_err(|err| anyhow!(err))
}

/// Computes the inclusive chunk-index ranges intersecting the logical slice.
fn compute_chunk_ranges(
    index: &[usize],
    counts: &[usize],
    chunk_shape: &[usize],
    array_shape: &[usize],
) -> anyhow::Result<Vec<(usize, usize)>> {
    if index.len() != counts.len() {
        return Err(anyhow!("index and counts must have the same length"));
    }
    if index.len() != chunk_shape.len() || index.len() != array_shape.len() {
        return Err(anyhow!("slice dimensions do not match array shape"));
    }

    let mut ranges = Vec::with_capacity(index.len());
    for ((start, count), (chunk_dim, array_dim)) in index
        .iter()
        .zip(counts.iter())
        .zip(chunk_shape.iter().zip(array_shape.iter()))
    {
        if *count == 0 {
            return Err(anyhow!("slice counts must be non-zero"));
        }
        if start.saturating_add(*count) > *array_dim {
            return Err(anyhow!(
                "slice [{}, {}) out of bounds for array dimension {}",
                start,
                start.saturating_add(*count),
                array_dim
            ));
        }
        let chunk_start = start / chunk_dim;
        let chunk_end = (start.saturating_add(*count).saturating_sub(1)) / chunk_dim;
        ranges.push((chunk_start, chunk_end));
    }
    Ok(ranges)
}

/// Expands per-dimension chunk ranges into a full list of chunk indices.
fn expand_chunk_indices(ranges: &[(usize, usize)]) -> Vec<Vec<usize>> {
    let mut indices = Vec::new();
    let mut current = vec![0usize; ranges.len()];
    for (i, (start, _)) in ranges.iter().enumerate() {
        current[i] = *start;
    }

    loop {
        indices.push(current.clone());
        let mut dim = ranges.len();
        while dim > 0 {
            dim -= 1;
            let (start, end) = ranges[dim];
            if current[dim] < end {
                current[dim] += 1;
                for reset in dim + 1..ranges.len() {
                    current[reset] = ranges[reset].0;
                }
                break;
            }
            if dim == 0 {
                dim = usize::MAX;
                break;
            }
        }
        if dim == usize::MAX {
            break;
        }
    }
    indices
}

/// Builds per-dimension slice indices for a specific chunk, respecting edge sizes.
fn build_chunk_slice_indices(
    index: &[usize],
    counts: &[usize],
    chunk_shape: &[usize],
    array_shape: &[usize],
    chunk_index: &[usize],
) -> Vec<NdIndex> {
    index
        .iter()
        .zip(counts.iter())
        .zip(chunk_shape.iter().zip(array_shape.iter()))
        .zip(chunk_index.iter())
        .map(|(((start, count), (chunk_dim, array_dim)), chunk_idx)| {
            let chunk_start = chunk_idx * chunk_dim;
            let chunk_len = if chunk_start >= *array_dim {
                0
            } else {
                (*array_dim - chunk_start).min(*chunk_dim)
            };
            let slice_start = start.saturating_sub(chunk_start).min(chunk_len);
            let slice_end = (start + count).min(chunk_start + chunk_len) - chunk_start;
            NdIndex::Slice {
                start: slice_start,
                len: slice_end - slice_start,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Array, ArrayPart, ChunkStore};
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use beacon_nd_arrow::NdArrowArray;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};

    #[derive(Debug, Clone)]
    struct TestStore {
        parts: HashMap<Vec<usize>, ArrayPart>,
    }

    #[async_trait::async_trait]
    impl ChunkStore for TestStore {
        fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayPart>> {
            let parts: Vec<ArrayPart> = self.parts.values().cloned().collect();
            stream::iter(parts.into_iter().map(Ok)).boxed()
        }

        async fn fetch_chunk(&self, chunk_index: Vec<usize>) -> anyhow::Result<Option<ArrayPart>> {
            Ok(self.parts.get(&chunk_index).cloned())
        }
    }

    fn make_array(values: Vec<i32>, shape: Vec<usize>, names: Vec<&str>) -> NdArrowArray {
        let dims = names
            .into_iter()
            .zip(shape.into_iter())
            .map(|(name, size)| Dimension::try_new(name, size).unwrap())
            .collect::<Vec<_>>();
        NdArrowArray::new(Arc::new(Int32Array::from(values)), Dimensions::new(dims)).unwrap()
    }

    #[tokio::test]
    async fn fetch_sliced_across_chunks_1d() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        parts.insert(
            vec![0],
            ArrayPart {
                array: make_array(vec![1, 2], vec![2], vec!["x"]),
                chunk_index: vec![0],
            },
        );
        parts.insert(
            vec![1],
            ArrayPart {
                array: make_array(vec![3, 4], vec![2], vec!["x"]),
                chunk_index: vec![1],
            },
        );

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape: vec![2],
            array_shape: vec![4],
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![1], vec![3])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[2, 3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_sliced_across_chunks_2d() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        for cy in 0..2 {
            for cx in 0..2 {
                let mut values = Vec::new();
                for y in 0..2 {
                    for x in 0..2 {
                        let gy = cy * 2 + y;
                        let gx = cx * 2 + x;
                        values.push((gy * 4 + gx) as i32);
                    }
                }
                parts.insert(
                    vec![cy, cx],
                    ArrayPart {
                        array: make_array(values, vec![2, 2], vec!["y", "x"]),
                        chunk_index: vec![cy, cx],
                    },
                );
            }
        }

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape: vec![2, 2],
            array_shape: vec![4, 4],
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![1, 1], vec![3, 3])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[5, 6, 7, 9, 10, 11, 13, 14, 15]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_sliced_single_chunk_partial_slice() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        parts.insert(
            vec![0],
            ArrayPart {
                array: make_array(vec![10, 20, 30, 40], vec![4], vec!["x"]),
                chunk_index: vec![0],
            },
        );

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape: vec![4],
            array_shape: vec![4],
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![1], vec![2])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[20, 30]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_sliced_handles_smaller_edge_chunks() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        parts.insert(
            vec![0],
            ArrayPart {
                array: make_array(vec![1, 2], vec![2], vec!["x"]),
                chunk_index: vec![0],
            },
        );
        parts.insert(
            vec![1],
            ArrayPart {
                array: make_array(vec![3], vec![1], vec!["x"]),
                chunk_index: vec![1],
            },
        );

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape: vec![2],
            array_shape: vec![3],
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![0], vec![3])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_sliced_handles_smaller_edge_chunks_2d() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        let chunk_shape = vec![2usize, 2usize];
        let array_shape = vec![3usize, 3usize];

        for cy in 0..2 {
            for cx in 0..2 {
                let chunk_start_y = cy * chunk_shape[0];
                let chunk_start_x = cx * chunk_shape[1];
                let chunk_len_y =
                    (array_shape[0].saturating_sub(chunk_start_y)).min(chunk_shape[0]);
                let chunk_len_x =
                    (array_shape[1].saturating_sub(chunk_start_x)).min(chunk_shape[1]);
                if chunk_len_y == 0 || chunk_len_x == 0 {
                    continue;
                }

                let mut values = Vec::new();
                for y in 0..chunk_len_y {
                    for x in 0..chunk_len_x {
                        let gy = chunk_start_y + y;
                        let gx = chunk_start_x + x;
                        values.push((gy * array_shape[1] + gx) as i32);
                    }
                }

                parts.insert(
                    vec![cy, cx],
                    ArrayPart {
                        array: make_array(values, vec![chunk_len_y, chunk_len_x], vec!["y", "x"]),
                        chunk_index: vec![cy, cx],
                    },
                );
            }
        }

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape,
            array_shape,
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![0, 0], vec![3, 3])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_sliced_handles_smaller_edge_chunks_3d() -> anyhow::Result<()> {
        let mut parts = HashMap::new();
        let chunk_shape = vec![2usize, 2usize, 2usize];
        let array_shape = vec![3usize, 4usize, 3usize];

        for cz in 0..2 {
            for cy in 0..2 {
                for cx in 0..2 {
                    let chunk_start_z = cz * chunk_shape[0];
                    let chunk_start_y = cy * chunk_shape[1];
                    let chunk_start_x = cx * chunk_shape[2];
                    let chunk_len_z =
                        (array_shape[0].saturating_sub(chunk_start_z)).min(chunk_shape[0]);
                    let chunk_len_y =
                        (array_shape[1].saturating_sub(chunk_start_y)).min(chunk_shape[1]);
                    let chunk_len_x =
                        (array_shape[2].saturating_sub(chunk_start_x)).min(chunk_shape[2]);
                    if chunk_len_z == 0 || chunk_len_y == 0 || chunk_len_x == 0 {
                        continue;
                    }

                    let mut values = Vec::new();
                    for z in 0..chunk_len_z {
                        for y in 0..chunk_len_y {
                            for x in 0..chunk_len_x {
                                let gz = chunk_start_z + z;
                                let gy = chunk_start_y + y;
                                let gx = chunk_start_x + x;
                                values.push(
                                    (gz * array_shape[1] * array_shape[2]
                                        + gy * array_shape[2]
                                        + gx) as i32,
                                );
                            }
                        }
                    }

                    parts.insert(
                        vec![cz, cy, cx],
                        ArrayPart {
                            array: make_array(
                                values,
                                vec![chunk_len_z, chunk_len_y, chunk_len_x],
                                vec!["z", "y", "x"],
                            ),
                            chunk_index: vec![cz, cy, cx],
                        },
                    );
                }
            }
        }

        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            chunk_shape,
            array_shape,
            chunk_provider: TestStore { parts },
        };

        let sliced = array
            .fetch_sliced(vec![0, 0, 0], vec![3, 4, 3])
            .await?
            .expect("slice exists");
        let values = sliced
            .array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let expected: Vec<i32> = (0..(3 * 4 * 3) as i32).collect();
        assert_eq!(values.values(), expected.as_slice());
        Ok(())
    }
}
