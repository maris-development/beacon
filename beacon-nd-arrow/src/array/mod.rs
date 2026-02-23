use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::{concat, take};
use futures::future::try_join_all;
use futures::stream::{self, BoxStream, StreamExt};

use crate::array::{
    backend::{ArrayBackend, mem::InMemoryArrayBackend},
    subset::ArraySubset,
};

pub mod backend;
pub mod subset;

/// N-dimensional Arrow-backed array with named dimensions and virtual stride support.
///
/// The underlying storage is a 1D backend, while `shape` + `strides` define the logical
/// N-dimensional view. This enables efficient virtual transformations like broadcasting
/// without immediately materializing data.
///
/// Key capabilities:
/// - Named-dimension broadcasting (xarray-style alignment constraints)
/// - Stride-aware slicing into new materialized arrays
/// - Lazy chunked reads that respect the current logical view
/// - Full materialization to `ArrayRef` when needed
#[derive(Debug, Clone)]
pub struct NdArrowArray<B: ArrayBackend = InMemoryArrayBackend> {
    pub backend: Arc<B>,
    pub data_type: arrow::datatypes::DataType,
    pub shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub strides: Vec<isize>,
}

impl NdArrowArray {
    pub fn new_in_mem(
        array: ArrayRef,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> anyhow::Result<Self> {
        let backend = InMemoryArrayBackend::new(array.clone(), shape.clone(), dimensions.clone());
        Self::new(backend, array.data_type().clone())
    }
}

impl<B: ArrayBackend> NdArrowArray<B> {
    /// Create a new contiguous row-major ND array view over a backend.
    ///
    /// # Validation
    /// - `shape.len()` must equal `dimensions.len()`
    /// - `product(shape)` must match backend length
    ///
    /// # Errors
    /// Returns an error if validation fails.
    pub fn new(backend: B, data_type: arrow::datatypes::DataType) -> anyhow::Result<Self> {
        let shape = backend.shape();
        let dimensions = backend.dimensions();
        if shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match dimensions length {}",
                shape.len(),
                dimensions.len()
            ));
        }

        // Validate that the total size implied by the shape matches the backend length
        let total_size: usize = shape.iter().product();
        if total_size != backend.len() {
            return Err(anyhow::anyhow!(
                "Total size implied by shape {} does not match backend length {}",
                total_size,
                backend.len()
            ));
        }

        // Calculate the strides from the shape for row-major order
        let strides = Self::row_major_strides(&shape);

        Self::from_parts(Arc::new(backend), data_type, shape, dimensions, strides)
    }

    fn from_parts(
        backend: Arc<B>,
        data_type: arrow::datatypes::DataType,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        strides: Vec<isize>,
    ) -> anyhow::Result<Self> {
        if shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match dimensions length {}",
                shape.len(),
                dimensions.len()
            ));
        }
        if shape.len() != strides.len() {
            return Err(anyhow::anyhow!(
                "Shape length {} does not match strides length {}",
                shape.len(),
                strides.len()
            ));
        }

        Ok(Self {
            backend,
            data_type,
            shape,
            dimensions,
            strides,
        })
    }

    /// Compute row-major (C-order) strides for a shape.
    ///
    /// The returned strides map a multi-index into a flat index for a contiguous row-major array.
    fn row_major_strides(shape: &[usize]) -> Vec<isize> {
        if shape.is_empty() {
            return vec![];
        }
        let mut strides = vec![0isize; shape.len()];
        let mut stride = 1isize;
        for (i, dim) in shape.iter().enumerate().rev() {
            strides[i] = stride;
            stride *= *dim as isize;
        }
        strides
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.shape.iter().product()
    }

    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    pub fn dimensions(&self) -> &[String] {
        &self.dimensions
    }

    pub fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    /// Materialize the current logical ND view as a flat `ArrayRef`.
    ///
    /// For contiguous row-major views, this forwards directly to the backend.
    /// For virtual/strided views (for example after broadcasting), this gathers
    /// elements according to logical indices and current strides.
    ///
    /// # Errors
    /// Returns an error if backend access or gather operation fails.
    pub async fn as_arrow_array_ref(&self) -> anyhow::Result<ArrayRef> {
        if self.is_contiguous_row_major() {
            return self.backend.slice(0, self.len()).await;
        }

        let source_values = self.backend.slice(0, self.backend.len()).await?;
        let source_indices = Self::broadcast_source_indices(&self.shape, &self.strides);
        let indices = UInt64Array::from(
            source_indices
                .into_iter()
                .map(|i| i as u64)
                .collect::<Vec<_>>(),
        );
        Ok(take(source_values.as_ref(), &indices, None)?)
    }

    /// Lazily read the logical array in flat row-major chunks.
    ///
    /// The stream is lazy: backend reads occur only as the stream is polled.
    ///
    /// Behavior:
    /// - Contiguous views read each chunk with a direct backend slice.
    /// - Strided/virtual views compute source indices for each chunk and materialize
    ///   only that chunk.
    ///
    /// The returned stream yields per-chunk `Result<ArrayRef>` so backend/materialization
    /// failures are propagated lazily.
    ///
    /// # Errors
    /// Returns an immediate error when `chunk_size == 0`.
    pub fn read_chunked(
        &self,
        chunk_size: usize,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<ArrayRef>>> {
        if chunk_size == 0 {
            return Err(anyhow::anyhow!("chunk_size must be greater than 0"));
        }

        let total_len = self.len();
        let is_contiguous = self.is_contiguous_row_major();
        let backend = Arc::clone(&self.backend);
        let shape = self.shape.clone();
        let strides = self.strides.clone();

        let stream = stream::unfold(0usize, move |position| {
            let backend = Arc::clone(&backend);
            let shape = shape.clone();
            let strides = strides.clone();

            async move {
                if position >= total_len {
                    return None;
                }

                let current_chunk_len = chunk_size.min(total_len - position);
                let next_position = position + current_chunk_len;

                let chunk_result = if is_contiguous {
                    backend.slice(position, current_chunk_len).await
                } else {
                    let indices =
                        Self::chunk_source_indices(&shape, &strides, position, current_chunk_len);
                    let ranges = Self::coalesce_linear_indices_to_ranges(&indices);
                    Self::materialize_backend_ranges_from_backend(&backend, &ranges).await
                };

                Some((chunk_result, next_position))
            }
        });

        Ok(stream.boxed())
    }

    /// Create a virtual broadcasted view with named-dimension alignment.
    ///
    /// This operation does not materialize values; it returns a new view sharing
    /// the same backend while updating `shape` and `strides`.
    ///
    /// Named-dimension rules (xarray-style):
    /// - Input dimensions must be present in target dimensions.
    /// - Input dimension order must be preserved as a subsequence.
    /// - Size compatibility per matched dim is `same` or `1 -> N`.
    /// - Missing target dims are treated as broadcast axes (stride 0).
    ///
    /// # Errors
    /// Returns an error if rank, names, order, or sizes are incompatible.
    pub fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> anyhow::Result<NdArrowArray<B>> {
        self.validate_broadcast_request(target_shape, dimensions)?;
        let out_strides = self.compute_broadcast_out_strides(target_shape, dimensions)?;

        NdArrowArray::from_parts(
            Arc::clone(&self.backend),
            self.data_type.clone(),
            target_shape.to_vec(),
            dimensions.to_vec(),
            out_strides,
        )
    }

    fn is_contiguous_row_major(&self) -> bool {
        self.strides == Self::row_major_strides(&self.shape) && self.backend.len() == self.len()
    }

    fn validate_broadcast_request(
        &self,
        target_shape: &[usize],
        target_dimensions: &[String],
    ) -> anyhow::Result<()> {
        if target_shape.len() != target_dimensions.len() {
            return Err(anyhow::anyhow!(
                "Target shape rank {} does not match target dimensions rank {}",
                target_shape.len(),
                target_dimensions.len()
            ));
        }

        Self::validate_unique_dimension_names(&self.dimensions)?;
        Self::validate_unique_dimension_names(target_dimensions)?;

        let target_positions: HashMap<&str, usize> = target_dimensions
            .iter()
            .map(String::as_str)
            .enumerate()
            .map(|(idx, name)| (name, idx))
            .collect();

        for dim in &self.dimensions {
            if !target_positions.contains_key(dim.as_str()) {
                return Err(anyhow::anyhow!(
                    "Input dimension '{}' is missing from target dimensions",
                    dim
                ));
            }
        }

        let mut last_pos = None;
        for dim in &self.dimensions {
            let pos = *target_positions.get(dim.as_str()).unwrap();
            if let Some(prev) = last_pos {
                if pos <= prev {
                    return Err(anyhow::anyhow!(
                        "Target dimensions reorder input dimensions; expected xarray-like subsequence order"
                    ));
                }
            }
            last_pos = Some(pos);
        }

        Ok(())
    }

    fn validate_unique_dimension_names(dimensions: &[String]) -> anyhow::Result<()> {
        let mut seen = HashSet::new();
        for dim in dimensions {
            if !seen.insert(dim.as_str()) {
                return Err(anyhow::anyhow!("Duplicate dimension name '{}'", dim));
            }
        }
        Ok(())
    }

    fn compute_broadcast_out_strides(
        &self,
        target_shape: &[usize],
        target_dimensions: &[String],
    ) -> anyhow::Result<Vec<isize>> {
        let mut input_by_name: HashMap<&str, (usize, isize)> = HashMap::new();
        for (idx, dim) in self.dimensions.iter().enumerate() {
            input_by_name.insert(dim.as_str(), (self.shape[idx], self.strides[idx]));
        }

        let mut out_strides = Vec::with_capacity(target_shape.len());
        for (target_idx, target_dim) in target_dimensions.iter().enumerate() {
            let target_size = target_shape[target_idx];
            match input_by_name.get(target_dim.as_str()) {
                None => {
                    out_strides.push(0);
                }
                Some((input_size, input_stride)) => {
                    if *input_size == target_size {
                        out_strides.push(*input_stride);
                    } else if *input_size == 1 && target_size >= 1 {
                        out_strides.push(0);
                    } else {
                        return Err(anyhow::anyhow!(
                            "Dimension '{}' is not broadcast-compatible: input size {}, target size {}",
                            target_dim,
                            input_size,
                            target_size
                        ));
                    }
                }
            }
        }

        Ok(out_strides)
    }

    fn broadcast_source_indices(target_shape: &[usize], out_strides: &[isize]) -> Vec<usize> {
        let rank = target_shape.len();
        if rank == 0 {
            return vec![0usize];
        }

        let out_len: usize = target_shape.iter().product();
        if out_len == 0 {
            return Vec::new();
        }

        let mut indices = Vec::with_capacity(out_len);
        let mut offset = vec![0usize; rank];

        loop {
            let source_idx = offset
                .iter()
                .enumerate()
                .map(|(axis, idx)| *idx as isize * out_strides[axis])
                .sum::<isize>() as usize;
            indices.push(source_idx);

            let mut carry_axis = rank;
            while carry_axis > 0 {
                let axis = carry_axis - 1;
                offset[axis] += 1;
                if offset[axis] < target_shape[axis] {
                    break;
                }
                offset[axis] = 0;
                carry_axis -= 1;
            }

            if carry_axis == 0 {
                break;
            }
        }

        indices
    }

    /// Slices the array according to the provided subset, returning a new NdArrowArray with the sliced data.
    ///
    /// This method validates the subset, computes stride-aware source indices,
    /// coalesces adjacent indices into backend ranges, and materializes the slice.
    ///
    /// The result is materialized into an `InMemoryArrayBackend` with a contiguous
    /// row-major layout for the subset shape.
    ///
    /// # Errors
    /// - Returns an error if the subset is invalid (e.g., out of bounds, rank mismatch).
    /// - Returns an error if there is an issue with slicing the backend data.
    pub async fn slice(&self, subset: ArraySubset) -> anyhow::Result<NdArrowArray> {
        if subset.start.iter().all(|&s| s == 0) && subset.shape == self.shape {
            let values = self.as_arrow_array_ref().await?;
            // Subset is the entire array, return self
            return NdArrowArray::new(
                InMemoryArrayBackend::new(values, self.shape.clone(), self.dimensions.clone()),
                self.data_type.clone(),
            );
        }

        let target_shape = subset.shape.clone();
        let ranges = self.backend_slice_ranges(&subset)?;
        let sliced_values = self.materialize_backend_ranges(&ranges).await?;

        NdArrowArray::new(
            InMemoryArrayBackend::new(sliced_values, target_shape.clone(), self.dimensions.clone()),
            self.data_type.clone(),
        )
    }

    fn backend_slice_ranges(&self, subset: &ArraySubset) -> anyhow::Result<Vec<(usize, usize)>> {
        self.validate_subset(subset)?;
        let linear_indices = self.subset_linear_indices(subset);
        Ok(Self::coalesce_linear_indices_to_ranges(&linear_indices))
    }

    fn validate_subset(&self, subset: &ArraySubset) -> anyhow::Result<()> {
        let rank = self.shape.len();
        if subset.start.len() != rank {
            return Err(anyhow::anyhow!(
                "Subset start rank {} does not match array rank {}",
                subset.start.len(),
                rank
            ));
        }
        if subset.shape.len() != rank {
            return Err(anyhow::anyhow!(
                "Subset shape rank {} does not match array rank {}",
                subset.shape.len(),
                rank
            ));
        }

        for axis in 0..rank {
            let start = subset.start[axis];
            let length = subset.shape[axis];
            let size = self.shape[axis];
            if start > size {
                return Err(anyhow::anyhow!(
                    "Subset start {} exceeds axis {} size {}",
                    start,
                    axis,
                    size
                ));
            }
            if length > size.saturating_sub(start) {
                return Err(anyhow::anyhow!(
                    "Subset length {} at axis {} with start {} exceeds axis size {}",
                    length,
                    axis,
                    start,
                    size
                ));
            }
        }

        Ok(())
    }

    fn subset_linear_indices(&self, subset: &ArraySubset) -> Vec<usize> {
        let rank = self.shape.len();
        let subset_len: usize = subset.shape.iter().product();

        if rank == 0 {
            return vec![0usize];
        }
        if subset_len == 0 {
            return Vec::new();
        }

        let mut linear_indices = Vec::with_capacity(subset_len);
        let mut offset = vec![0usize; rank];

        loop {
            let linear_index = (0..rank)
                .map(|axis| (subset.start[axis] + offset[axis]) as isize * self.strides[axis])
                .sum::<isize>() as usize;
            linear_indices.push(linear_index);

            let mut carry_axis = rank;
            while carry_axis > 0 {
                let axis = carry_axis - 1;
                offset[axis] += 1;
                if offset[axis] < subset.shape[axis] {
                    break;
                }
                offset[axis] = 0;
                carry_axis -= 1;
            }

            if carry_axis == 0 {
                break;
            }
        }

        linear_indices
    }

    fn coalesce_linear_indices_to_ranges(linear_indices: &[usize]) -> Vec<(usize, usize)> {
        if linear_indices.is_empty() {
            return vec![(0, 0)];
        }

        let mut ranges = Vec::new();
        let mut current_start = linear_indices[0];
        let mut current_len = 1usize;

        for index in linear_indices.iter().skip(1) {
            if *index == current_start + current_len {
                current_len += 1;
            } else {
                ranges.push((current_start, current_len));
                current_start = *index;
                current_len = 1;
            }
        }
        ranges.push((current_start, current_len));

        ranges
    }

    fn chunk_source_indices(
        shape: &[usize],
        strides: &[isize],
        start: usize,
        len: usize,
    ) -> Vec<usize> {
        let logical_strides = Self::logical_row_major_strides(shape);
        (start..start + len)
            .map(|position| {
                Self::logical_position_to_source_index(position, shape, &logical_strides, strides)
            })
            .collect()
    }

    fn logical_row_major_strides(shape: &[usize]) -> Vec<usize> {
        if shape.is_empty() {
            return vec![];
        }
        let mut strides = vec![0usize; shape.len()];
        let mut stride = 1usize;
        for (axis, dim) in shape.iter().enumerate().rev() {
            strides[axis] = stride;
            stride = stride.saturating_mul(*dim);
        }
        strides
    }

    fn logical_position_to_source_index(
        mut position: usize,
        shape: &[usize],
        logical_strides: &[usize],
        source_strides: &[isize],
    ) -> usize {
        if shape.is_empty() {
            return 0;
        }

        let mut source_index = 0isize;
        for axis in 0..shape.len() {
            let logical_stride = logical_strides[axis];
            let coord = if logical_stride == 0 {
                0
            } else {
                position / logical_stride
            };
            if logical_stride != 0 {
                position %= logical_stride;
            }
            source_index += coord as isize * source_strides[axis];
        }

        source_index as usize
    }

    async fn materialize_backend_ranges(
        &self,
        ranges: &[(usize, usize)],
    ) -> anyhow::Result<ArrayRef> {
        Self::materialize_backend_ranges_from_backend(&self.backend, ranges).await
    }

    async fn materialize_backend_ranges_from_backend(
        backend: &Arc<B>,
        ranges: &[(usize, usize)],
    ) -> anyhow::Result<ArrayRef> {
        if ranges.len() == 1 && ranges[0].1 == 0 {
            return backend.slice(0, 0).await;
        }

        let chunk_futures = ranges
            .iter()
            .map(|(start, len)| backend.slice(*start, *len));
        let mut chunks = try_join_all(chunk_futures).await?;

        if chunks.len() == 1 {
            Ok(chunks.remove(0))
        } else {
            let chunk_refs: Vec<&dyn Array> = chunks.iter().map(|c| c.as_ref()).collect();
            Ok(concat(&chunk_refs)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;
    use futures::StreamExt;

    use crate::array::{
        NdArrowArray, backend::ArrayBackend, backend::mem::InMemoryArrayBackend,
        subset::ArraySubset,
    };

    #[derive(Debug)]
    struct RecordingBackend {
        values: ArrayRef,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        calls: Arc<Mutex<Vec<(usize, usize)>>>,
    }

    impl RecordingBackend {
        fn new(
            values: ArrayRef,
            shape: Vec<usize>,
            dimensions: Vec<String>,
            calls: Arc<Mutex<Vec<(usize, usize)>>>,
        ) -> Self {
            Self {
                values,
                shape,
                dimensions,
                calls,
            }
        }
    }

    #[async_trait::async_trait]
    impl ArrayBackend for RecordingBackend {
        fn len(&self) -> usize {
            self.values.len()
        }

        fn shape(&self) -> Vec<usize> {
            self.shape.clone()
        }

        fn dimensions(&self) -> Vec<String> {
            self.dimensions.clone()
        }

        async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
            self.calls.lock().unwrap().push((start, length));
            Ok(self.values.slice(start, length))
        }
    }

    #[tokio::test]
    async fn slice_3d_subset_returns_expected_values_and_shape() {
        let values = Arc::new(Int32Array::from((0..24).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(
                values,
                vec![2, 3, 4],
                vec!["t".to_string(), "y".to_string(), "x".to_string()],
            ),
            DataType::Int32,
        )
        .unwrap();

        let sliced = array
            .slice(ArraySubset {
                start: vec![1, 1, 1],
                shape: vec![1, 2, 2],
            })
            .await
            .unwrap();

        assert_eq!(sliced.shape(), &[1, 2, 2]);
        assert_eq!(sliced.dimensions(), &["t", "y", "x"]);

        let out = sliced.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.values(), &[17, 18, 21, 22]);
    }

    #[tokio::test]
    async fn slice_returns_error_for_out_of_bounds_subset() {
        let values = Arc::new(Int32Array::from((0..24).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(
                values,
                vec![2, 3, 4],
                vec!["t".to_string(), "y".to_string(), "x".to_string()],
            ),
            DataType::Int32,
        )
        .unwrap();

        let result = array
            .slice(ArraySubset {
                start: vec![0, 2, 3],
                shape: vec![2, 2, 2],
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn slice_returns_error_for_rank_mismatch() {
        let values = Arc::new(Int32Array::from((0..24).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(
                values,
                vec![2, 3, 4],
                vec!["t".to_string(), "y".to_string(), "x".to_string()],
            ),
            DataType::Int32,
        )
        .unwrap();

        let result = array
            .slice(ArraySubset {
                start: vec![0, 1],
                shape: vec![1, 2],
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn slice_uses_multiple_backend_calls_for_non_aligned_subset() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let values: ArrayRef = Arc::new(Int32Array::from((0..24).collect::<Vec<_>>()));
        let backend = RecordingBackend::new(
            values,
            vec![2, 3, 4],
            vec!["t".to_string(), "y".to_string(), "x".to_string()],
            calls.clone(),
        );

        let array = NdArrowArray::new(backend, DataType::Int32).unwrap();

        let sliced = array
            .slice(ArraySubset {
                start: vec![0, 1, 1],
                shape: vec![2, 2, 2],
            })
            .await
            .unwrap();

        let out = sliced.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.values(), &[5, 6, 9, 10, 17, 18, 21, 22]);

        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded, vec![(5, 2), (9, 2), (17, 2), (21, 2)]);
    }

    #[test]
    fn backend_slice_ranges_returns_expected_ranges() {
        let values = Arc::new(Int32Array::from((0..24).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(
                values,
                vec![2, 3, 4],
                vec!["t".to_string(), "y".to_string(), "x".to_string()],
            ),
            DataType::Int32,
        )
        .unwrap();

        let ranges = array
            .backend_slice_ranges(&ArraySubset {
                start: vec![0, 1, 1],
                shape: vec![2, 2, 2],
            })
            .unwrap();

        assert_eq!(ranges, vec![(5, 2), (9, 2), (17, 2), (21, 2)]);
    }

    #[tokio::test]
    async fn broadcast_named_dims_adds_leading_dim_and_repeats_values() {
        let values = Arc::new(Int32Array::from((0..6).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![2, 3], vec!["x".to_string(), "y".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let target_shape = vec![4, 2, 3];
        let target_dims = vec!["t".to_string(), "x".to_string(), "y".to_string()];
        let broadcasted = array.broadcast(&target_shape, &target_dims).unwrap();

        assert_eq!(broadcasted.shape(), &[4, 2, 3]);
        assert_eq!(broadcasted.dimensions(), &["t", "x", "y"]);

        let out = broadcasted.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut expected = Vec::new();
        for _ in 0..4 {
            expected.extend(0..6);
        }
        assert_eq!(out.values(), expected.as_slice());
    }

    #[tokio::test]
    async fn broadcast_errors_when_reordering_existing_dimensions() {
        let values = Arc::new(Int32Array::from((0..6).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![2, 3], vec!["x".to_string(), "y".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let target_shape = vec![3, 2];
        let target_dims = vec!["y".to_string(), "x".to_string()];
        let result = array.broadcast(&target_shape, &target_dims);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn broadcast_errors_on_incompatible_dimension_sizes() {
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![2], vec!["x".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let target_shape = vec![3];
        let target_dims = vec!["x".to_string()];
        let result = array.broadcast(&target_shape, &target_dims);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn slice_after_broadcast_uses_virtual_strides() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![3], vec!["x".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let broadcasted = array
            .broadcast(&[2, 3], &["t".to_string(), "x".to_string()])
            .unwrap();

        let sliced = broadcasted
            .slice(ArraySubset {
                start: vec![0, 1],
                shape: vec![2, 2],
            })
            .await
            .unwrap();

        assert_eq!(sliced.shape(), &[2, 2]);
        assert_eq!(sliced.dimensions(), &["t", "x"]);

        let out = sliced.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.values(), &[2, 3, 2, 3]);
    }

    #[tokio::test]
    async fn broadcast_lon_1d_to_lat_lon_time_3d() {
        let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![3], vec!["lon".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let broadcasted = array
            .broadcast(
                &[2, 3, 4],
                &["lat".to_string(), "lon".to_string(), "time".to_string()],
            )
            .unwrap();

        assert_eq!(broadcasted.shape(), &[2, 3, 4]);
        assert_eq!(broadcasted.dimensions(), &["lat", "lon", "time"]);
        assert_eq!(broadcasted.strides, vec![0, 1, 0]);

        let out = broadcasted.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut expected = Vec::with_capacity(2 * 3 * 4);
        for _lat in 0..2 {
            for lon in [10, 20, 30] {
                for _time in 0..4 {
                    expected.push(lon);
                }
            }
        }

        assert_eq!(out.values(), expected.as_slice());
    }

    #[tokio::test]
    async fn slice_after_broadcast_lon_1d_to_lat_lon_time_3d() {
        let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![3], vec!["lon".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let broadcasted = array
            .broadcast(
                &[2, 3, 4],
                &["lat".to_string(), "lon".to_string(), "time".to_string()],
            )
            .unwrap();

        let sliced = broadcasted
            .slice(ArraySubset {
                start: vec![1, 1, 1],
                shape: vec![1, 2, 2],
            })
            .await
            .unwrap();

        assert_eq!(sliced.shape(), &[1, 2, 2]);
        assert_eq!(sliced.dimensions(), &["lat", "lon", "time"]);

        let out = sliced.as_arrow_array_ref().await.unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.values(), &[20, 20, 30, 30]);
    }

    #[test]
    fn broadcast_is_virtual_and_does_not_touch_backend() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let values: ArrayRef = Arc::new(Int32Array::from((0..6).collect::<Vec<_>>()));
        let backend = RecordingBackend::new(
            values,
            vec![2, 3],
            vec!["x".to_string(), "y".to_string()],
            calls.clone(),
        );
        let array = NdArrowArray::new(backend, DataType::Int32).unwrap();

        let broadcasted = array
            .broadcast(
                &[4, 2, 3],
                &["t".to_string(), "x".to_string(), "y".to_string()],
            )
            .unwrap();

        assert_eq!(broadcasted.shape(), &[4, 2, 3]);
        assert_eq!(broadcasted.dimensions(), &["t", "x", "y"]);
        assert_eq!(broadcasted.strides, vec![0, 3, 1]);
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_chunked_contiguous_reads_expected_chunks() {
        let values = Arc::new(Int32Array::from((0..10).collect::<Vec<_>>()));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![10], vec!["x".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let mut stream = array.read_chunked(4).unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
            chunks.push(chunk.values().to_vec());
        }

        assert_eq!(chunks, vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9]]);
    }

    #[tokio::test]
    async fn read_chunked_strided_is_lazy_and_applies_strides() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let backend =
            RecordingBackend::new(values, vec![3], vec!["lon".to_string()], calls.clone());

        let array = NdArrowArray::new(backend, DataType::Int32).unwrap();

        let broadcasted = array
            .broadcast(&[2, 3], &["lat".to_string(), "lon".to_string()])
            .unwrap();

        let mut stream = broadcasted.read_chunked(2).unwrap();
        assert!(calls.lock().unwrap().is_empty());

        let first = stream.next().await.unwrap().unwrap();
        let first = first.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first.values(), &[10, 20]);
        assert!(!calls.lock().unwrap().is_empty());

        let second = stream.next().await.unwrap().unwrap();
        let second = second.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second.values(), &[30, 10]);

        let third = stream.next().await.unwrap().unwrap();
        let third = third.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(third.values(), &[20, 30]);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn read_chunked_after_broadcast_lon_to_lat_lon_time_preserves_order() {
        let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![3], vec!["lon".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let broadcasted = array
            .broadcast(
                &[2, 3, 4],
                &["lat".to_string(), "lon".to_string(), "time".to_string()],
            )
            .unwrap();

        let mut stream = broadcasted.read_chunked(5).unwrap();
        let mut reconstructed = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
            reconstructed.extend_from_slice(chunk.values());
        }

        let mut expected = Vec::with_capacity(2 * 3 * 4);
        for _lat in 0..2 {
            for lon in [10, 20, 30] {
                for _time in 0..4 {
                    expected.push(lon);
                }
            }
        }

        assert_eq!(reconstructed, expected);
    }

    #[tokio::test]
    async fn read_chunked_after_broadcast_emits_expected_chunk_boundaries() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array = NdArrowArray::new(
            InMemoryArrayBackend::new(values, vec![3], vec!["lon".to_string()]),
            DataType::Int32,
        )
        .unwrap();

        let broadcasted = array
            .broadcast(
                &[2, 3, 2],
                &["lat".to_string(), "lon".to_string(), "time".to_string()],
            )
            .unwrap();

        let mut stream = broadcasted.read_chunked(4).unwrap();

        let c1 = stream.next().await.unwrap().unwrap();
        let c1 = c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c1.values(), &[1, 1, 2, 2]);

        let c2 = stream.next().await.unwrap().unwrap();
        let c2 = c2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c2.values(), &[3, 3, 1, 1]);

        let c3 = stream.next().await.unwrap().unwrap();
        let c3 = c3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c3.values(), &[2, 2, 3, 3]);

        assert!(stream.next().await.is_none());
    }
}
