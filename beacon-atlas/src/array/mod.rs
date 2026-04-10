use std::sync::Arc;

use beacon_nd_arrow::array::{
    backend::{ArrayBackend, BackendSubsetResult},
    subset::ArraySubset,
};
use object_store::ObjectStore;

use crate::{
    array::{compat::AtlasArrowCompat, layout::ArrayLayout},
    arrow_object_store::ArrowObjectStoreReader,
};

pub mod compat;
pub mod file;
pub mod io_cache;
pub mod layout;
pub mod reader;
pub mod statistics;
pub mod writer;

/// Translate an n-dimensional subset into flat half-open ranges `(start, end)`.
///
/// Atlas stores array values as one logical 1D stream in row-major order.
/// This helper turns a multi-dimensional slice request into the smallest set of
/// contiguous flat ranges that cover the requested values.
///
/// The main optimization is that fully selected trailing axes are merged into a
/// single contiguous block. For example, selecting full rows from a 2D array
/// becomes one range per row instead of one range per element.
fn subset_to_flat_ranges(subset: &ArraySubset, array_shape: &[usize]) -> Vec<(usize, usize)> {
    assert_eq!(subset.start.len(), array_shape.len());
    assert_eq!(subset.shape.len(), array_shape.len());

    if array_shape.is_empty() {
        return vec![(0, 1)];
    }

    if subset.shape.contains(&0) {
        return Vec::new();
    }

    for ((&start, &len), &axis_len) in subset
        .start
        .iter()
        .zip(subset.shape.iter())
        .zip(array_shape.iter())
    {
        assert!(start <= axis_len);
        assert!(len <= axis_len - start);
    }

    if array_shape.len() == 1 {
        let start = subset.start[0];
        return vec![(start, start + subset.shape[0])];
    }

    let mut strides = vec![1; array_shape.len()];
    for axis in (0..array_shape.len().saturating_sub(1)).rev() {
        strides[axis] = strides[axis + 1] * array_shape[axis + 1];
    }

    let mut block_axis = array_shape.len() - 1;
    while block_axis > 0
        && subset.start[block_axis] == 0
        && subset.shape[block_axis] == array_shape[block_axis]
    {
        block_axis -= 1;
    }

    // `base_offset` is the flat position of the first requested element.
    // `block_len` is the largest contiguous run we can read at once after
    // collapsing fully selected trailing axes.
    let base_offset: usize = (0..array_shape.len())
        .map(|axis| subset.start[axis] * strides[axis])
        .sum();
    let block_len: usize = subset.shape[block_axis..].iter().product();

    if block_axis == 0 {
        return vec![(base_offset, base_offset + block_len)];
    }

    let range_count: usize = subset.shape[..block_axis].iter().product();
    let mut ranges = Vec::with_capacity(range_count);
    let mut prefix = subset.start[..block_axis].to_vec();
    let mut current_start = base_offset;

    // Walk the non-collapsed prefix axes like an odometer. Each step advances
    // to the next contiguous block in row-major order.
    for range_index in 0..range_count {
        ranges.push((current_start, current_start + block_len));

        if range_index + 1 == range_count {
            break;
        }

        for axis in (0..block_axis).rev() {
            prefix[axis] += 1;
            current_start += strides[axis];

            let axis_end = subset.start[axis] + subset.shape[axis];
            if prefix[axis] < axis_end {
                break;
            }

            prefix[axis] = subset.start[axis];
            current_start -= subset.shape[axis] * strides[axis];
        }
    }

    ranges
}

/// Array backend backed by Arrow IPC batches stored in an object store.
///
/// Each logical dataset array is described by an [`ArrayLayout`], which tells
/// Atlas where the array begins inside the flat stored value stream and what
/// its n-dimensional shape is. Subset reads are implemented by:
///
/// 1. translating the n-dimensional subset into flat contiguous ranges,
/// 2. mapping those ranges onto stored Arrow record batches,
/// 3. concatenating the resulting Arrow slices,
/// 4. converting them into `Vec<A>` using [`AtlasArrowCompat`], and
/// 5. reshaping the values into an `ndarray::ArrayD<A>`.
#[derive(Debug, Clone)]
pub struct AtlasArrayBackend<S: ObjectStore + Clone, A: AtlasArrowCompat> {
    marker: std::marker::PhantomData<A>,
    arrow_array_reader: Arc<ArrowObjectStoreReader<S>>,
    io_cache: Arc<io_cache::IoCache>,
    layout: Arc<ArrayLayout>,
}

#[async_trait::async_trait]
impl<S: ObjectStore + Clone, A: AtlasArrowCompat> ArrayBackend<A> for AtlasArrayBackend<S, A> {
    fn len(&self) -> usize {
        self.layout
            .array_shape
            .iter()
            .map(|&dim| dim as usize)
            .product()
    }

    /// Returns `true` when the array contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn shape(&self) -> Vec<usize> {
        self.layout
            .array_shape
            .iter()
            .map(|&dim| dim as usize)
            .collect()
    }
    fn dimensions(&self) -> Vec<String> {
        self.layout.dimensions.clone()
    }
    fn fill_value(&self) -> Option<A> {
        None
    }

    /// Read an n-dimensional subset from the stored flat Arrow value stream.
    ///
    /// The implementation first converts the logical subset into flat ranges,
    /// then slices the necessary IPC record batches, concatenates those slices,
    /// converts the Arrow array into `Vec<A>` plus an optional validity mask,
    /// and finally reshapes both into the requested subset shape.
    async fn read_subset_with_validity(
        &self,
        subset: ArraySubset,
    ) -> anyhow::Result<BackendSubsetResult<A>> {
        self.validate_subset(&subset)?;

        let subset_shape = subset.shape.clone();
        let ranges = Self::translate_subset_to_ranges(&subset, &self.shape());

        if ranges.is_empty() {
            let values = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&subset_shape), Vec::new())
                .map_err(|err| anyhow::anyhow!("failed to build empty subset array: {err}"))?;
            return Ok(BackendSubsetResult::new(values, None));
        }

        let batch_size = self.batch_size().await?;
        let dataset_start = usize::try_from(self.layout.array_start)
            .map_err(|err| anyhow::anyhow!("array_start does not fit in usize: {err}"))?;
        let mut chunks = Vec::new();

        for (range_start, range_end) in ranges {
            let mut absolute_start = dataset_start
                .checked_add(range_start)
                .ok_or_else(|| anyhow::anyhow!("subset start overflow"))?;
            let absolute_end = dataset_start
                .checked_add(range_end)
                .ok_or_else(|| anyhow::anyhow!("subset end overflow"))?;

            while absolute_start < absolute_end {
                let batch_index = absolute_start / batch_size;
                let offset_in_batch = absolute_start % batch_size;
                let len = (absolute_end - absolute_start).min(batch_size - offset_in_batch);

                // A flat subset range may cross batch boundaries, so split it
                // into per-batch slices and fetch each slice independently.
                let batch = self.read_batch(batch_index, offset_in_batch, len).await?;

                anyhow::ensure!(
                    batch.num_columns() == 1,
                    "expected a single array column, got {}",
                    batch.num_columns()
                );

                chunks.push(batch.column(0).clone());
                absolute_start += len;
            }
        }

        let concatenated = if chunks.len() == 1 {
            chunks.pop().expect("one chunk exists")
        } else {
            let chunk_refs: Vec<&dyn arrow::array::Array> =
                chunks.iter().map(|chunk| chunk.as_ref()).collect();
            arrow::compute::concat(&chunk_refs)
                .map_err(|err| anyhow::anyhow!("failed to concatenate subset chunks: {err}"))?
        };

        let converted = A::from_arrow_array_with_validity(concatenated.as_ref())?;
        let values =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&subset_shape), converted.values)
                .map_err(|err| anyhow::anyhow!("failed to build subset ndarray: {err}"))?;
        let validity = converted
            .validity
            .map(|mask| ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&subset_shape), mask))
            .transpose()
            .map_err(|err| anyhow::anyhow!("failed to build subset validity ndarray: {err}"))?;

        Ok(BackendSubsetResult::new(values, validity))
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<A>> {
        Ok(self.read_subset_with_validity(subset).await?.values)
    }
}

impl<S: ObjectStore + Clone, A: AtlasArrowCompat> AtlasArrayBackend<S, A> {
    /// Create a new Atlas array backend for one dataset array.
    pub fn new(
        arrow_array_reader: Arc<ArrowObjectStoreReader<S>>,
        io_cache: Arc<io_cache::IoCache>,
        layout: Arc<ArrayLayout>,
    ) -> Self {
        Self {
            marker: std::marker::PhantomData,
            arrow_array_reader,
            io_cache,
            layout,
        }
    }

    /// Translate an n-dimensional subset into flat half-open ranges `(start, end)`.
    ///
    /// The backing store is row-major and physically contiguous, so we coalesce
    /// as far left as possible when trailing axes are fully selected.
    fn translate_subset_to_ranges(
        subset: &ArraySubset,
        array_shape: &[usize],
    ) -> Vec<(usize, usize)> {
        subset_to_flat_ranges(subset, array_shape)
    }

    /// Determine the stored IPC batch size from the first batch.
    ///
    /// Atlas currently assumes a stable batch size for all non-terminal batches,
    /// which matches how the writer emits the flat value stream.
    async fn batch_size(&self) -> anyhow::Result<usize> {
        let first_batch = self.read_batch(0, 0, usize::MAX).await?;
        anyhow::ensure!(first_batch.num_rows() > 0, "first record batch is empty");
        Ok(first_batch.num_rows())
    }

    /// Read and slice a single stored Arrow record batch.
    ///
    /// Batches are cached by `(path, batch_index)` so repeated subset reads can
    /// reuse previously decoded Arrow data.
    async fn read_batch(
        &self,
        index: usize,
        start: usize,
        len: usize,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        let path = self.arrow_array_reader.path().clone();
        let batch = self
            .io_cache
            .get_or_insert_with((path, index), |_| async move {
                self.arrow_array_reader
                    .read_batch(index)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("batch index out of range"))
            })
            .await?;
        let slice_len = len.min(batch.num_rows().saturating_sub(start));
        Ok(batch.slice(start, slice_len))
    }
}

#[cfg(test)]
mod tests {
    use super::AtlasArrayBackend;
    use crate::{
        array::{io_cache::IoCache, layout::ArrayLayout},
        arrow_object_store::ArrowObjectStoreReader,
    };
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        ipc::writer::FileWriter,
        record_batch::RecordBatch,
    };
    use beacon_nd_arrow::array::{backend::ArrayBackend, subset::ArraySubset};
    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::{io::Cursor, sync::Arc};

    type TestBackend = AtlasArrayBackend<Arc<dyn ObjectStore>, i32>;

    async fn make_test_backend(
        values: Vec<i32>,
        batch_sizes: &[usize],
        layout: ArrayLayout,
    ) -> anyhow::Result<TestBackend> {
        anyhow::ensure!(values.len() == batch_sizes.iter().sum::<usize>());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "array",
            DataType::Int32,
            false,
        )]));
        let mut cursor = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new(&mut cursor, &schema)?;

        let mut offset = 0;
        for &batch_size in batch_sizes {
            let batch_values = values[offset..offset + batch_size].to_vec();
            offset += batch_size;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(batch_values))],
            )?;
            writer.write(&batch)?;
        }
        writer.finish()?;

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = Path::from("array.arrow");
        store
            .put(&path, Bytes::from(cursor.into_inner()).into())
            .await?;

        let reader = Arc::new(ArrowObjectStoreReader::new(store.clone(), path).await?);
        Ok(TestBackend::new(
            reader,
            Arc::new(IoCache::new(1024 * 1024)),
            Arc::new(layout),
        ))
    }

    #[test]
    fn translate_subset_to_ranges_returns_full_array_as_one_range() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![0, 0, 0],
                shape: vec![3, 4, 5],
            },
            &[3, 4, 5],
        );

        assert_eq!(ranges, vec![(0, 60)]);
    }

    #[test]
    fn translate_subset_to_ranges_splits_non_contiguous_rows() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 3],
            },
            &[4, 5],
        );

        assert_eq!(ranges, vec![(6, 9), (11, 14)]);
    }

    #[test]
    fn translate_subset_to_ranges_merges_when_trailing_axes_are_full() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![1, 2, 0],
                shape: vec![2, 2, 6],
            },
            &[4, 5, 6],
        );

        assert_eq!(ranges, vec![(42, 54), (72, 84)]);
    }

    #[test]
    fn translate_subset_to_ranges_returns_one_range_for_scalar() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![],
                shape: vec![],
            },
            &[],
        );

        assert_eq!(ranges, vec![(0, 1)]);
    }

    #[test]
    fn translate_subset_to_ranges_returns_one_range_for_1d_subset() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![3],
                shape: vec![4],
            },
            &[10],
        );

        assert_eq!(ranges, vec![(3, 7)]);
    }

    #[test]
    fn translate_subset_to_ranges_returns_single_element_ranges_when_needed() {
        let ranges = TestBackend::translate_subset_to_ranges(
            &ArraySubset {
                start: vec![1, 1, 1],
                shape: vec![2, 2, 1],
            },
            &[3, 4, 5],
        );

        assert_eq!(ranges, vec![(26, 27), (31, 32), (46, 47), (51, 52)]);
    }

    #[tokio::test]
    async fn read_subset_reads_across_batch_boundaries() -> anyhow::Result<()> {
        let backend = make_test_backend(
            (0..8).collect(),
            &[5, 3],
            ArrayLayout {
                dataset_index: 0,
                array_start: 0,
                array_len: 8,
                array_shape: vec![8],
                chunk_shape: vec![8],
                dimensions: vec!["x".to_string()],
            },
        )
        .await?;

        let subset = backend
            .read_subset(ArraySubset {
                start: vec![3],
                shape: vec![4],
            })
            .await?;

        assert_eq!(subset.shape(), &[4]);
        assert_eq!(subset.iter().copied().collect::<Vec<_>>(), vec![3, 4, 5, 6]);
        Ok(())
    }

    #[tokio::test]
    async fn read_subset_reads_non_contiguous_ranges_with_array_offset() -> anyhow::Result<()> {
        let backend = make_test_backend(
            (0..12).collect(),
            &[5, 5, 2],
            ArrayLayout {
                dataset_index: 0,
                array_start: 2,
                array_len: 8,
                array_shape: vec![2, 4],
                chunk_shape: vec![2, 4],
                dimensions: vec!["x".to_string(), "y".to_string()],
            },
        )
        .await?;

        let subset = backend
            .read_subset(ArraySubset {
                start: vec![0, 1],
                shape: vec![2, 2],
            })
            .await?;

        assert_eq!(subset.shape(), &[2, 2]);
        assert_eq!(subset.iter().copied().collect::<Vec<_>>(), vec![3, 4, 7, 8]);
        Ok(())
    }
}
