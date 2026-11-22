//! The chunk grid of one opened file, and the read of one chunk.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use beacon_nd_array::{
    array::subset::ArraySubset, arrow::array::ndarray_to_arrow_array, dataset::Dataset, NdArrayD,
};
use datafusion::error::{DataFusionError, Result};
use indexmap::IndexMap;

/// One opened file, cut into chunks and ready to read.
#[derive(Debug)]
pub(super) struct FileScan {
    arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    /// The dimension names the chunks index, outermost first.
    dims: Vec<String>,
    schema: Arc<Schema>,
    /// The chunks, in C order.
    chunks: Vec<ArraySubset>,
}

impl FileScan {
    /// Cut `dataset` into chunks of about `batch_size` elements. A dataset whose
    /// grid holds at most `batch_size` rows is one chunk.
    ///
    /// The list depends only on the dataset and on `batch_size`. The slices of
    /// one file rely on that: each one builds the same list.
    pub(super) fn try_new(dataset: Dataset, batch_size: usize) -> Result<Self> {
        let (dims, shape, chunk_shape) = layout(&dataset)?;
        let rows = shape
            .iter()
            .try_fold(1usize, |rows, &len| rows.checked_mul(len));
        // A small file is one chunk. Else a native chunk layout wins, and a
        // contiguous array is cut at the batch size.
        let chunk_shape = if rows.is_some_and(|rows| rows <= batch_size) {
            shape.clone()
        } else if chunk_shape != shape {
            chunk_shape
        } else {
            c_order_chunk_shape(&shape, batch_size)
        };
        let schema = Arc::new(Schema::new(
            dataset
                .arrays
                .iter()
                .map(|(name, array)| {
                    let data_type: DataType = array.datatype().into();
                    Field::new(name.clone(), data_type, true)
                })
                .collect::<Vec<_>>(),
        ));

        Ok(Self {
            chunks: chunk_subsets(&shape, &chunk_shape),
            arrays: dataset.arrays,
            dims,
            schema,
        })
    }

    pub(super) fn chunks(&self) -> &[ArraySubset] {
        &self.chunks
    }

    pub(super) fn dims(&self) -> &[String] {
        &self.dims
    }

    pub(super) fn arrays(&self) -> &IndexMap<String, Arc<dyn NdArrayD>> {
        &self.arrays
    }

    /// Read one chunk into an nd batch that is not broadcast yet.
    ///
    /// Each array is read on its own axes, so a coordinate is read once per
    /// chunk and not once per row of it.
    pub(super) async fn read(&self, chunk: &ArraySubset) -> Result<NdRecordBatch> {
        let target = Dimensions::try_new(
            self.dims
                .iter()
                .zip(chunk.shape.iter())
                .map(|(name, &size)| Dimension::new(name.as_str(), size))
                .collect(),
        )?;

        let mut columns = Vec::with_capacity(self.arrays.len());
        for (name, array) in &self.arrays {
            let own = array_subset(chunk, &self.dims, array.as_ref());
            let sliced = array.subset(own).await.map_err(execution)?;
            let values = ndarray_to_arrow_array(sliced.as_ref())
                .await
                .map_err(execution)?;
            let dims = Dimensions::try_new(
                sliced
                    .dimensions()
                    .iter()
                    .zip(sliced.shape().iter())
                    .map(|(dim, &size)| Dimension::new(dim.as_str(), size))
                    .collect(),
            )?;
            columns.push(
                NdArrowArray::try_new(values, dims)
                    .map_err(|e| execution(format!("nd column '{name}': {e}")))?,
            );
        }

        NdRecordBatch::try_new(self.schema.clone(), columns, target)
    }
}

fn execution(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// The dimension names, the shape and the chunk shape that drive the grid.
fn layout(dataset: &Dataset) -> Result<(Vec<String>, Vec<usize>, Vec<usize>)> {
    for (name, array) in &dataset.arrays {
        if !array
            .dimensions()
            .iter()
            .all(|dim| dataset.dimensions.contains_key(dim))
        {
            return Err(execution(format!(
                "array '{name}' has dimensions {:?} that the dataset does not have",
                array.dimensions()
            )));
        }
    }
    if dataset.arrays.is_empty() {
        return Err(execution("the dataset has no arrays, so it has no grid"));
    }

    // The grid spans only the dimensions the selected arrays use. An axis that
    // no array has would repeat every value once per chunk along it.
    let used: Vec<(&String, usize)> = dataset
        .dimensions
        .iter()
        .filter(|(dim, _)| {
            dataset
                .arrays
                .values()
                .any(|array| array.dimensions().contains(dim))
        })
        .map(|(dim, &len)| (dim, len))
        .collect();

    // An array on every used dimension drives the grid, and the others broadcast onto it.
    if let Some((_, full)) = dataset
        .arrays
        .iter()
        .filter(|(_, array)| array.dimensions().len() == used.len())
        .max_by_key(|(_, array)| array.shape().iter().product::<usize>())
    {
        return Ok((full.dimensions(), full.shape(), full.chunk_shape()));
    }

    // No array spans every used dimension, so those dimensions are the grid.
    let dims: Vec<String> = used.iter().map(|(dim, _)| (*dim).clone()).collect();
    let shape: Vec<usize> = used.iter().map(|(_, len)| *len).collect();
    Ok((dims, shape.clone(), shape))
}

/// A chunk shape of about `batch_size` elements, filled from the last axis.
fn c_order_chunk_shape(shape: &[usize], batch_size: usize) -> Vec<usize> {
    let mut chunk = vec![1usize; shape.len()];
    // The element count of the axes that are whole.
    let mut inner = 1usize;
    for axis in (0..shape.len()).rev() {
        let len = shape[axis].max(1);
        match inner.checked_mul(len) {
            Some(size) if size <= batch_size => {
                chunk[axis] = len;
                inner = size;
            }
            _ => {
                // This axis does not fit. Cut it, and leave the outer axes at 1.
                chunk[axis] = (batch_size / inner).max(1).min(len);
                break;
            }
        }
    }
    chunk
}

/// Every chunk of `shape` at `chunk_shape`, in C order. An edge chunk is smaller.
fn chunk_subsets(shape: &[usize], chunk_shape: &[usize]) -> Vec<ArraySubset> {
    if shape.is_empty() {
        return vec![ArraySubset::new(vec![], vec![])];
    }
    let counts: Vec<usize> = shape
        .iter()
        .zip(chunk_shape)
        .map(|(&len, &chunk)| {
            if len == 0 {
                0
            } else {
                len.div_ceil(chunk.max(1))
            }
        })
        .collect();
    if counts.contains(&0) {
        return vec![];
    }

    let total: usize = counts.iter().product();
    let mut subsets = Vec::with_capacity(total);
    for linear in 0..total {
        let mut rest = linear;
        let mut index = vec![0usize; shape.len()];
        for axis in (0..shape.len()).rev() {
            index[axis] = rest % counts[axis];
            rest /= counts[axis];
        }
        let start: Vec<usize> = index
            .iter()
            .zip(chunk_shape)
            .map(|(&i, &chunk)| i * chunk.max(1))
            .collect();
        let size: Vec<usize> = shape
            .iter()
            .zip(&start)
            .zip(chunk_shape)
            .map(|((&len, &start), &chunk)| chunk.max(1).min(len - start))
            .collect();
        subsets.push(ArraySubset::new(start, size));
    }
    subsets
}

/// The part of `chunk` that `array` holds, on the axes of `array`.
fn array_subset(chunk: &ArraySubset, dims: &[String], array: &dyn NdArrayD) -> ArraySubset {
    let mut start = Vec::new();
    let mut shape = Vec::new();
    for dim in &array.dimensions() {
        if let Some(axis) = dims.iter().position(|d| d == dim) {
            start.push(chunk.start[axis]);
            shape.push(chunk.shape[axis]);
        }
    }
    ArraySubset::new(start, shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::test_util::{open_test_file as open, GRIDDED_FILE};

    /// Keep only the arrays in `names`.
    fn only(mut dataset: Dataset, names: &[&str]) -> Dataset {
        dataset
            .arrays
            .retain(|name, _| names.contains(&name.as_str()));
        dataset
    }

    #[tokio::test]
    async fn a_native_chunk_layout_sets_the_grid() {
        let dataset = only(open(GRIDDED_FILE).await, &["analysed_sst", "lat"]);

        let scan = FileScan::try_new(dataset, 8192).unwrap();

        // analysed_sst is [1, 1208, 1920] in native chunks of [1, 604, 960].
        assert_eq!(scan.dims(), ["time", "lat", "lon"]);
        assert_eq!(scan.chunks().len(), 4);
        assert_eq!(scan.chunks()[0].start, vec![0, 0, 0]);
        assert_eq!(scan.chunks()[0].shape, vec![1, 604, 960]);
        assert_eq!(scan.chunks()[3].start, vec![0, 604, 960]);
        let rows: usize = scan.chunks().iter().map(ArraySubset::rows).sum();
        assert_eq!(rows, 1208 * 1920);
    }

    #[tokio::test]
    async fn a_small_grid_is_one_chunk_whatever_its_native_chunks() {
        let dataset = only(open(GRIDDED_FILE).await, &["analysed_sst", "lat"]);

        // The grid holds 1208 * 1920 rows, and the batch size covers all of them.
        let scan = FileScan::try_new(dataset, 1208 * 1920).unwrap();

        assert_eq!(scan.chunks().len(), 1);
        assert_eq!(scan.chunks()[0].start, vec![0, 0, 0]);
        assert_eq!(scan.chunks()[0].shape, vec![1, 1208, 1920]);
    }

    #[tokio::test]
    async fn a_contiguous_array_is_cut_at_the_batch_size() {
        let dataset = only(open(GRIDDED_FILE).await, &["sea_ice_fraction"]);

        let scan = FileScan::try_new(dataset, 4096).unwrap();

        // Two rows of `lon` (1920 each) fit in 4096 elements.
        assert_eq!(scan.chunks()[0].shape, vec![1, 2, 1920]);
        assert_eq!(scan.chunks().len(), 604);
    }

    #[tokio::test]
    async fn a_chunk_reads_each_array_on_its_own_axes() {
        let dataset = only(open(GRIDDED_FILE).await, &["analysed_sst", "lat"]);
        let scan = FileScan::try_new(dataset, 8192).unwrap();

        let batch = scan.read(&scan.chunks()[0]).await.unwrap();

        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["analysed_sst", "lat"]);
        // The grid variable holds the chunk. The coordinate holds one value per `lat` of the chunk.
        assert_eq!(batch.columns()[0].values().len(), 604 * 960);
        assert_eq!(batch.columns()[1].values().len(), 604);
    }

    #[tokio::test]
    async fn a_dataset_with_no_arrays_is_an_error() {
        let dataset = only(open(GRIDDED_FILE).await, &[]);

        let error = FileScan::try_new(dataset, 8192).unwrap_err();

        assert!(error.to_string().contains("no arrays"), "{error}");
    }
}
