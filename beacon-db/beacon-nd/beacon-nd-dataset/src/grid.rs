//! The chunk grid of one dataset.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use nd_arrow_array::{Dimension, Dimensions, NdRecordBatch};

use crate::{Error, NdDataset, NdVariable, Region, Result};

/// One dataset, cut into chunks in C order.
#[derive(Debug, Clone)]
pub struct ChunkGrid {
    /// The dimensions the chunks index, outermost first.
    dimensions: Dimensions,
    /// The chunks, in C order.
    chunks: Vec<Region>,
}

impl ChunkGrid {
    /// Cut `dataset` into chunks of about `batch_size` rows.
    ///
    /// A grid of at most `batch_size` rows is one chunk. Else a native chunk
    /// layout sets the chunks. Else the grid is cut at `batch_size` from the
    /// last axis. A `batch_size` of 0 counts as 1.
    ///
    /// The grid spans the dimensions that the variables use. A variable on
    /// all of them drives the grid. A dataset with no variable has the grid
    /// of all its dimensions.
    ///
    /// The chunks depend only on the dataset and on `batch_size`.
    /// `Unsupported` when the chunk count does not fit in `usize`.
    pub fn try_new(dataset: &dyn NdDataset, batch_size: usize) -> Result<Self> {
        let batch_size = batch_size.max(1);
        let (dimensions, chunk_shape) = layout(dataset)?;
        let shape = dimensions.shape();
        let rows = shape
            .iter()
            .try_fold(1usize, |rows, &len| rows.checked_mul(len));
        // A small grid is one chunk, else native chunks win, else cut at the batch size.
        let chunk_shape = if rows.is_some_and(|rows| rows <= batch_size) {
            shape.clone()
        } else if chunk_shape != shape {
            chunk_shape
        } else {
            c_order_chunk_shape(&shape, batch_size)
        };
        let chunks = chunk_regions(&shape, &chunk_shape).ok_or_else(|| Error::Unsupported {
            dataset: dataset.name().to_string(),
            reason: format!(
                "the grid {shape:?} in chunks of {chunk_shape:?} has more chunks than fit in usize"
            ),
        })?;
        Ok(Self { dimensions, chunks })
    }

    /// The dimensions the chunks index, outermost first.
    pub fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    /// The chunks, in C order.
    pub fn chunks(&self) -> &[Region] {
        &self.chunks
    }

    /// The part of `chunk` that `variable` holds, on the axes of `variable`.
    pub fn variable_region(&self, variable: &dyn NdVariable, chunk: &Region) -> Region {
        let mut start = Vec::new();
        let mut shape = Vec::new();
        for dimension in variable.dimensions() {
            if let Some(axis) = self.dimensions.position(dimension.name()) {
                start.push(chunk.start[axis]);
                shape.push(chunk.shape[axis]);
            }
        }
        Region::new(start, shape)
    }
}

/// Read each variable of `dataset` in `chunk` into one un-broadcast batch.
///
/// The target has the dimensions of `grid` and the shape of `chunk`. The
/// function reads each variable on its own axes, so it reads a coordinate
/// once per chunk and not once per row. The columns follow `variable_names()`.
pub async fn read_chunk(
    dataset: &dyn NdDataset,
    grid: &ChunkGrid,
    chunk: &Region,
) -> Result<NdRecordBatch> {
    let target = Dimensions::try_new(
        grid.dimensions
            .iter()
            .zip(&chunk.shape)
            .map(|(dimension, &size)| Dimension::new(dimension.name(), size)),
    )?;

    let names = dataset.variable_names();
    let mut fields = Vec::with_capacity(names.len());
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let variable = dataset.variable(&name).ok_or_else(|| Error::Invalid {
            dataset: dataset.name().to_string(),
            reason: format!("the variable '{name}' is not available"),
        })?;
        let region = grid.variable_region(variable.as_ref(), chunk);
        columns.push(variable.read(&region).await?);
        fields.push(Field::new(
            variable.name(),
            variable.data_type().clone(),
            true,
        ));
    }

    Ok(NdRecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
        target,
    )?)
}

/// The dimensions and the chunk shape that drive the grid.
fn layout(dataset: &dyn NdDataset) -> Result<(Dimensions, Vec<usize>)> {
    let mut variables = Vec::new();
    for name in dataset.variable_names() {
        let variable = dataset.variable(&name).ok_or_else(|| Error::Invalid {
            dataset: dataset.name().to_string(),
            reason: format!("the variable '{name}' is not available"),
        })?;
        variables.push(variable);
    }
    // A dataset with no variable still has rows, for a count.
    if variables.is_empty() {
        let dimensions = dataset.dimensions().clone();
        let shape = dimensions.shape();
        return Ok((dimensions, shape));
    }

    // An axis that no variable uses would repeat each value once per chunk.
    let used: Vec<Dimension> = dataset
        .dimensions()
        .iter()
        .filter(|dimension| {
            variables
                .iter()
                .any(|variable| variable.dimensions().contains(dimension.name()))
        })
        .cloned()
        .collect();

    // A variable on every used dimension drives the grid, and the others broadcast onto it.
    if let Some(driver) = variables
        .iter()
        .filter(|variable| variable.dimensions().rank() == used.len())
        .max_by_key(|variable| element_count(variable.dimensions()))
    {
        let dimensions = driver.dimensions().clone();
        let chunk_shape = driver
            .chunk_shape()
            .map_or_else(|| dimensions.shape(), <[usize]>::to_vec);
        return Ok((dimensions, chunk_shape));
    }

    // No variable spans every used dimension, so those dimensions are the grid.
    let dimensions = Dimensions::try_new(used)?;
    let shape = dimensions.shape();
    Ok((dimensions, shape))
}

/// The element count, or `usize::MAX` when it does not fit.
fn element_count(dimensions: &Dimensions) -> usize {
    dimensions.iter().fold(1usize, |count, dimension| {
        count.saturating_mul(dimension.size())
    })
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
///
/// `None` when the chunk count does not fit in `usize`.
fn chunk_regions(shape: &[usize], chunk_shape: &[usize]) -> Option<Vec<Region>> {
    if shape.is_empty() {
        return Some(vec![Region::new(vec![], vec![])]);
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
        return Some(vec![]);
    }

    let total = counts
        .iter()
        .try_fold(1usize, |total, &count| total.checked_mul(count))?;
    let mut regions = Vec::with_capacity(total);
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
        regions.push(Region::new(start, size));
    }
    Some(regions)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type};

    use super::*;
    use crate::memory::testing::{Meta, dataset, dims, meta, ramp};
    use crate::{DatasetSchema, MemoryDataset, select_columns};

    fn sst() -> MemoryDataset {
        dataset(vec![
            ramp("analysed_sst", &[("time", 1), ("lat", 1208), ("lon", 1920)])
                .with_chunk_shape(vec![1, 604, 960]),
            ramp("lat", &[("lat", 1208)]),
        ])
    }

    #[test]
    fn a_native_chunk_layout_sets_the_grid() {
        let grid = ChunkGrid::try_new(&sst(), 8192).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["time", "lat", "lon"]);
        assert_eq!(grid.chunks().len(), 4);
        assert_eq!(
            grid.chunks()[0],
            Region::new(vec![0, 0, 0], vec![1, 604, 960])
        );
        assert_eq!(grid.chunks()[3].start, vec![0, 604, 960]);
        assert_eq!(
            grid.chunks().iter().map(Region::len).sum::<usize>(),
            1208 * 1920
        );
    }

    #[test]
    fn a_small_grid_is_one_chunk_whatever_its_native_chunks() {
        let grid = ChunkGrid::try_new(&sst(), 1208 * 1920).unwrap();
        assert_eq!(
            grid.chunks(),
            [Region::new(vec![0, 0, 0], vec![1, 1208, 1920])]
        );
    }

    #[test]
    fn a_contiguous_array_is_cut_at_the_batch_size() {
        let ds = dataset(vec![ramp(
            "ice",
            &[("time", 1), ("lat", 1208), ("lon", 1920)],
        )]);
        let grid = ChunkGrid::try_new(&ds, 4096).unwrap();
        assert_eq!(grid.chunks()[0].shape, vec![1, 2, 1920]);
        assert_eq!(grid.chunks().len(), 604);
    }

    #[test]
    fn a_variable_region_lies_on_the_axes_of_the_variable() {
        let ds = sst();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        let lat = ds.variable("lat").unwrap();
        assert_eq!(
            grid.variable_region(lat.as_ref(), &grid.chunks()[3]),
            Region::new(vec![604], vec![604])
        );
    }

    #[test]
    fn the_grid_spans_only_the_used_dimensions() {
        let time: Arc<dyn NdVariable> = Arc::new(meta("time", DataType::Float64, &[("time", 5)]));
        let ds = DatasetSchema::try_new("test.nd", dims(&[("time", 5), ("lat", 3)]), vec![time])
            .unwrap();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["time"]);
        assert_eq!(grid.chunks(), [Region::new(vec![0], vec![5])]);
    }

    #[test]
    fn variables_on_disjoint_dimensions_give_the_cross_product() {
        let ds = dataset(vec![ramp("lat", &[("lat", 3)]), ramp("lon", &[("lon", 4)])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["lat", "lon"]);
        assert_eq!(grid.chunks(), [Region::new(vec![0, 0], vec![3, 4])]);
    }

    #[test]
    fn the_last_variable_wins_a_driver_tie() {
        let ds = dataset(vec![
            ramp("a", &[("x", 2), ("y", 3)]).with_chunk_shape(vec![1, 3]),
            ramp("b", &[("x", 2), ("y", 3)]).with_chunk_shape(vec![2, 1]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        assert_eq!(grid.chunks()[0].shape, vec![2, 1]);
    }

    #[test]
    fn a_dataset_with_no_variable_has_the_grid_of_its_dimensions() {
        let ds = DatasetSchema::try_new(
            "test.nd",
            dims(&[("time", 1), ("lat", 1208), ("lon", 1920)]),
            vec![],
        )
        .unwrap();
        let grid = ChunkGrid::try_new(&ds, 1208 * 1920).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["time", "lat", "lon"]);
        assert_eq!(grid.chunks().len(), 1);
        assert_eq!(grid.chunks()[0].len(), 1208 * 1920);
    }

    #[test]
    fn a_rank_0_grid_is_one_chunk_of_one_row() {
        let ds = dataset(vec![ramp("title", &[])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.chunks(), [Region::new(vec![], vec![])]);
        assert_eq!(grid.chunks()[0].len(), 1);
    }

    #[test]
    fn a_dimension_of_size_0_gives_no_chunk() {
        let ds = dataset(vec![ramp("v", &[("n", 0), ("m", 2)])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert!(grid.chunks().is_empty());
    }

    #[test]
    fn a_batch_size_of_0_counts_as_1() {
        let ds = dataset(vec![ramp("v", &[("n", 3)])]);
        let grid = ChunkGrid::try_new(&ds, 0).unwrap();
        assert_eq!(grid.chunks().len(), 3);
        assert!(grid.chunks().iter().all(|chunk| chunk.shape == vec![1]));
    }

    #[test]
    fn a_row_count_that_overflows_is_cut() {
        let sizes = [("a", 1 << 40), ("b", 1 << 40)];
        let v = meta("v", DataType::Float64, &sizes)
            .0
            .with_chunk_shape(vec![1 << 39, 1 << 40]);
        let ds = DatasetSchema::try_new("test.nd", dims(&sizes), vec![Arc::new(Meta(v))]).unwrap();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.chunks().len(), 2);
        assert_eq!(grid.chunks()[1].start, vec![1 << 39, 0]);
    }

    #[tokio::test]
    async fn a_chunk_reads_each_variable_on_its_own_axes() {
        let ds = sst();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        let batch = read_chunk(&ds, &grid, &grid.chunks()[0]).await.unwrap();
        let names: Vec<&str> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["analysed_sst", "lat"]);
        assert_eq!(batch.column(0).len(), 604 * 960);
        assert_eq!(batch.column(1).len(), 604);
        assert_eq!(batch.target().shape(), vec![1, 604, 960]);
        assert_eq!(ds.reads("analysed_sst"), 1);
        assert_eq!(ds.reads("lat"), 1);
    }

    #[tokio::test]
    async fn a_chunk_holds_the_values_of_its_region() {
        let ds = dataset(vec![ramp("v", &[("n", 6)])]);
        let grid = ChunkGrid::try_new(&ds, 4).unwrap();
        let batch = read_chunk(&ds, &grid, &grid.chunks()[1]).await.unwrap();
        let values = batch
            .column(0)
            .values()
            .as_primitive::<Float64Type>()
            .values()
            .to_vec();
        assert_eq!(values, vec![4.0, 5.0]);
    }

    #[tokio::test]
    async fn a_chunk_of_no_variable_has_rows_and_no_read() {
        let ds = sst();
        let view = select_columns(Arc::new(ds.clone()), &[]);
        let grid = ChunkGrid::try_new(view.as_ref(), 8192).unwrap();
        let batch = read_chunk(view.as_ref(), &grid, &grid.chunks()[0])
            .await
            .unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), grid.chunks()[0].len());
        assert_eq!(ds.reads("analysed_sst") + ds.reads("lat"), 0);
    }

    #[tokio::test]
    async fn a_rank_0_variable_goes_in_each_chunk() {
        let ds = dataset(vec![ramp("v", &[("n", 6)]), ramp("title", &[])]);
        let grid = ChunkGrid::try_new(&ds, 4).unwrap();
        let batch = read_chunk(&ds, &grid, &grid.chunks()[0]).await.unwrap();
        assert_eq!(batch.column(1).len(), 1);
    }
}
