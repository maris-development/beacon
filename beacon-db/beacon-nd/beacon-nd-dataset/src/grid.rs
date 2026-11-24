//! The chunk grid of one dataset. This module does no I/O.

use std::ops::Range;

use nd_arrow_array::Dimensions;

use crate::dataset::{used_dimensions, variables, wrong_chunk_rank};
use crate::{Error, NdDataset, NdVariable, Region, Result};

/// One dataset, cut into chunks in C order.
#[derive(Debug, Clone)]
pub struct ChunkGrid {
    /// The dimensions the chunks index, outermost first.
    dimensions: Dimensions,
    /// The shape of a chunk that is not at an edge. Each entry is at least 1 on an axis with chunks.
    chunk_shape: Vec<usize>,
    /// The chunk count of each axis.
    counts: Vec<usize>,
    /// The chunk count of the grid.
    len: usize,
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
        // An axis of size 0 has no chunk, so the divide never sees a chunk of size 0.
        let counts: Vec<usize> = shape
            .iter()
            .zip(&chunk_shape)
            .map(|(&len, &chunk)| if len == 0 { 0 } else { len.div_ceil(chunk) })
            .collect();
        let len = if counts.contains(&0) {
            0
        } else {
            counts
                .iter()
                .try_fold(1usize, |total, &count| total.checked_mul(count))
                .ok_or_else(|| Error::Unsupported {
                    dataset: dataset.name().to_string(),
                    reason: format!(
                        "the grid {shape:?} in chunks of {chunk_shape:?} has more chunks than fit in usize"
                    ),
                })?
        };
        Ok(Self {
            dimensions,
            chunk_shape,
            counts,
            len,
        })
    }

    /// The chunk count of the grid.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when the grid has no chunk.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Chunk `index` in C order. An edge chunk is smaller.
    ///
    /// # Panics
    ///
    /// When `index` is not below `len()`.
    pub fn chunk(&self, index: usize) -> Region {
        assert!(
            index < self.len,
            "chunk {index} of a grid with {} chunks",
            self.len
        );
        let rank = self.counts.len();
        let mut start = vec![0usize; rank];
        // Start from the size of each axis, then cut it to the chunk.
        let mut shape = self.dimensions.shape();
        let mut rest = index;
        for axis in (0..rank).rev() {
            let chunk = self.chunk_shape[axis];
            start[axis] = (rest % self.counts[axis]) * chunk;
            shape[axis] = chunk.min(shape[axis] - start[axis]);
            rest /= self.counts[axis];
        }
        Region::new(start, shape)
    }

    /// Each chunk, in C order.
    pub fn chunks(&self) -> impl ExactSizeIterator<Item = Region> + '_ {
        (0..self.len).map(|index| self.chunk(index))
    }

    /// The smallest region that holds each chunk of `range`. `None` for an empty range.
    ///
    /// # Panics
    ///
    /// When `range` is not empty and does not lie in `0..len()`.
    pub fn span(&self, range: Range<usize>) -> Option<Region> {
        if range.is_empty() {
            return None;
        }
        let first = self.chunk(range.start);
        let last = self.chunk(range.end - 1);
        let mut start = first.start().to_vec();
        let mut shape = self.dimensions.shape();
        // While the first and the last chunk have the same place on each outer axis,
        // each chunk of the range has that place too.
        let mut same_outer = true;
        for axis in 0..shape.len() {
            if same_outer {
                shape[axis] = last.start()[axis] + last.shape()[axis] - first.start()[axis];
                same_outer = first.start()[axis] == last.start()[axis];
            } else {
                // The chunks between the first and the last cover this axis in full.
                start[axis] = 0;
            }
        }
        Some(Region::new(start, shape))
    }

    /// The dimensions the chunks index, outermost first.
    pub fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }
}

/// The dimensions and the chunk shape that drive the grid.
fn layout(dataset: &dyn NdDataset) -> Result<(Dimensions, Vec<usize>)> {
    let variables = variables(dataset)?;
    // A dataset with no variable still has rows, for a count.
    if variables.is_empty() {
        let dimensions = dataset.dimensions().clone();
        let shape = dimensions.shape();
        return Ok((dimensions, shape));
    }

    // An axis that no variable uses would repeat each value once per chunk.
    let used = used_dimensions(dataset, &variables)?;

    // A variable on every used dimension drives the grid, and the others broadcast onto it.
    // Each such variable has the same set of dimensions. The last one wins, with its axis order.
    if let Some(driver) = variables
        .iter()
        .rev()
        .find(|variable| variable.dimensions().rank() == used.rank())
    {
        let chunk_shape = native_chunk_shape(dataset, driver.as_ref())?;
        return Ok((driver.dimensions().clone(), chunk_shape));
    }

    // No variable spans every used dimension, so those dimensions are the grid.
    let shape = used.shape();
    Ok((used, shape))
}

/// The native chunk shape of `driver`, or its shape when it has no chunks.
///
/// Each entry of a native shape is at least 1. `Invalid` on a wrong rank.
fn native_chunk_shape(dataset: &dyn NdDataset, driver: &dyn NdVariable) -> Result<Vec<usize>> {
    match driver.chunk_shape() {
        None => Ok(driver.dimensions().shape()),
        Some(shape) if shape.len() == driver.dimensions().rank() => {
            Ok(shape.iter().map(|&size| size.max(1)).collect())
        }
        Some(shape) => Err(Error::Invalid {
            dataset: dataset.name().to_string(),
            reason: wrong_chunk_rank(driver, shape.len()),
        }),
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use nd_arrow_array::Attributes;

    use super::*;
    use crate::memory::testing::{Meta, dataset, dims, meta, ramp};
    use crate::{DatasetSchema, MemoryDataset};

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
        assert_eq!(grid.len(), 4);
        assert_eq!(grid.chunks().len(), 4);
        assert_eq!(grid.chunk(0), Region::new(vec![0, 0, 0], vec![1, 604, 960]));
        assert_eq!(grid.chunk(3).start(), [0, 604, 960]);
        assert_eq!(
            grid.chunks().map(|chunk| chunk.len()).sum::<usize>(),
            1208 * 1920
        );
    }

    /// Each chunk of `grid`, in C order.
    fn all(grid: &ChunkGrid) -> Vec<Region> {
        grid.chunks().collect()
    }

    #[test]
    fn a_small_grid_is_one_chunk_whatever_its_native_chunks() {
        let grid = ChunkGrid::try_new(&sst(), 1208 * 1920).unwrap();
        assert_eq!(
            all(&grid),
            [Region::new(vec![0, 0, 0], vec![1, 1208, 1920])]
        );
    }

    #[test]
    fn an_edge_chunk_is_smaller() {
        let ds = dataset(vec![
            ramp("v", &[("a", 3), ("b", 5)]).with_chunk_shape(vec![2, 2]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        assert_eq!(grid.len(), 6);
        assert_eq!(
            all(&grid),
            [
                Region::new(vec![0, 0], vec![2, 2]),
                Region::new(vec![0, 2], vec![2, 2]),
                Region::new(vec![0, 4], vec![2, 1]),
                Region::new(vec![2, 0], vec![1, 2]),
                Region::new(vec![2, 2], vec![1, 2]),
                Region::new(vec![2, 4], vec![1, 1]),
            ]
        );
    }

    #[test]
    fn a_native_chunk_larger_than_the_grid_is_cut_to_the_grid() {
        let ds = dataset(vec![
            ramp("v", &[("a", 3), ("b", 5)]).with_chunk_shape(vec![8, 2]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        assert_eq!(grid.len(), 3);
        assert_eq!(grid.chunk(2), Region::new(vec![0, 4], vec![3, 1]));
    }

    #[test]
    fn a_grid_with_very_many_chunks_holds_no_chunk_list() {
        let sizes = [("a", 1 << 40), ("b", 1 << 20)];
        let v: Arc<dyn NdVariable> = Arc::new(meta("v", DataType::Float64, &sizes));
        let ds = DatasetSchema::try_new("test.nd", dims(&sizes), vec![v]).unwrap();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.len(), 1 << 47);
        assert_eq!(grid.chunk(0), Region::new(vec![0, 0], vec![1, 8192]));
        assert_eq!(
            grid.chunk((1 << 47) - 1),
            Region::new(vec![(1 << 40) - 1, (1 << 20) - 8192], vec![1, 8192])
        );
    }

    #[test]
    fn a_span_is_the_smallest_region_with_each_chunk_of_a_range() {
        // A grid of 3 x 5 in chunks of [2, 2]: 2 rows of 3 chunks.
        let ds = dataset(vec![
            ramp("v", &[("a", 3), ("b", 5)]).with_chunk_shape(vec![2, 2]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        assert_eq!(grid.span(0..0), None);
        assert_eq!(grid.span(3..3), None);
        // One chunk is its own span.
        assert_eq!(grid.span(4..5), Some(grid.chunk(4)));
        // Two chunks of one row.
        assert_eq!(grid.span(1..3), Some(Region::new(vec![0, 2], vec![2, 3])));
        // A range over two rows covers the inner axis in full.
        assert_eq!(grid.span(2..4), Some(Region::new(vec![0, 0], vec![3, 5])));
        assert_eq!(grid.span(0..6), Some(Region::new(vec![0, 0], vec![3, 5])));
    }

    #[test]
    fn a_span_holds_each_chunk_of_each_range() {
        let ds = dataset(vec![
            ramp("v", &[("a", 3), ("b", 5), ("c", 4)]).with_chunk_shape(vec![1, 2, 3]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        for from in 0..grid.len() {
            for to in from + 1..=grid.len() {
                let span = grid.span(from..to).unwrap();
                for chunk in (from..to).map(|index| grid.chunk(index)) {
                    for axis in 0..3 {
                        let end = chunk.start()[axis] + chunk.shape()[axis];
                        assert!(span.start()[axis] <= chunk.start()[axis], "{from}..{to}");
                        assert!(
                            end <= span.start()[axis] + span.shape()[axis],
                            "{from}..{to}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "chunk 4 of a grid with 4 chunks")]
    fn a_chunk_past_the_grid_panics() {
        ChunkGrid::try_new(&sst(), 8192).unwrap().chunk(4);
    }

    #[test]
    fn a_contiguous_array_is_cut_at_the_batch_size() {
        let ds = dataset(vec![ramp(
            "ice",
            &[("time", 1), ("lat", 1208), ("lon", 1920)],
        )]);
        let grid = ChunkGrid::try_new(&ds, 4096).unwrap();
        assert_eq!(grid.chunk(0).shape(), [1, 2, 1920]);
        assert_eq!(grid.len(), 604);
    }

    #[test]
    fn the_grid_spans_only_the_used_dimensions() {
        let time: Arc<dyn NdVariable> = Arc::new(meta("time", DataType::Float64, &[("time", 5)]));
        let ds = DatasetSchema::try_new("test.nd", dims(&[("time", 5), ("lat", 3)]), vec![time])
            .unwrap();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["time"]);
        assert_eq!(all(&grid), [Region::new(vec![0], vec![5])]);
    }

    #[test]
    fn variables_on_disjoint_dimensions_give_the_cross_product() {
        let ds = dataset(vec![ramp("lat", &[("lat", 3)]), ramp("lon", &[("lon", 4)])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["lat", "lon"]);
        assert_eq!(all(&grid), [Region::new(vec![0, 0], vec![3, 4])]);
    }

    #[test]
    fn the_last_variable_wins_a_driver_tie() {
        let ds = dataset(vec![
            ramp("a", &[("x", 2), ("y", 3)]).with_chunk_shape(vec![1, 3]),
            ramp("b", &[("x", 2), ("y", 3)]).with_chunk_shape(vec![2, 1]),
        ]);
        let grid = ChunkGrid::try_new(&ds, 2).unwrap();
        assert_eq!(grid.chunk(0).shape(), [2, 1]);
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
        assert_eq!(grid.len(), 1);
        assert_eq!(grid.chunk(0).len(), 1208 * 1920);
    }

    #[test]
    fn a_rank_0_grid_is_one_chunk_of_one_row() {
        let ds = dataset(vec![ramp("title", &[])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(all(&grid), [Region::new(vec![], vec![])]);
        assert_eq!(grid.chunk(0).len(), 1);
    }

    #[test]
    fn a_dimension_of_size_0_gives_no_chunk() {
        let ds = dataset(vec![ramp("v", &[("n", 0), ("m", 2)])]);
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert!(grid.is_empty());
        assert_eq!(grid.chunks().len(), 0);
    }

    #[test]
    fn a_batch_size_of_0_counts_as_1() {
        let ds = dataset(vec![ramp("v", &[("n", 3)])]);
        let grid = ChunkGrid::try_new(&ds, 0).unwrap();
        assert_eq!(grid.len(), 3);
        assert!(grid.chunks().all(|chunk| chunk.shape() == [1]));
    }

    #[test]
    fn a_row_count_that_overflows_is_cut() {
        let sizes = [("a", 1 << 40), ("b", 1 << 40)];
        let v = meta("v", DataType::Float64, &sizes)
            .0
            .with_chunk_shape(vec![1 << 39, 1 << 40]);
        let ds = DatasetSchema::try_new("test.nd", dims(&sizes), vec![Arc::new(Meta(v))]).unwrap();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        assert_eq!(grid.len(), 2);
        assert_eq!(grid.chunk(1).start(), [1 << 39, 0]);
    }

    #[test]
    fn a_chunk_count_that_overflows_is_unsupported() {
        let sizes = [("a", 1 << 40), ("b", 1 << 40)];
        let v: Arc<dyn NdVariable> = Arc::new(meta("v", DataType::Float64, &sizes));
        let ds = DatasetSchema::try_new("test.nd", dims(&sizes), vec![v]).unwrap();
        let result = ChunkGrid::try_new(&ds, 8192);
        assert!(
            matches!(result, Err(Error::Unsupported { .. })),
            "{result:?}"
        );
    }

    /// A dataset with the one variable `v(a=4, b=4)` and the chunk shape `[2]`.
    #[derive(Debug)]
    struct WrongChunkRank {
        dimensions: Dimensions,
        attributes: Attributes,
    }

    impl NdDataset for WrongChunkRank {
        fn name(&self) -> &str {
            "test.nd"
        }
        fn dimensions(&self) -> &Dimensions {
            &self.dimensions
        }
        fn attributes(&self) -> &Attributes {
            &self.attributes
        }
        fn variable_names(&self) -> Vec<Arc<str>> {
            vec![Arc::from("v")]
        }
        fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
            (name == "v").then(|| {
                let v = meta("v", DataType::Float64, &[("a", 4), ("b", 4)])
                    .0
                    .with_chunk_shape(vec![2]);
                Arc::new(Meta(v)) as Arc<dyn NdVariable>
            })
        }
    }

    #[test]
    fn a_chunk_shape_of_the_wrong_rank_is_invalid() {
        let ds = WrongChunkRank {
            dimensions: dims(&[("a", 4), ("b", 4)]),
            attributes: Attributes::new(),
        };
        let result = ChunkGrid::try_new(&ds, 2);
        assert!(matches!(result, Err(Error::Invalid { .. })), "{result:?}");
    }
}
