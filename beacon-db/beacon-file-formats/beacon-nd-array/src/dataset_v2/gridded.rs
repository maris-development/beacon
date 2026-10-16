use std::sync::Arc;

use indexmap::IndexMap;

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    arrow::batch::{c_order_chunk_shape, generate_chunk_subsets},
    dataset_v2::Dataset,
};

pub struct GriddedDataset {
    pub inner: Dataset,
    pub grid_dimensions: Vec<String>,
    pub chunk_sizes: Vec<usize>,
}

impl GriddedDataset {
    pub fn try_new(
        mut inner: Dataset,
        projection: Option<Arc<[String]>>,
    ) -> anyhow::Result<GriddedDataset> {
        // Check if all the variables
        if let Some(projection) = &projection {
            let mut projected_arrays = IndexMap::new();
            // Keep track of the projected dimensions
            let mut projected_dimensions = Vec::new();
            for var in projection.iter() {
                if let Some(array) = inner.arrays.get(var) {
                    projected_arrays.insert(var.clone(), array.clone());
                    for dim in &array.dimensions() {
                        if !projected_dimensions.contains(dim) {
                            projected_dimensions.push(dim.clone());
                        }
                    }
                }
            }
            inner.arrays = projected_arrays;
            inner
                .dimensions
                .retain(|dim, _| projected_dimensions.contains(dim));
        }

        // The highest-rank array defines the grid; a tie keeps the first array.
        let mut grid_array: Option<&Arc<dyn NdArrayD>> = None;
        for array in inner.arrays.values() {
            if grid_array.is_none_or(|grid| array.dimensions().len() > grid.dimensions().len()) {
                grid_array = Some(array);
            }
        }
        let (grid_dimensions, grid_shape, chunk_sizes) = match grid_array {
            Some(array) => (array.dimensions(), array.shape(), array.chunk_shape()),
            None => (Vec::new(), Vec::new(), Vec::new()),
        };

        // Check that each array's dimensions are rank 0 or are included in the grid dimensions in the same order (because being broadcastable is required)
        for (name, array) in &inner.arrays {
            let dimensions = array.dimensions();
            let mut next_axis = 0;
            for (dim, size) in dimensions.iter().zip(array.shape()) {
                let Some(offset) = grid_dimensions[next_axis..].iter().position(|d| d == dim)
                else {
                    anyhow::bail!(
                        "array '{name}' with dimensions {dimensions:?} does not fit the grid {grid_dimensions:?} in order"
                    );
                };
                let axis = next_axis + offset;
                if grid_shape[axis] != size {
                    anyhow::bail!(
                        "array '{name}' has size {size} for dimension '{dim}', the grid has {}",
                        grid_shape[axis]
                    );
                }
                next_axis = axis + 1;
            }
        }

        Ok(GriddedDataset {
            inner,
            grid_dimensions,
            chunk_sizes,
        })
    }

    /// The size of each grid dimension, in grid order.
    pub fn shape(&self) -> Vec<usize> {
        self.grid_dimensions
            .iter()
            .map(|dim| self.inner.dimensions[dim])
            .collect()
    }

    /// Cut the grid into the chunks that the scan reads one at a time.
    ///
    /// The cut follows `chunk_sizes`, and a chunk at the end of an axis is smaller.
    /// A grid that is stored in one chunk is cut into C-order runs of at most
    /// `batch_size` cells, so a large contiguous variable does not become one chunk.
    ///
    /// # Arguments
    ///
    /// * `batch_size` - The largest number of cells in one chunk of a grid that is stored in one chunk
    ///
    /// # Returns
    ///
    /// The chunks in C order. A grid of rank 0 gives one empty subset.
    pub fn chunks(&self, batch_size: usize) -> Vec<ArraySubset> {
        let shape = self.shape();
        let chunk_shape = if self.chunk_sizes == shape {
            c_order_chunk_shape(&shape, batch_size)
        } else {
            self.chunk_sizes.clone()
        };
        generate_chunk_subsets(&shape, &chunk_shape)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::NdArray;

    fn array(shape: &[usize], dims: &[&str]) -> Arc<dyn NdArrayD> {
        let len = shape.iter().product();
        Arc::new(
            NdArray::<i32>::try_new_from_vec_in_mem(
                vec![0; len],
                shape.to_vec(),
                dims.iter().map(|d| d.to_string()).collect(),
                None,
            )
            .unwrap(),
        )
    }

    fn dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let mut dimensions = IndexMap::new();
        for (_, array) in &arrays {
            for (dim, size) in array.dimensions().into_iter().zip(array.shape()) {
                dimensions.insert(dim, size);
            }
        }
        Dataset {
            name: "test".to_string(),
            dimensions,
            arrays: arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
            invented_dimensions: HashSet::new(),
        }
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn grid_follows_the_highest_rank_array() {
        let gridded = GriddedDataset::try_new(
            dataset(vec![
                ("scalar", array(&[], &[])),
                ("time", array(&[2], &["time"])),
                ("lat_lon", array(&[3, 4], &["lat", "lon"])),
                ("time_lon", array(&[2, 4], &["time", "lon"])),
                ("temp", array(&[2, 3, 4], &["time", "lat", "lon"])),
            ]),
            None,
        )
        .unwrap();
        assert_eq!(gridded.grid_dimensions, strings(&["time", "lat", "lon"]));
        assert_eq!(gridded.chunk_sizes, vec![2, 3, 4]);
    }

    #[test]
    fn projection_narrows_the_grid() {
        let projection: Arc<[String]> = strings(&["time"]).into();
        let gridded = GriddedDataset::try_new(
            dataset(vec![
                ("time", array(&[2], &["time"])),
                ("temp", array(&[2, 3], &["time", "lat"])),
            ]),
            Some(projection),
        )
        .unwrap();
        assert_eq!(gridded.grid_dimensions, strings(&["time"]));
        assert_eq!(gridded.chunk_sizes, vec![2]);
        assert_eq!(gridded.inner.dimensions.len(), 1);
    }

    #[test]
    fn reversed_dimensions_are_rejected() {
        let result = GriddedDataset::try_new(
            dataset(vec![
                ("lat_lon", array(&[3, 4], &["lat", "lon"])),
                ("lon_lat", array(&[4, 3], &["lon", "lat"])),
            ]),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn foreign_dimension_is_rejected() {
        let result = GriddedDataset::try_new(
            dataset(vec![
                ("temp", array(&[2, 3], &["time", "lat"])),
                ("depth", array(&[5], &["depth"])),
            ]),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn size_mismatch_is_rejected() {
        let result = GriddedDataset::try_new(
            dataset(vec![
                ("temp", array(&[2, 3], &["time", "lat"])),
                ("lat", array(&[5], &["lat"])),
            ]),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn scalars_give_an_empty_grid() {
        let gridded = GriddedDataset::try_new(
            dataset(vec![("a", array(&[], &[])), ("b", array(&[], &[]))]),
            None,
        )
        .unwrap();
        assert!(gridded.grid_dimensions.is_empty());
        assert!(gridded.chunk_sizes.is_empty());
    }

    fn temp_grid() -> GriddedDataset {
        GriddedDataset::try_new(
            dataset(vec![("temp", array(&[4, 5], &["time", "lat"]))]),
            None,
        )
        .unwrap()
    }

    #[test]
    fn chunks_follow_the_chunk_sizes() {
        let mut gridded = temp_grid();
        gridded.chunk_sizes = vec![3, 5];

        let chunks = gridded.chunks(usize::MAX);

        assert_eq!(chunks.len(), 2);
        assert_eq!(
            (chunks[0].start.clone(), chunks[0].shape.clone()),
            (vec![0, 0], vec![3, 5])
        );
        assert_eq!(
            (chunks[1].start.clone(), chunks[1].shape.clone()),
            (vec![3, 0], vec![1, 5])
        );
    }

    #[test]
    fn a_grid_in_one_chunk_is_cut_by_batch_size() {
        let chunks = temp_grid().chunks(10);

        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|chunk| chunk.shape == vec![2, 5]));
    }

    #[test]
    fn a_grid_of_rank_zero_gives_one_chunk() {
        let gridded = GriddedDataset::try_new(dataset(vec![("a", array(&[], &[]))]), None).unwrap();

        let chunks = gridded.chunks(10);

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].shape.is_empty());
    }
}
