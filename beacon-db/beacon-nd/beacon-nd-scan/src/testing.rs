//! The test fixtures of the scan.
// Later tasks use the opener and `entry`; until they land, some items have no caller.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Float64Array;
use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
use datafusion_datasource::PartitionedFile;

/// What the test opener gives for one path.
// Later tasks write `Entry::Dataset(grid_dataset(..))`, so the variant holds the dataset unboxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum Entry {
    /// The opener gives this dataset.
    Dataset(MemoryDataset),
    /// The opener fails with `Invalid` and this reason.
    Invalid(&'static str),
    /// The opener fails with `Unsupported` and this reason.
    Unsupported(&'static str),
}

/// A file opener that gives datasets from memory and counts its opens.
#[derive(Debug, Default)]
pub(crate) struct MemoryOpener {
    /// What the opener gives for each path.
    pub(crate) entries: HashMap<String, Entry>,
    /// The count of opens.
    pub(crate) opens: AtomicUsize,
}

impl MemoryOpener {
    /// Give `entry` for `path`.
    pub(crate) fn with(mut self, path: &str, entry: Entry) -> Self {
        self.entries.insert(path.to_string(), entry);
        self
    }

    /// The count of opens.
    pub(crate) fn opens(&self) -> usize {
        self.opens.load(Ordering::Relaxed)
    }
}

/// A Float64 variable with the values 0.0, 1.0, .. in C order.
pub(crate) fn ramp(name: &str, dims: &[(&str, usize)]) -> MemoryVariable {
    let dimensions =
        Dimensions::try_new(dims.iter().map(|(name, size)| Dimension::new(*name, *size))).unwrap();
    let values = Float64Array::from_iter_values((0..dimensions.num_elements()).map(|i| i as f64));
    MemoryVariable::new(
        name,
        NdArrowArray::try_new(Arc::new(values), dimensions).unwrap(),
    )
}

/// temp(time=1, lat=8, lon=6) in chunks of [1, 4, 3], lat(lat=8), lon(lon=6).
pub(crate) fn grid_dataset(name: &str) -> MemoryDataset {
    let temp = ramp("temp", &[("time", 1), ("lat", 8), ("lon", 6)]).with_chunk_shape(vec![1, 4, 3]);
    let lat = ramp("lat", &[("lat", 8)]);
    let lon = ramp("lon", &[("lon", 6)]);
    MemoryDataset::try_new(name, vec![temp, lat, lon]).unwrap()
}

/// `PartitionedFile::new(path, 1000)`.
pub(crate) fn entry(path: &str) -> PartitionedFile {
    PartitionedFile::new(path, 1000)
}

#[cfg(test)]
mod tests {
    use beacon_nd_dataset::ChunkGrid;

    use super::grid_dataset;

    #[test]
    fn the_grid_fixture_has_four_chunks_of_twelve_rows() {
        let grid = ChunkGrid::try_new(&grid_dataset("a.nd"), 12).unwrap();
        assert_eq!(grid.dimensions().names(), vec!["time", "lat", "lon"]);
        assert_eq!(grid.chunks().len(), 4);
        assert!(grid.chunks().iter().all(|chunk| chunk.len() == 12));
        assert_eq!(grid.chunks()[3].start, vec![0, 4, 3]);
    }
}
