//! The test fixtures of the scan.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use beacon_nd_dataset::{
    Attributes, Dimension, Dimensions, Error, MemoryDataset, MemoryVariable, NdArrowArray,
    NdDataset, NdVariable, Region,
};
use beacon_nd_encoding::encoding::encoded_schema;
use datafusion_datasource::PartitionedFile;
use object_store::{ObjectMeta, ObjectStore};

use crate::opener::NdFileOpener;

/// What the test opener gives for one path.
// Later tasks write `Entry::Dataset(grid_dataset(..))`, so the variant holds the dataset unboxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum Entry {
    /// The opener gives this dataset.
    Dataset(MemoryDataset),
    /// The opener gives a `BrokenDataset`: each read fails.
    Broken,
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

#[async_trait]
impl NdFileOpener for MemoryOpener {
    fn format_name(&self) -> &'static str {
        "memory"
    }

    async fn open(
        &self,
        _store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> beacon_nd_dataset::Result<Arc<dyn NdDataset>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        let path = file.location.as_ref();
        match self.entries.get(path) {
            Some(Entry::Dataset(dataset)) => Ok(Arc::new(dataset.clone())),
            Some(Entry::Broken) => Ok(Arc::new(BrokenDataset::new())),
            Some(Entry::Invalid(reason)) => Err(Error::format(*reason)),
            Some(Entry::Unsupported(reason)) => Err(Error::Unsupported {
                dataset: path.to_string(),
                reason: reason.to_string(),
            }),
            None => Err(Error::format("no such file")),
        }
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
fn grid_variables() -> Vec<MemoryVariable> {
    vec![
        ramp("temp", &[("time", 1), ("lat", 8), ("lon", 6)]).with_chunk_shape(vec![1, 4, 3]),
        ramp("lat", &[("lat", 8)]),
        ramp("lon", &[("lon", 6)]),
    ]
}

/// temp(time=1, lat=8, lon=6) in chunks of [1, 4, 3], lat(lat=8), lon(lon=6).
pub(crate) fn grid_dataset(name: &str) -> MemoryDataset {
    MemoryDataset::try_new(name, grid_variables()).unwrap()
}

/// The grid fixture with the rank-0 Float64 variable `level`.
pub(crate) fn leveled_dataset(name: &str, level: Option<f64>) -> MemoryDataset {
    let value = Arc::new(Float64Array::from(vec![level]));
    let level = MemoryVariable::new("level", NdArrowArray::try_new_scalar(value).unwrap());
    let mut variables = grid_variables();
    variables.push(level);
    MemoryDataset::try_new(name, variables).unwrap()
}

/// A dataset with the one 1-D variable `name` on the dimension `name`.
pub(crate) fn coordinate(name: &str, values: ArrayRef) -> MemoryDataset {
    let dimensions = Dimensions::try_new([Dimension::new(name, values.len())]).unwrap();
    let variable = MemoryVariable::new(name, NdArrowArray::try_new(values, dimensions).unwrap());
    MemoryDataset::try_new("test.nd", vec![variable]).unwrap()
}

/// A logical schema with a nullable field for each of `fields`.
pub(crate) fn logical(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// The `beacon.nd` encoded form of `logical(fields)`.
pub(crate) fn encoded(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(encoded_schema(&logical(fields)))
}

/// The dataset `broken.nd` with the variable `temp(n=3)`. Each read of `temp` fails.
#[derive(Debug)]
pub(crate) struct BrokenDataset {
    dimensions: Dimensions,
    attributes: Attributes,
}

impl BrokenDataset {
    pub(crate) fn new() -> Self {
        Self {
            dimensions: Dimensions::try_new([Dimension::new("n", 3)]).unwrap(),
            attributes: Attributes::new(),
        }
    }
}

impl NdDataset for BrokenDataset {
    fn name(&self) -> &str {
        "broken.nd"
    }
    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }
    fn attributes(&self) -> &Attributes {
        &self.attributes
    }
    fn variable_names(&self) -> Vec<Arc<str>> {
        vec![Arc::from("temp")]
    }
    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        (name == "temp").then(|| {
            Arc::new(BrokenVariable {
                dimensions: self.dimensions.clone(),
                attributes: Attributes::new(),
            }) as Arc<dyn NdVariable>
        })
    }
}

/// The variable of `BrokenDataset`.
#[derive(Debug)]
struct BrokenVariable {
    dimensions: Dimensions,
    attributes: Attributes,
}

#[async_trait]
impl NdVariable for BrokenVariable {
    fn name(&self) -> &str {
        "temp"
    }
    fn data_type(&self) -> &DataType {
        &DataType::Float64
    }
    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }
    fn chunk_shape(&self) -> Option<&[usize]> {
        None
    }
    fn attributes(&self) -> &Attributes {
        &self.attributes
    }
    async fn read(&self, _: &Region) -> beacon_nd_dataset::Result<NdArrowArray> {
        Err(Error::format("disk gone"))
    }
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
        assert_eq!(grid.len(), 4);
        assert!(grid.chunks().all(|chunk| chunk.len() == 12));
        assert_eq!(grid.chunk(3).start(), [0, 4, 3]);
    }
}
