//! A dataset in memory, for tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use async_trait::async_trait;
use indexmap::IndexMap;
use nd_arrow_array::compute::{Selector, Slice, isel};
use nd_arrow_array::{Attributes, Dimension, Dimensions, NdArrowArray};

use crate::{DatasetSchema, NdDataset, NdVariable, Region, Result, VariableInfo};

/// A variable that holds its values in memory and counts its reads.
#[derive(Debug)]
pub struct MemoryVariable {
    info: VariableInfo,
    array: NdArrowArray,
    reads: AtomicUsize,
}

impl MemoryVariable {
    /// The type, the dimensions and the attributes come from `array`.
    pub fn new(name: impl Into<Arc<str>>, array: NdArrowArray) -> Self {
        let info = VariableInfo::new(name, array.data_type().clone(), array.dims().clone())
            .with_attributes(array.attributes().clone());
        Self {
            info,
            array,
            reads: AtomicUsize::new(0),
        }
    }

    /// Set the native chunk shape.
    pub fn with_chunk_shape(mut self, shape: Vec<usize>) -> Self {
        self.info = self.info.with_chunk_shape(shape);
        self
    }

    /// The count of `read` calls.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NdVariable for MemoryVariable {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn data_type(&self) -> &DataType {
        &self.info.data_type
    }

    fn dimensions(&self) -> &Dimensions {
        &self.info.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        self.info.chunk_shape.as_deref()
    }

    fn attributes(&self) -> &Attributes {
        &self.info.attributes
    }

    async fn read(&self, region: &Region) -> Result<NdArrowArray> {
        region.check(self.name(), self.dimensions())?;
        self.reads.fetch_add(1, Ordering::Relaxed);
        if region.rank() == 0 {
            return Ok(self.array.clone());
        }
        let selections: Vec<(&str, Selector)> = self
            .dimensions()
            .iter()
            .zip(region.start.iter().zip(&region.shape))
            .map(|(dimension, (&start, &length))| {
                let slice = Slice::range(start as isize, (start + length) as isize);
                (dimension.name(), Selector::Slice(slice))
            })
            .collect();
        Ok(isel(&self.array, &selections)?)
    }
}

/// A dataset of memory variables. A clone shares the variables and their read counts.
#[derive(Debug, Clone)]
pub struct MemoryDataset {
    schema: DatasetSchema,
    variables: IndexMap<Arc<str>, Arc<MemoryVariable>>,
}

impl MemoryDataset {
    /// The dataset dimensions are the union of the variable dimensions, in first-seen order.
    pub fn try_new(name: impl Into<String>, variables: Vec<MemoryVariable>) -> Result<Self> {
        let variables: Vec<Arc<MemoryVariable>> = variables.into_iter().map(Arc::new).collect();
        let mut union: Vec<Dimension> = Vec::new();
        for dimension in variables.iter().flat_map(|v| v.dimensions().iter()) {
            // Keep the first size. The schema check rejects a second size.
            if !union.iter().any(|d| d.name() == dimension.name()) {
                union.push(dimension.clone());
            }
        }
        let schema = DatasetSchema::try_new(
            name,
            Dimensions::try_new(union)?,
            variables
                .iter()
                .map(|v| v.clone() as Arc<dyn NdVariable>)
                .collect(),
        )?;
        let variables = variables
            .into_iter()
            .map(|v| (Arc::from(v.name()), v))
            .collect();
        Ok(Self { schema, variables })
    }

    /// Set the global attributes.
    pub fn with_attributes(mut self, attributes: Attributes) -> Self {
        self.schema = self.schema.with_attributes(attributes);
        self
    }

    /// Set the dimensions that the reader gave a name.
    pub fn with_invented_dimensions(mut self, names: Vec<Arc<str>>) -> Self {
        self.schema = self.schema.with_invented_dimensions(names);
        self
    }

    /// The count of `read` calls on one variable. 0 for an unknown name.
    pub fn reads(&self, variable: &str) -> usize {
        self.variables.get(variable).map_or(0, |v| v.reads())
    }
}

impl NdDataset for MemoryDataset {
    fn name(&self) -> &str {
        self.schema.name()
    }

    fn dimensions(&self) -> &Dimensions {
        self.schema.dimensions()
    }

    fn attributes(&self) -> &Attributes {
        self.schema.attributes()
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        self.schema.variable_names()
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        self.schema.variable(name)
    }

    fn invented_dimensions(&self) -> &[Arc<str>] {
        self.schema.invented_dimensions()
    }
}

/// Helpers for the tests of this crate.
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use arrow::datatypes::DataType;
    use async_trait::async_trait;
    use nd_arrow_array::{Attributes, Dimension, Dimensions, NdArrowArray};

    use super::{MemoryDataset, MemoryVariable};
    use crate::{NdVariable, Region, Result, VariableInfo};

    /// Dimensions with these names and sizes, in this order.
    pub(crate) fn dims(sizes: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(sizes.iter().map(|(n, s)| Dimension::new(*n, *s))).unwrap()
    }

    /// A Float64 variable whose values are 0.0, 1.0, 2.0, .. in C order.
    pub(crate) fn ramp(name: &str, dims: &[(&str, usize)]) -> MemoryVariable {
        let dimensions = self::dims(dims);
        let values =
            Float64Array::from_iter_values((0..dimensions.num_elements()).map(|i| i as f64));
        MemoryVariable::new(
            name,
            NdArrowArray::try_new(Arc::new(values), dimensions).unwrap(),
        )
    }

    /// `MemoryDataset::try_new("test.nd", variables).unwrap()`.
    pub(crate) fn dataset(variables: Vec<MemoryVariable>) -> MemoryDataset {
        MemoryDataset::try_new("test.nd", variables).unwrap()
    }

    /// A variable with metadata and no data. `read` panics.
    #[derive(Debug)]
    pub(crate) struct Meta(pub VariableInfo);

    #[async_trait]
    impl NdVariable for Meta {
        fn name(&self) -> &str {
            &self.0.name
        }
        fn data_type(&self) -> &DataType {
            &self.0.data_type
        }
        fn dimensions(&self) -> &Dimensions {
            &self.0.dimensions
        }
        fn chunk_shape(&self) -> Option<&[usize]> {
            self.0.chunk_shape.as_deref()
        }
        fn attributes(&self) -> &Attributes {
            &self.0.attributes
        }
        async fn read(&self, _: &Region) -> Result<NdArrowArray> {
            unimplemented!()
        }
    }

    /// A metadata-only variable with these dimensions.
    pub(crate) fn meta(name: &str, data_type: DataType, dims: &[(&str, usize)]) -> Meta {
        Meta(VariableInfo::new(name, data_type, self::dims(dims)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    use super::testing::{dataset, ramp};
    use super::*;
    use crate::{Error, NdDataset, NdVariable, Region, validate};

    #[tokio::test]
    async fn a_read_returns_the_region_in_c_order() {
        let temp = ramp("temp", &[("time", 2), ("lat", 3)]);
        let part = temp
            .read(&Region::new(vec![1, 1], vec![1, 2]))
            .await
            .unwrap();
        assert_eq!(part.shape(), vec![1, 2]);
        assert_eq!(part.dims().names(), vec!["time", "lat"]);
        let values = part
            .values()
            .as_primitive::<Float64Type>()
            .values()
            .to_vec();
        assert_eq!(values, vec![4.0, 5.0]);
        assert_eq!(temp.reads(), 1);
    }

    #[tokio::test]
    async fn a_rank_0_read_returns_the_one_value() {
        let scalar = ramp("title", &[]);
        let value = scalar.read(&Region::new(vec![], vec![])).await.unwrap();
        assert_eq!(value.len(), 1);
    }

    #[tokio::test]
    async fn a_region_outside_the_variable_is_an_error_and_is_not_a_read() {
        let lat = ramp("lat", &[("lat", 3)]);
        let error = lat.read(&Region::new(vec![2], vec![2])).await.unwrap_err();
        assert!(matches!(error, Error::InvalidRegion { .. }));
        assert_eq!(lat.reads(), 0);
    }

    #[tokio::test]
    async fn a_dataset_takes_its_dimensions_from_its_variables() {
        let ds = dataset(vec![
            ramp("temp", &[("time", 2), ("lat", 3)]),
            ramp("lon", &[("lon", 4)]),
        ]);
        assert_eq!(ds.dimensions().names(), vec!["time", "lat", "lon"]);
        assert_eq!(ds.dimensions().shape(), vec![2, 3, 4]);
        validate(&ds).unwrap();
        assert_eq!(ds.reads("temp"), 0);
    }

    #[test]
    fn two_sizes_for_one_dimension_are_invalid() {
        let result =
            MemoryDataset::try_new("x", vec![ramp("a", &[("n", 2)]), ramp("b", &[("n", 3)])]);
        assert!(matches!(result, Err(Error::Invalid { .. })));
    }

    #[tokio::test]
    async fn a_read_of_an_empty_region_returns_no_values() {
        let temp = ramp("temp", &[("time", 2), ("lat", 0)]);
        let part = temp
            .read(&Region::new(vec![1, 0], vec![1, 0]))
            .await
            .unwrap();
        assert_eq!(part.shape(), vec![1, 0]);
        assert_eq!(part.dims().names(), vec!["time", "lat"]);
        assert!(part.is_empty());
        assert_eq!(temp.reads(), 1);
    }

    #[tokio::test]
    async fn a_clone_shares_the_read_counts() {
        let ds = dataset(vec![ramp("lat", &[("lat", 3)])]);
        let clone = ds.clone();
        let lat = clone.variable("lat").unwrap();
        lat.read(&Region::new(vec![0], vec![3])).await.unwrap();
        assert_eq!(ds.reads("lat"), 1);
        assert_eq!(ds.reads("x"), 0);
    }
}
