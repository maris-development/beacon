//! Ready metadata for a simple format.

use std::sync::Arc;

use arrow::datatypes::DataType;
use indexmap::IndexMap;
use indexmap::map::Entry;
use nd_arrow_array::{Attributes, Dimensions};

use crate::dataset::not_unique;
use crate::{Error, NdDataset, NdVariable, Result, validate};

/// The metadata of one variable. A format variable forwards to it.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableInfo {
    /// The name of the variable.
    pub name: Arc<str>,
    /// The Arrow type that `read` returns.
    pub data_type: DataType,
    /// The dimensions, in C order, with their sizes.
    pub dimensions: Dimensions,
    /// The native chunk shape. `None` for a variable without chunks.
    pub chunk_shape: Option<Vec<usize>>,
    /// The attributes of the variable.
    pub attributes: Attributes,
}

impl VariableInfo {
    /// A variable without chunks and without attributes.
    pub fn new(name: impl Into<Arc<str>>, data_type: DataType, dimensions: Dimensions) -> Self {
        Self {
            name: name.into(),
            data_type,
            dimensions,
            chunk_shape: None,
            attributes: Attributes::new(),
        }
    }

    /// Set the native chunk shape.
    pub fn with_chunk_shape(mut self, shape: Vec<usize>) -> Self {
        self.chunk_shape = Some(shape);
        self
    }

    /// Set the attributes.
    pub fn with_attributes(mut self, attributes: Attributes) -> Self {
        self.attributes = attributes;
        self
    }
}

/// The metadata of one dataset. A format dataset forwards to it.
#[derive(Debug, Clone)]
pub struct DatasetSchema {
    name: String,
    dimensions: Dimensions,
    attributes: Attributes,
    invented_dimensions: Vec<Arc<str>>,
    variables: IndexMap<Arc<str>, Arc<dyn NdVariable>>,
}

impl DatasetSchema {
    /// A schema with the variables in the order of `variables`.
    ///
    /// Runs the checks of `validate`.
    pub fn try_new(
        name: impl Into<String>,
        dimensions: Dimensions,
        variables: Vec<Arc<dyn NdVariable>>,
    ) -> Result<Self> {
        let name = name.into();
        let mut map = IndexMap::with_capacity(variables.len());
        for variable in variables {
            // The map keeps one entry for each name, so find a duplicate here.
            match map.entry(Arc::<str>::from(variable.name())) {
                Entry::Occupied(entry) => {
                    return Err(Error::Invalid {
                        dataset: name,
                        reason: not_unique(entry.key()),
                    });
                }
                Entry::Vacant(entry) => {
                    entry.insert(variable);
                }
            }
        }
        let schema = Self {
            name,
            dimensions,
            attributes: Attributes::new(),
            invented_dimensions: Vec::new(),
            variables: map,
        };
        validate(&schema)?;
        Ok(schema)
    }

    /// Set the global attributes.
    pub fn with_attributes(mut self, attributes: Attributes) -> Self {
        self.attributes = attributes;
        self
    }

    /// Set the dimensions that the reader gave a name.
    pub fn with_invented_dimensions(mut self, names: Vec<Arc<str>>) -> Self {
        self.invented_dimensions = names;
        self
    }
}

impl NdDataset for DatasetSchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    fn attributes(&self) -> &Attributes {
        &self.attributes
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        self.variables.keys().cloned().collect()
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        self.variables.get(name).cloned()
    }

    fn invented_dimensions(&self) -> &[Arc<str>] {
        &self.invented_dimensions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::testing::{Meta, dims, meta};

    fn var(name: &str, sizes: &[(&str, usize)]) -> Arc<dyn NdVariable> {
        Arc::new(meta(name, DataType::Float64, sizes))
    }

    fn assert_invalid(result: Result<DatasetSchema>) {
        let error = result.unwrap_err();
        assert!(matches!(error, Error::Invalid { ref dataset, .. } if dataset == "ds"));
    }

    #[test]
    fn a_valid_schema_lists_its_variables_in_order() {
        let schema = DatasetSchema::try_new(
            "ds",
            dims(&[("time", 2), ("lat", 3)]),
            vec![
                var("temp", &[("time", 2), ("lat", 3)]),
                var("lat", &[("lat", 3)]),
            ],
        )
        .unwrap();
        let names = schema.variable_names();
        assert_eq!(names, [Arc::from("temp"), Arc::from("lat")]);
        assert!(schema.variable("lat").is_some());
        assert!(schema.variable("x").is_none());
        assert!(schema.invented_dimensions().is_empty());
    }

    #[test]
    fn a_size_conflict_is_invalid() {
        assert_invalid(DatasetSchema::try_new(
            "ds",
            dims(&[("lat", 3)]),
            vec![var("lat", &[("lat", 4)])],
        ));
    }

    #[test]
    fn an_unknown_dimension_is_invalid() {
        assert_invalid(DatasetSchema::try_new(
            "ds",
            dims(&[("lat", 3)]),
            vec![var("v", &[("depth", 2)])],
        ));
    }

    #[test]
    fn a_duplicate_name_is_invalid() {
        assert_invalid(DatasetSchema::try_new(
            "ds",
            dims(&[("lat", 3)]),
            vec![var("lat", &[("lat", 3)]), var("lat", &[("lat", 3)])],
        ));
    }

    #[test]
    fn a_chunk_shape_of_the_wrong_rank_is_invalid() {
        let temp = meta("temp", DataType::Float64, &[("time", 2), ("lat", 3)])
            .0
            .with_chunk_shape(vec![2]);
        assert_invalid(DatasetSchema::try_new(
            "ds",
            dims(&[("time", 2), ("lat", 3)]),
            vec![Arc::new(Meta(temp))],
        ));
    }

    /// A dataset whose `variable("a")` returns a variable named `b`.
    #[derive(Debug)]
    struct Renamed {
        dimensions: Dimensions,
        attributes: Attributes,
    }

    impl NdDataset for Renamed {
        fn name(&self) -> &str {
            "ds"
        }
        fn dimensions(&self) -> &Dimensions {
            &self.dimensions
        }
        fn attributes(&self) -> &Attributes {
            &self.attributes
        }
        fn variable_names(&self) -> Vec<Arc<str>> {
            vec![Arc::from("a")]
        }
        fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
            (name == "a").then(|| var("b", &[("lat", 3)]))
        }
    }

    #[test]
    fn validate_rejects_a_variable_under_the_wrong_name() {
        let dataset = Renamed {
            dimensions: dims(&[("lat", 3)]),
            attributes: Attributes::new(),
        };
        let error = validate(&dataset).unwrap_err();
        assert!(matches!(error, Error::Invalid { ref dataset, .. } if dataset == "ds"));
    }
}
