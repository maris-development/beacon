use indexmap::IndexMap;

use crate::NdArrayD;
use std::{collections::HashSet, sync::Arc};
pub mod gridded;
pub mod morsel;
pub mod ragged;

pub struct Dataset {
    pub name: String,
    pub dimensions: IndexMap<String, usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    pub invented_dimensions: HashSet<String>,
}

impl Dataset {
    /// Create a dataset whose dimensions come from its arrays.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the dataset, such as the file path
    /// * `arrays` - The arrays of the dataset, by name
    /// * `invented_dimensions` - The dimensions that the reader invented, because the file names none
    ///
    /// # Errors
    ///
    /// Returns an error when two arrays give one dimension different sizes.
    pub fn from_arrays(
        name: String,
        arrays: IndexMap<String, Arc<dyn NdArrayD>>,
        invented_dimensions: HashSet<String>,
    ) -> anyhow::Result<Dataset> {
        let mut dimensions = IndexMap::new();
        for (array_name, array) in &arrays {
            for (dimension, size) in array.dimensions().into_iter().zip(array.shape()) {
                if let Some(&held) = dimensions.get(&dimension)
                    && held != size
                {
                    anyhow::bail!(
                        "dataset '{name}': dimension '{dimension}' has size {held}, array '{array_name}' gives it {size}"
                    );
                }
                dimensions.insert(dimension, size);
            }
        }
        Ok(Dataset {
            name,
            dimensions,
            arrays,
            invented_dimensions,
        })
    }

    /// The Arrow schema of the dataset, with the fields sorted by name.
    ///
    /// Every field is nullable, because broadcast and ragged padding can add nulls.
    /// A row-size array of a ragged dataset and its attributes are not fields.
    pub fn arrow_schema(&self) -> arrow::datatypes::Schema {
        let row_size_arrays: Vec<&str> = self
            .arrays
            .keys()
            .filter_map(|key| key.strip_suffix(".sample_dimension"))
            .collect();
        let is_row_size_part = |name: &str| {
            row_size_arrays.iter().any(|row_size_name| {
                name == *row_size_name
                    || name
                        .strip_prefix(row_size_name)
                        .is_some_and(|rest| rest.starts_with('.'))
            })
        };

        let mut fields: Vec<arrow::datatypes::Field> = self
            .arrays
            .iter()
            .filter(|(name, _)| !is_row_size_part(name))
            .map(|(name, array)| arrow::datatypes::Field::new(name, array.datatype().into(), true))
            .collect();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        arrow::datatypes::Schema::new(fields)
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn from_arrays_collects_the_dimensions() {
        let arrays = IndexMap::from([
            ("temp".to_string(), array(&[2, 3], &["time", "lat"])),
            ("lat".to_string(), array(&[3], &["lat"])),
        ]);

        let dataset = Dataset::from_arrays("test".to_string(), arrays, HashSet::new()).unwrap();

        assert_eq!(dataset.dimensions["time"], 2);
        assert_eq!(dataset.dimensions["lat"], 3);
        assert_eq!(dataset.dimensions.len(), 2);
    }

    #[test]
    fn from_arrays_rejects_two_sizes_for_one_dimension() {
        let arrays = IndexMap::from([
            ("temp".to_string(), array(&[2, 3], &["time", "lat"])),
            ("lat".to_string(), array(&[5], &["lat"])),
        ]);

        let result = Dataset::from_arrays("test".to_string(), arrays, HashSet::new());

        assert!(result.is_err());
    }

    #[test]
    fn arrow_schema_is_sorted_nullable_and_skips_row_sizes() {
        let sample_dimension: Arc<dyn NdArrayD> = Arc::new(
            NdArray::<String>::try_new_from_vec_in_mem(vec!["z_obs".to_string()], vec![], vec![], None)
                .unwrap(),
        );
        let arrays = IndexMap::from([
            ("temp".to_string(), array(&[4], &["z_obs"])),
            ("lat".to_string(), array(&[2], &["casts"])),
            ("z_row_size".to_string(), array(&[2], &["casts"])),
            ("z_row_size.sample_dimension".to_string(), sample_dimension),
        ]);
        let dataset = Dataset::from_arrays("test".to_string(), arrays, HashSet::new()).unwrap();

        let schema = dataset.arrow_schema();

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["lat", "temp"]);
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
    }
}
