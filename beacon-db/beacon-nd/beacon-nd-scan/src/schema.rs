//! The Arrow schema of one file.

use arrow::datatypes::{Field, Schema};
use beacon_nd_dataset::NdDataset;

/// The Arrow schema of one file: one nullable field per variable, sorted by name.
///
/// The field has the type that the variable reads. The sort is here, not in the readers.
pub fn file_schema(dataset: &dyn NdDataset) -> Schema {
    let mut names = dataset.variable_names();
    names.sort();
    let fields: Vec<Field> = names
        .iter()
        .filter_map(|name| dataset.variable(name))
        .map(|variable| Field::new(variable.name(), variable.data_type().clone(), true))
        .collect();
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::file_schema;
    use crate::testing::grid_dataset;

    #[test]
    fn the_schema_of_a_file_is_sorted_by_name() {
        let schema = file_schema(&grid_dataset("a.nd"));

        let names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["lat", "lon", "temp"]);
        for field in schema.fields() {
            assert_eq!(field.data_type(), &DataType::Float64, "{}", field.name());
            assert!(field.is_nullable(), "{}", field.name());
        }
    }
}
