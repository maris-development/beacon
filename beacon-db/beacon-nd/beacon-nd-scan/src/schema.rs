//! The Arrow schema of one file, and the column rules of the scan schema.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use beacon_nd_dataset::NdDataset;
use beacon_nd_encoding::encoding::nd_value_type;

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

/// The names of the projected columns, in projected order.
pub(crate) fn file_columns(projected_schema: &Schema) -> Vec<String> {
    projected_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// The logical schema of the projected columns: each `beacon.nd` field with its value type.
///
/// The filter of the scan sees these types.
pub(crate) fn table_schema(projected_schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = projected_schema
        .fields()
        .iter()
        .filter_map(|field| {
            let value_type = nd_value_type(field.data_type()).ok()?;
            Some(Field::new(field.name(), value_type, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use beacon_nd_encoding::encoding::encoded_schema;

    use super::{file_columns, file_schema};
    use crate::testing::grid_dataset;

    /// The encoded schema of the Float64 columns `columns`.
    fn projected(columns: &[&str]) -> SchemaRef {
        let logical = Schema::new(
            columns
                .iter()
                .map(|name| Field::new(*name, DataType::Float64, true))
                .collect::<Vec<_>>(),
        );
        Arc::new(encoded_schema(&logical))
    }

    #[test]
    fn a_file_is_projected_to_the_wanted_columns() {
        let schema = projected(&["lat", "temp"]);

        assert_eq!(file_columns(&schema), ["lat", "temp"]);
    }

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
