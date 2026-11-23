//! The Arrow schema of one file, and the column rules of the scan schema.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
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

/// True when `name` is one of `partition_fields`.
pub(crate) fn is_partition(partition_fields: &[FieldRef], name: &str) -> bool {
    partition_fields.iter().any(|field| field.name() == name)
}

/// The projected fields that are file columns, in projected order.
pub(crate) fn file_fields<'a>(
    projected_schema: &'a Schema,
    partition_fields: &'a [FieldRef],
) -> impl Iterator<Item = &'a FieldRef> {
    projected_schema
        .fields()
        .iter()
        .filter(|field| !is_partition(partition_fields, field.name()))
}

/// The names of the projected columns that are file columns, in projected order.
pub(crate) fn file_columns(
    projected_schema: &Schema,
    partition_fields: &[FieldRef],
) -> Vec<String> {
    file_fields(projected_schema, partition_fields)
        .map(|field| field.name().clone())
        .collect()
}

/// The logical table type of each `beacon.nd` encoded field of `encoded`.
pub(crate) fn table_types(encoded: &Schema) -> HashMap<String, DataType> {
    encoded
        .fields()
        .iter()
        .filter_map(|field| {
            let value_type = nd_value_type(field.data_type()).ok()?;
            Some((field.name().clone(), value_type))
        })
        .collect()
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

        assert_eq!(file_columns(&schema, &[]), ["lat", "temp"]);
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
