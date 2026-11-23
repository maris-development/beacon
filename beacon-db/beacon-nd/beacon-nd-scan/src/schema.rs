//! The Arrow schema of one file, and the column rules of the scan schema.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use beacon_nd_dataset::NdDataset;
use beacon_nd_encoding::encoding::nd_value_type;
use datafusion::common::plan_datafusion_err;
use datafusion::error::Result;

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

/// The logical form of an encoded schema: each `beacon.nd` field with its value type.
///
/// The filter of the scan sees these types. A field that is not encoded gives a plan error.
pub(crate) fn logical_projection(encoded: &Schema) -> Result<SchemaRef> {
    let fields = encoded
        .fields()
        .iter()
        .map(|field| {
            let value_type = nd_value_type(field.data_type()).map_err(|error| {
                plan_datafusion_err!(
                    "the column '{}' of the ND scan is not beacon.nd encoded: {error}",
                    field.name()
                )
            })?;
            Ok(Field::new(field.name(), value_type, true))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_nd_encoding::encoding::encoded_schema;
    use datafusion::error::DataFusionError;

    use super::{file_schema, logical_projection};
    use crate::tests::fixtures::grid_dataset;

    #[test]
    fn the_logical_projection_has_the_value_type_of_each_encoded_field() {
        let logical = Schema::new(vec![
            Field::new("lat", DataType::Float64, false),
            Field::new("n", DataType::Int32, true),
        ]);

        let projection = logical_projection(&encoded_schema(&logical)).unwrap();

        let fields: Vec<(&str, &DataType, bool)> = projection
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable(),
                )
            })
            .collect();
        assert_eq!(
            fields,
            [
                ("lat", &DataType::Float64, true),
                ("n", &DataType::Int32, true)
            ]
        );
    }

    #[test]
    fn a_field_that_is_not_encoded_is_a_plan_error() {
        let plain = Schema::new(vec![Field::new("lat", DataType::Float64, true)]);

        let error = logical_projection(&plain).unwrap_err();

        assert!(
            matches!(error, DataFusionError::Plan(ref text) if text.contains("'lat'")),
            "{error}"
        );
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
