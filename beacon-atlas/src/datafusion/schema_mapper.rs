use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::ColumnStatistics;
use datafusion::datasource::schema_adapter::SchemaMapper;

/// A [`SchemaMapper`] for atlas datasets.
///
/// Constructed with just the projected output schema. On each call to
/// [`map_batch`](SchemaMapper::map_batch), inspects the incoming batch's schema
/// and maps it to the output schema by:
///
/// 1. Re-ordering columns to match the output schema
/// 2. Casting columns whose types differ from the output schema
/// 3. Filling missing columns with null arrays
///
/// This avoids needing a separate adapter/mapper per dataset schema — a single
/// instance handles any input schema as long as the output schema is fixed.
pub struct AtlasSchemaMapper {
    /// The projected output schema.
    output_schema: SchemaRef,
}

impl Debug for AtlasSchemaMapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasSchemaMapper")
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl AtlasSchemaMapper {
    /// Create a new mapper targeting the given projected output schema.
    pub fn new(output_schema: SchemaRef) -> Self {
        Self { output_schema }
    }

    /// The output schema this mapper produces.
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

impl SchemaMapper for AtlasSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let input_schema = batch.schema();

        let mapped = self
            .output_schema
            .fields()
            .iter()
            .map(|target_field| {
                let name = target_field.name();
                let target_dt = target_field.data_type();

                match input_schema.column_with_name(name) {
                    Some((idx, input_field)) => {
                        let col = batch.column(idx);
                        if input_field.data_type() == target_dt {
                            Ok(col.clone())
                        } else {
                            cast(col, target_dt).map_err(|e| {
                                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                            })
                        }
                    }
                    None => Ok(new_null_array(target_dt, num_rows)),
                }
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let batch =
            RecordBatch::try_new_with_options(self.output_schema.clone(), mapped, &options)?;
        Ok(batch)
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> datafusion::common::Result<Vec<ColumnStatistics>> {
        Ok(vec![
            ColumnStatistics::new_unknown();
            self.output_schema.fields().len()
        ])
    }
}

/// Returns the column names from `input_schema` that are needed to satisfy
/// the given `output_schema`.
///
/// Use this to build a projection when reading a dataset so only the required
/// columns are loaded from storage.
pub fn required_columns(
    output_schema: &SchemaRef,
    input_schema: &arrow::datatypes::Schema,
) -> Vec<String> {
    output_schema
        .fields()
        .iter()
        .filter_map(|field| {
            input_schema
                .column_with_name(field.name())
                .map(|(_, f)| f.name().clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn output_schema(fields: Vec<(&str, DataType)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(n, dt)| Field::new(n, dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_direct_mapping_same_schema() {
        let schema = output_schema(vec![("a", DataType::Int32), ("b", DataType::Float64)]);
        let mapper = AtlasSchemaMapper::new(schema.clone());

        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let result = mapper.map_batch(input).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), schema);
    }

    #[test]
    fn test_null_fill_missing_column() {
        let schema = output_schema(vec![
            ("a", DataType::Int32),
            ("b", DataType::Float64),
            ("c", DataType::Utf8),
        ]);
        let mapper = AtlasSchemaMapper::new(schema);

        let input_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        )
        .unwrap();

        let result = mapper.map_batch(input).unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.column(1).null_count(), 2);
        assert_eq!(result.column(2).null_count(), 2);
    }

    #[test]
    fn test_cast_column_type() {
        let schema = output_schema(vec![("x", DataType::Float64)]);
        let mapper = AtlasSchemaMapper::new(schema);

        let input_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, true)]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(Float32Array::from(vec![1.5, 2.5]))],
        )
        .unwrap();

        let result = mapper.map_batch(input).unwrap();
        assert_eq!(*result.schema().field(0).data_type(), DataType::Float64);

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(vals.value(0), 1.5);
        assert_eq!(vals.value(1), 2.5);
    }

    #[test]
    fn test_reorder_columns() {
        let schema = output_schema(vec![("a", DataType::Utf8), ("b", DataType::Int32)]);
        let mapper = AtlasSchemaMapper::new(schema);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Utf8, true),
        ]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![100, 200])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let result = mapper.map_batch(input).unwrap();
        assert_eq!(result.schema().field(0).name(), "a");
        assert_eq!(result.schema().field(1).name(), "b");

        let a_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a_col.value(0), "hello");

        let b_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(b_col.value(0), 100);
    }

    #[test]
    fn test_required_columns() {
        let schema = output_schema(vec![
            ("a", DataType::Int32),
            ("b", DataType::Float64),
            ("c", DataType::Utf8),
        ]);
        let input_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Int32, true),
        ]);

        let required = required_columns(&schema, &input_schema);
        assert_eq!(required, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_empty_batch() {
        let schema = output_schema(vec![("a", DataType::Int32), ("b", DataType::Float64)]);
        let mapper = AtlasSchemaMapper::new(schema);

        let input_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .unwrap();

        let result = mapper.map_batch(input).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 2);
    }
}
