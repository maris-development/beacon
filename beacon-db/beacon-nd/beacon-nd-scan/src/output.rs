//! The output step: one chunk of one file becomes one batch of the projected schema.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use beacon_datafusion_ext::scan_adapt::BatchAdapter;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_nd_dataset::NdDataset;
use beacon_nd_dataset::{
    Dimensions as DatasetDimensions, NdArrowArray as DatasetArray, NdRecordBatch as DatasetBatch,
};
use beacon_nd_encoding::encoding::{
    encode_nd_array, encode_nd_record_batch, encoded_schema, nd_value_type,
};
use beacon_nd_encoding::{
    Dimension as EncodedDimension, Dimensions as EncodedDimensions, NdArrowArray as EncodedArray,
    NdRecordBatch as EncodedBatch,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;

use crate::schema::{file_fields, is_partition};

/// The axis of a partition column in a chunk with no file column.
const ROW_AXIS: &str = "row";

/// How the chunks of one file become batches of the scan.
#[derive(Debug)]
pub(crate) struct FileOutput {
    /// Reorders, casts and null-fills the file columns. `None` when no file column is projected.
    adapter: Option<BatchAdapter>,
    /// The encoded partition fields of the table.
    partition_fields: Vec<FieldRef>,
    /// The partition values of this file, in the order of `partition_fields`.
    partition_values: Vec<ScalarValue>,
    /// The rank-0 partition columns, one for each projected partition field, in projected order.
    scalar_partitions: Vec<ArrayRef>,
    /// The encoded schema that the scan emits.
    projected_schema: SchemaRef,
}

impl FileOutput {
    /// Make the output of one file.
    ///
    /// `projected_schema` is the encoded schema that the scan emits.
    /// `partition_fields` are the encoded partition fields of the table, with the values of this file.
    /// The dataset can hold more variables than the projection names. The output drops them.
    pub(crate) fn try_new(
        dataset: &dyn NdDataset,
        projected_schema: &SchemaRef,
        partition_fields: &[FieldRef],
        partition_values: &[ScalarValue],
    ) -> Result<Self> {
        let target_fields: Vec<FieldRef> = file_fields(projected_schema, partition_fields)
            .cloned()
            .collect();
        let adapter = if target_fields.is_empty() {
            None
        } else {
            let target = Arc::new(Schema::new_with_metadata(
                target_fields,
                projected_schema.metadata().clone(),
            ));
            let file_schema = Schema::new(
                dataset
                    .variable_names()
                    .iter()
                    .map(|name| {
                        let variable = dataset.variable(name).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "the dataset '{}' has no variable '{name}'",
                                dataset.name()
                            ))
                        })?;
                        Ok(Field::new(&**name, variable.data_type().clone(), true))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            // Every column is nd-encoded and casts leniently, so the strict rule is never asked.
            Some(BatchAdapter::try_new(
                target,
                &encoded_schema(&file_schema),
                &DefaultArrowTypeWidening::new(),
            )?)
        };

        let mut output = Self {
            adapter,
            partition_fields: partition_fields.to_vec(),
            partition_values: partition_values.to_vec(),
            scalar_partitions: Vec::new(),
            projected_schema: Arc::clone(projected_schema),
        };
        // The rank-0 columns are the same for each chunk of the file.
        output.scalar_partitions = output
            .projected_partitions()
            .map(|field| output.partition_column(field, None))
            .collect::<Result<_>>()?;
        Ok(output)
    }

    /// Encode one chunk, and adapt it to the projected schema.
    pub(crate) fn finish(&self, batch: &DatasetBatch) -> Result<RecordBatch> {
        let Some(adapter) = &self.adapter else {
            return self.finish_without_file_columns(batch.num_rows());
        };

        let columns = batch
            .columns()
            .iter()
            .map(to_encoded_array)
            .collect::<Result<Vec<_>>>()?;
        let nd = EncodedBatch::try_new(
            Arc::clone(batch.schema()),
            columns,
            to_encoded_dimensions(batch.target())?,
        )?;
        let adapted = adapter.adapt(&encode_nd_record_batch(&nd)?).map_err(|e| {
            DataFusionError::Execution(format!("failed to adapt the batch to the scan schema: {e}"))
        })?;

        // Put each partition column back at its projected place.
        let mut file_columns = adapted.columns().iter();
        let mut partitions = self.scalar_partitions.iter();
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| {
                let next = if self.is_partition(field.name()) {
                    partitions.next()
                } else {
                    file_columns.next()
                };
                next.cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "the scan output has no column for '{}'",
                        field.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.batch(columns, 1)
    }

    /// The batch of a chunk with no file column.
    fn finish_without_file_columns(&self, rows: usize) -> Result<RecordBatch> {
        if self.scalar_partitions.is_empty() {
            // A row carrier: the batch states the rows of the chunk and has no column.
            return self.batch(Vec::new(), rows);
        }
        // Rank-0 columns alone hold one row, so the rows of the chunk become an axis.
        let columns = self
            .projected_partitions()
            .map(|field| self.partition_column(field, Some(rows)))
            .collect::<Result<Vec<_>>>()?;
        self.batch(columns, 1)
    }

    /// A batch of the projected schema with `rows` rows.
    fn batch(&self, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
        RecordBatch::try_new_with_options(
            Arc::clone(&self.projected_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// The projected fields that are partition fields, in projected order.
    fn projected_partitions(&self) -> impl Iterator<Item = &FieldRef> {
        self.projected_schema
            .fields()
            .iter()
            .filter(|field| self.is_partition(field.name()))
    }

    fn is_partition(&self, name: &str) -> bool {
        is_partition(&self.partition_fields, name)
    }

    /// One partition column, nd-encoded: rank 0, or on the row axis of `rows` cells.
    fn partition_column(&self, field: &FieldRef, rows: Option<usize>) -> Result<ArrayRef> {
        let value_type = nd_value_type(field.data_type())?;
        let index = self
            .partition_fields
            .iter()
            .position(|declared| declared.name() == field.name())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "'{}' is not a partition column of this scan",
                    field.name()
                ))
            })?;
        let value = self
            .partition_values
            .get(index)
            .cloned()
            .unwrap_or(ScalarValue::Null);

        let values = value.to_array_of_size(rows.unwrap_or(1)).map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to build the partition column '{}': {e}",
                field.name()
            ))
        })?;
        // The table declares the type, so a path value of another type is cast to it.
        let values = if values.data_type() == &value_type {
            values
        } else {
            arrow::compute::cast(values.as_ref(), &value_type)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };

        let dims = match rows {
            None => EncodedDimensions::scalar(),
            Some(rows) => EncodedDimensions::try_new(vec![EncodedDimension::new(ROW_AXIS, rows)])?,
        };
        Ok(encode_nd_array(&EncodedArray::try_new(values, dims)?))
    }
}

/// The same column as an encoding array. The values are shared, not copied.
fn to_encoded_array(column: &DatasetArray) -> Result<EncodedArray> {
    EncodedArray::try_new(
        Arc::clone(column.values()),
        to_encoded_dimensions(column.dims())?,
    )
}

fn to_encoded_dimensions(dims: &DatasetDimensions) -> Result<EncodedDimensions> {
    EncodedDimensions::try_new(
        dims.iter()
            .map(|dim| EncodedDimension::new(dim.name(), dim.size()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, AsArray, Float32Array};
    use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int32Type, Schema, SchemaRef};
    use beacon_nd_dataset::{
        ChunkGrid, Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray, NdDataset,
        read_chunk, select_columns,
    };
    use beacon_nd_encoding::encoding::{decode_nd_array, decode_nd_record_batch, encoded_schema};
    use datafusion::scalar::ScalarValue;

    use super::FileOutput;
    use crate::testing::grid_dataset;

    /// The encoded schema of logical `fields`.
    fn projected(fields: &[(&str, DataType)]) -> SchemaRef {
        let logical = Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        );
        Arc::new(encoded_schema(&logical))
    }

    /// The encoded partition field `year: Int32`.
    fn year() -> FieldRef {
        projected(&[("year", DataType::Int32)])
            .field(0)
            .clone()
            .into()
    }

    /// Chunk `k` of `dataset` at a batch size of 12.
    async fn chunk(dataset: &dyn NdDataset, k: usize) -> beacon_nd_dataset::NdRecordBatch {
        let grid = ChunkGrid::try_new(dataset, 12).unwrap();
        read_chunk(dataset, &grid, &grid.chunks()[k]).await.unwrap()
    }

    /// The grid fixture with no variable: each chunk has 12 rows and no column.
    fn count_dataset() -> Arc<dyn NdDataset> {
        select_columns(Arc::new(grid_dataset("a.nd")), &[])
    }

    #[tokio::test]
    async fn a_batch_gets_the_projected_order_and_nulls_for_a_missing_column() {
        let schema = projected(&[
            ("lat", DataType::Float64),
            ("salt", DataType::Float64),
            ("temp", DataType::Float64),
        ]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["lat", "temp"]);

        let output = FileOutput::try_new(dataset.as_ref(), &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), schema);
        assert!(batch.column(1).is_null(0), "salt is null");
        let temp = decode_nd_array(batch.column(2), 0).unwrap();
        assert_eq!(temp.values().len(), 12);
        let lat = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(lat.values().len(), 4);
    }

    #[tokio::test]
    async fn a_batch_decodes_to_the_values_of_the_chunk() {
        let schema = projected(&[("temp", DataType::Float64)]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["temp"]);

        let output = FileOutput::try_new(dataset.as_ref(), &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 3).await).unwrap();

        let decoded = decode_nd_record_batch(&batch).unwrap();
        assert_eq!(
            decoded
                .column(0)
                .values()
                .as_primitive::<Float64Type>()
                .values(),
            &[
                27.0, 28.0, 29.0, 33.0, 34.0, 35.0, 39.0, 40.0, 41.0, 45.0, 46.0, 47.0
            ]
        );
    }

    #[tokio::test]
    async fn a_dataset_with_more_variables_than_the_projection_is_adapted() {
        let schema = projected(&[("temp", DataType::Float64)]);
        let dataset = grid_dataset("a.nd");

        let output = FileOutput::try_new(&dataset, &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(&dataset, 0).await).unwrap();

        assert_eq!(batch.schema(), schema);
        let temp = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(temp.values().len(), 12);
    }

    #[tokio::test]
    async fn a_variable_after_unprojected_variables_is_adapted() {
        // `lon` is the third variable of the dataset and the only projected column.
        let schema = projected(&[("lon", DataType::Float64)]);
        let dataset = grid_dataset("a.nd");

        let output = FileOutput::try_new(&dataset, &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(&dataset, 0).await).unwrap();

        assert_eq!(batch.schema(), schema);
        let lon = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(
            lon.values().as_primitive::<Float64Type>().values(),
            &[0.0, 1.0, 2.0]
        );
    }

    #[tokio::test]
    async fn a_partition_column_before_a_file_column_keeps_its_place() {
        let schema = projected(&[("year", DataType::Int32), ("temp", DataType::Float64)]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["temp"]);

        let output = FileOutput::try_new(
            dataset.as_ref(),
            &schema,
            &[year()],
            &[ScalarValue::Int32(Some(2024))],
        )
        .unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.schema(), schema);
        let year = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(year.values().as_primitive::<Int32Type>().values(), &[2024]);
        let temp = decode_nd_array(batch.column(1), 0).unwrap();
        assert_eq!(temp.values().len(), 12);
    }

    #[tokio::test]
    async fn a_missing_partition_value_is_null() {
        let schema = projected(&[("temp", DataType::Float64), ("year", DataType::Int32)]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["temp"]);

        let output = FileOutput::try_new(dataset.as_ref(), &schema, &[year()], &[]).unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        let year = decode_nd_array(batch.column(1), 0).unwrap();
        assert_eq!(year.data_type(), &DataType::Int32);
        assert!(year.values().is_null(0), "the value is null");
    }

    #[tokio::test]
    async fn a_partition_column_is_a_scalar_in_each_batch() {
        let schema = projected(&[("temp", DataType::Float64), ("year", DataType::Int32)]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["temp"]);

        let output = FileOutput::try_new(
            dataset.as_ref(),
            &schema,
            &[year()],
            &[ScalarValue::Int32(Some(2024))],
        )
        .unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), schema);
        let year = decode_nd_array(batch.column(1), 0).unwrap();
        assert_eq!(year.dims().rank(), 0);
        assert_eq!(year.values().as_primitive::<Int32Type>().values(), &[2024]);
    }

    #[tokio::test]
    async fn a_count_chunk_is_a_row_carrier() {
        let schema = Arc::new(Schema::empty());
        let dataset = count_dataset();

        let output = FileOutput::try_new(dataset.as_ref(), &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 12);
    }

    #[tokio::test]
    async fn a_partition_only_chunk_lies_on_a_row_axis() {
        let schema = projected(&[("year", DataType::Int32)]);
        let dataset = count_dataset();

        let output = FileOutput::try_new(
            dataset.as_ref(),
            &schema,
            &[year()],
            &[ScalarValue::Int32(Some(2024))],
        )
        .unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let year = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(
            year.dims(),
            &beacon_nd_encoding::Dimensions::try_new(vec![beacon_nd_encoding::Dimension::new(
                "row", 12
            )])
            .unwrap()
        );
        assert_eq!(
            year.values().as_primitive::<Int32Type>().values(),
            &[2024; 12]
        );
    }

    #[tokio::test]
    async fn a_column_of_another_type_is_cast_to_the_table_type() {
        let x = Dimensions::try_new([Dimension::new("x", 3)]).unwrap();
        let values = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]));
        let temp = MemoryVariable::new("temp", NdArrowArray::try_new(values, x).unwrap());
        let dataset = MemoryDataset::try_new("a.nd", vec![temp]).unwrap();
        let schema = projected(&[("temp", DataType::Float64)]);

        let output = FileOutput::try_new(&dataset, &schema, &[], &[]).unwrap();
        let batch = output.finish(&chunk(&dataset, 0).await).unwrap();

        let temp = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(temp.data_type(), &DataType::Float64);
        assert_eq!(
            temp.values().as_primitive::<Float64Type>().values(),
            &[1.0, 2.0, 3.0]
        );
    }

    #[tokio::test]
    async fn a_partition_value_of_the_wrong_type_is_cast() {
        let schema = projected(&[("temp", DataType::Float64), ("year", DataType::Int32)]);
        let dataset = select_columns(Arc::new(grid_dataset("a.nd")), &["temp"]);

        let output = FileOutput::try_new(
            dataset.as_ref(),
            &schema,
            &[year()],
            &[ScalarValue::Utf8(Some("2024".to_string()))],
        )
        .unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        let year = decode_nd_array(batch.column(1), 0).unwrap();
        assert_eq!(year.values().as_primitive::<Int32Type>().values(), &[2024]);
    }
}
