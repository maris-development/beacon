//! The output step: one chunk of one file becomes one batch of the projected schema.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use beacon_datafusion_ext::scan_adapt::BatchAdapter;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_nd_dataset::NdDataset;
use beacon_nd_dataset::{
    Dimensions as DatasetDimensions, NdArrowArray as DatasetArray, NdRecordBatch as DatasetBatch,
};
use beacon_nd_encoding::encoding::{encode_nd_record_batch, encoded_schema};
use beacon_nd_encoding::{
    Dimension as EncodedDimension, Dimensions as EncodedDimensions, NdArrowArray as EncodedArray,
    NdRecordBatch as EncodedBatch,
};
use datafusion::error::{DataFusionError, Result};

/// How the chunks of one file become batches of the scan.
#[derive(Debug)]
pub(crate) struct FileOutput {
    /// Reorders, casts and null-fills the file columns. `None` when no column is projected.
    adapter: Option<BatchAdapter>,
    /// The encoded schema that the scan emits.
    projected_schema: SchemaRef,
}

impl FileOutput {
    /// Make the output of one file.
    ///
    /// `projected_schema` is the encoded schema that the scan emits.
    /// The dataset can hold more variables than the projection names. The output drops them.
    pub(crate) fn try_new(dataset: &dyn NdDataset, projected_schema: &SchemaRef) -> Result<Self> {
        let adapter = if projected_schema.fields().is_empty() {
            None
        } else {
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
                Arc::clone(projected_schema),
                &encoded_schema(&file_schema),
                &DefaultArrowTypeWidening::new(),
            )?)
        };
        Ok(Self {
            adapter,
            projected_schema: Arc::clone(projected_schema),
        })
    }

    /// Encode one chunk, and adapt it to the projected schema.
    pub(crate) fn finish(&self, batch: &DatasetBatch) -> Result<RecordBatch> {
        let Some(adapter) = &self.adapter else {
            // A row carrier: the batch states the rows of the chunk and has no column.
            return RecordBatch::try_new_with_options(
                Arc::clone(&self.projected_schema),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
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
        adapter.adapt(&encode_nd_record_batch(&nd)?).map_err(|e| {
            DataFusionError::Execution(format!("failed to adapt the batch to the scan schema: {e}"))
        })
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
    use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};
    use beacon_nd_dataset::{
        ChunkGrid, Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray, NdDataset,
        read_chunk, select_columns,
    };
    use beacon_nd_encoding::encoding::{decode_nd_array, decode_nd_record_batch, encoded_schema};

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

        let output = FileOutput::try_new(dataset.as_ref(), &schema).unwrap();
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

        let output = FileOutput::try_new(dataset.as_ref(), &schema).unwrap();
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

        let output = FileOutput::try_new(&dataset, &schema).unwrap();
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

        let output = FileOutput::try_new(&dataset, &schema).unwrap();
        let batch = output.finish(&chunk(&dataset, 0).await).unwrap();

        assert_eq!(batch.schema(), schema);
        let lon = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(
            lon.values().as_primitive::<Float64Type>().values(),
            &[0.0, 1.0, 2.0]
        );
    }

    #[tokio::test]
    async fn a_count_chunk_is_a_row_carrier() {
        let schema = Arc::new(Schema::empty());
        let dataset = count_dataset();

        let output = FileOutput::try_new(dataset.as_ref(), &schema).unwrap();
        let batch = output.finish(&chunk(dataset.as_ref(), 0).await).unwrap();

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 12);
    }

    #[tokio::test]
    async fn a_column_of_another_type_is_cast_to_the_table_type() {
        let x = Dimensions::try_new([Dimension::new("x", 3)]).unwrap();
        let values = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]));
        let temp = MemoryVariable::new("temp", NdArrowArray::try_new(values, x).unwrap());
        let dataset = MemoryDataset::try_new("a.nd", vec![temp]).unwrap();
        let schema = projected(&[("temp", DataType::Float64)]);

        let output = FileOutput::try_new(&dataset, &schema).unwrap();
        let batch = output.finish(&chunk(&dataset, 0).await).unwrap();

        let temp = decode_nd_array(batch.column(0), 0).unwrap();
        assert_eq!(temp.data_type(), &DataType::Float64);
        assert_eq!(
            temp.values().as_primitive::<Float64Type>().values(),
            &[1.0, 2.0, 3.0]
        );
    }
}
