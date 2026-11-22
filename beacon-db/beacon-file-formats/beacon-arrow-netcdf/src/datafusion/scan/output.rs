//! What one file gives the scan: its projected arrays, and how a chunk of them
//! becomes a batch of the projected schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::{encode_nd_record_batch, encoded_schema, NdRecordBatch};
use beacon_datafusion_ext::scan_adapt::batch_adapter_factory;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_nd_array::dataset::Dataset;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr_adapter::BatchAdapter;

/// How a chunk of one file becomes a batch of the projected schema.
#[derive(Debug)]
pub(super) struct FileOutput {
    /// Reorders, casts and null-fills onto the projected schema.
    adapter: BatchAdapter,
}

impl FileOutput {
    /// Keep the arrays of `dataset` that `projected_schema` names.
    ///
    /// `None` when the file has none of them. Such a file gives no row.
    pub(super) fn resolve(
        mut dataset: Dataset,
        projected_schema: &SchemaRef,
    ) -> Result<Option<(Dataset, Self)>> {
        dataset
            .arrays
            .retain(|name, _| projected_schema.index_of(name).is_ok());
        if dataset.arrays.is_empty() {
            return Ok(None);
        }

        let file_schema = Schema::new(
            dataset
                .arrays
                .iter()
                .map(|(name, array)| Field::new(name, DataType::from(array.datatype()), true))
                .collect::<Vec<_>>(),
        );
        // Every column is nd-encoded and casts leniently, so the strict rule is never asked.
        let adapter = batch_adapter_factory(
            projected_schema.clone(),
            Arc::new(DefaultArrowTypeWidening::new()),
        )
        .make_adapter(&Arc::new(encoded_schema(&file_schema)))?;

        Ok(Some((dataset, Self { adapter })))
    }

    /// Encode one chunk, and adapt it to the projected schema.
    pub(super) fn finish(&self, batch: &NdRecordBatch) -> Result<RecordBatch> {
        let encoded = encode_nd_record_batch(batch)?;
        self.adapter.adapt_batch(&encoded).map_err(|e| {
            DataFusionError::Execution(format!("failed to adapt the batch to the scan schema: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::FieldRef;
    use beacon_datafusion_ext::nd::nd_encoded_field_of;

    use super::*;
    use crate::datafusion::scan::chunks::FileScan;
    use crate::datafusion::test_util::{open_test_file, GRIDDED_FILE};

    /// The encoded field of a logical file column.
    fn enc(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(nd_encoded_field_of(&Field::new(name, data_type, true)))
    }

    fn projected(fields: Vec<FieldRef>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[tokio::test]
    async fn a_file_is_projected_to_the_wanted_columns() {
        let schema = projected(vec![enc("lat", DataType::Float32)]);

        let (dataset, _) = FileOutput::resolve(open_test_file(GRIDDED_FILE).await, &schema)
            .unwrap()
            .expect("the file has lat");

        let names: Vec<&String> = dataset.arrays.keys().collect();
        assert_eq!(names, ["lat"]);
    }

    #[tokio::test]
    async fn a_file_with_none_of_the_columns_gives_nothing() {
        let schema = projected(vec![enc("no_such_column", DataType::Float64)]);

        let resolved = FileOutput::resolve(open_test_file(GRIDDED_FILE).await, &schema).unwrap();

        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn a_batch_gets_the_projected_order_and_nulls_for_a_missing_column() {
        // A column the file lacks first, then a real column.
        let schema = projected(vec![
            enc("no_such_column", DataType::Float64),
            enc("lat", DataType::Float32),
        ]);

        let (dataset, output) = FileOutput::resolve(open_test_file(GRIDDED_FILE).await, &schema)
            .unwrap()
            .expect("the file has lat");
        let scan = FileScan::try_new(dataset, 8192).unwrap();
        let nd = scan.read(&scan.chunks()[0]).await.unwrap();
        let batch = output.finish(&nd).unwrap();

        assert_eq!(batch.schema(), schema);
        // One encoded row carries the whole chunk.
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).null_count(), 1, "a missing column is null");
    }
}
