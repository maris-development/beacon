//! What one file gives the scan: its projected arrays, and how a chunk of them
//! becomes a batch of the projected schema.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::{encode_nd_record_batch, encoded_schema, NdRecordBatch};
use beacon_datafusion_ext::scan_adapt::batch_adapter_factory;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema;
use beacon_nd_array::dataset::{AnyDataset, Dataset};
use beacon_nd_array::projection::DatasetProjection;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr_adapter::BatchAdapter;

/// How a chunk of one file becomes a batch of the projected schema.
#[derive(Debug)]
pub(super) struct FileOutput {
    /// Reorders, casts and null-fills onto the projected schema.
    adapter: BatchAdapter,
}

impl FileOutput {
    /// Project `dataset` to the file columns of `projected_schema`.
    ///
    /// `None` when the file has none of them. Such a file gives no row.
    pub(super) fn resolve(
        dataset: AnyDataset,
        projected_schema: &SchemaRef,
    ) -> Result<Option<(Dataset, Self)>> {
        let file_schema = any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            DataFusionError::Execution(format!("failed to derive the schema of the dataset: {e}"))
        })?;
        let projection: Vec<usize> = file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| projected_schema.index_of(field.name()).is_ok())
            .map(|(index, _)| index)
            .collect();
        if projection.is_empty() {
            return Ok(None);
        }

        let source_schema: SchemaRef = Arc::new(encoded_schema(&file_schema.project(&projection)?));
        // Every column is nd-encoded and casts leniently, so the strict rule is never asked.
        let adapter = batch_adapter_factory(
            projected_schema.clone(),
            Arc::new(DefaultArrowTypeWidening::new()),
        )
        .make_adapter(&source_schema)?;

        let projected = dataset
            .project(&DatasetProjection {
                dimension_projection: None,
                index_projection: Some(projection),
            })
            .map_err(|e| {
                DataFusionError::Execution(format!("failed to project the dataset: {e}"))
            })?;
        let AnyDataset::Regular(projected) = projected else {
            return Err(ragged_error(projected.name()));
        };

        Ok(Some((projected, Self { adapter })))
    }

    /// Encode one chunk, and adapt it to the projected schema.
    pub(super) fn finish(&self, batch: &NdRecordBatch) -> Result<RecordBatch> {
        let encoded = encode_nd_record_batch(batch)?;
        self.adapter.adapt_batch(&encoded).map_err(|e| {
            DataFusionError::Execution(format!("failed to adapt the batch to the scan schema: {e}"))
        })
    }
}

/// The error for a ragged dataset, which this scan does not read.
pub(in crate::datafusion) fn refuse_ragged(dataset: &AnyDataset) -> Result<()> {
    match dataset {
        AnyDataset::Regular(_) => Ok(()),
        AnyDataset::Ragged { .. } => Err(ragged_error(dataset.name())),
    }
}

fn ragged_error(name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "'{name}' is a ragged dataset; the NetCDF reader does not support ragged datasets yet"
    ))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use beacon_datafusion_ext::nd::nd_encoded_field_of;
    use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};

    use super::*;
    use crate::datafusion::scan::chunks::FileScan;

    async fn open(name: &str) -> AnyDataset {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join(name);
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let object = store
            .head(&Path::from_absolute_path(&path).unwrap())
            .await
            .unwrap();
        crate::oxcdf_reader::open_object(store, &object)
            .await
            .unwrap()
    }

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

        let (dataset, _) = FileOutput::resolve(open("gridded-example.nc").await, &schema)
            .unwrap()
            .expect("the file has lat");

        let names: Vec<&String> = dataset.arrays.keys().collect();
        assert_eq!(names, ["lat"]);
    }

    #[tokio::test]
    async fn a_file_with_none_of_the_columns_gives_nothing() {
        let schema = projected(vec![enc("no_such_column", DataType::Float64)]);

        let resolved = FileOutput::resolve(open("gridded-example.nc").await, &schema).unwrap();

        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn a_batch_gets_the_projected_order_and_nulls_for_a_missing_column() {
        // A column the file lacks first, then a real column.
        let schema = projected(vec![
            enc("no_such_column", DataType::Float64),
            enc("lat", DataType::Float32),
        ]);

        let (dataset, output) = FileOutput::resolve(open("gridded-example.nc").await, &schema)
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

    #[tokio::test]
    async fn a_ragged_dataset_is_refused() {
        let error = refuse_ragged(&open("wod_ctd_1964.nc").await).unwrap_err();

        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error}"
        );
        assert!(error.to_string().contains("ragged"), "{error}");
        assert!(refuse_ragged(&open("gridded-example.nc").await).is_ok());
    }
}
