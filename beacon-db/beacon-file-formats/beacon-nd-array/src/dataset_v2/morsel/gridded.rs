//! The morsel of one chunk of a gridded dataset.

use std::{fmt, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Schema, SchemaRef},
};
use beacon_datafusion_ext::{
    morsel_planner::Morsel,
    nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch, encode_nd_record_batch},
};
use datafusion::error::Result;
use futures::future::BoxFuture;

use super::common::{column_error, file_field, null_column, read_subset, row_count_batch};
use crate::{
    array::subset::ArraySubset, arrow::batch::generate_array_subset_from_chunk,
    dataset_v2::gridded::GriddedDataset,
};

/// One chunk of a gridded dataset.
pub struct GriddedChunkMorsel {
    dataset: Arc<GriddedDataset>,
    subset: ArraySubset,
    schema: SchemaRef, // The logical file schema that the scan projects, without partition columns
}

impl GriddedChunkMorsel {
    /// Create the morsel that reads `subset` of the grid of `dataset`.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The gridded dataset of one file
    /// * `subset` - The part of the grid to read
    /// * `schema` - The logical file schema that the scan projects, without partition columns
    pub fn new(dataset: Arc<GriddedDataset>, subset: ArraySubset, schema: SchemaRef) -> Self {
        Self {
            dataset,
            subset,
            schema,
        }
    }

    /// The part of the grid that this morsel reads.
    pub fn subset(&self) -> &ArraySubset {
        &self.subset
    }

    /// Read the chunk into one nd-encoded batch, in the column order of the scan schema.
    ///
    /// Each array is cut on its own axes and stays un-broadcast. A column that
    /// the file does not hold is a rank-0 null, which broadcasts onto the grid.
    /// A scan without columns (`COUNT(*)`) gets a batch with only a row count.
    async fn read(&self) -> Result<RecordBatch> {
        if self.schema.fields().is_empty() {
            return row_count_batch(self.subset.shape.iter().product());
        }

        let mut fields = Vec::with_capacity(self.schema.fields().len());
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let column = self
                .read_column(field)
                .await
                .map_err(|e| column_error(field.name(), e))?;
            fields.push(file_field(field, column.data_type().clone()));
            columns.push(column);
        }

        let batch = NdRecordBatch::try_new(Arc::new(Schema::new(fields)), columns, self.target()?)?;
        encode_nd_record_batch(&batch)
    }

    /// The grid of the chunk: the grid dimensions, with the sizes of the subset.
    fn target(&self) -> Result<Dimensions> {
        Dimensions::try_new(
            self.dataset
                .grid_dimensions
                .iter()
                .zip(&self.subset.shape)
                .map(|(name, &size)| Dimension::new(name.as_str(), size))
                .collect(),
        )
    }

    /// Read the part of the chunk that lies on the axes of the array of `field`.
    async fn read_column(&self, field: &Field) -> Result<NdArrowArray> {
        let Some(array) = self.dataset.inner.arrays.get(field.name()) else {
            return null_column(field.data_type());
        };
        let part = generate_array_subset_from_chunk(
            &self.subset,
            &self.dataset.grid_dimensions,
            array.as_ref(),
        );
        let (values, dimensions) = read_subset(array.as_ref(), part).await?;
        NdArrowArray::try_new(values, Dimensions::try_new(dimensions)?)
    }
}

impl fmt::Debug for GriddedChunkMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GriddedChunkMorsel")
            .field("dataset", &self.dataset.inner.name)
            .field("start", &self.subset.start)
            .field("shape", &self.subset.shape)
            .finish()
    }
}

impl Morsel for GriddedChunkMorsel {
    fn produce(self: Box<Self>) -> BoxFuture<'static, Result<RecordBatch>> {
        Box::pin(async move { self.read().await })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::{array::Array, datatypes::DataType};
    use indexmap::IndexMap;

    use super::*;
    use crate::dataset_v2::{
        Dataset,
        morsel::test_util::{array, decode_all, i32_values, schema},
    };

    /// A 2x3 `temp` grid on (time, lat), with a `lat` coordinate.
    fn grid() -> Arc<GriddedDataset> {
        let arrays = IndexMap::from([
            (
                "temp".to_string(),
                array((0..6).collect(), &[2, 3], &["time", "lat"]),
            ),
            ("lat".to_string(), array(vec![10, 20, 30], &[3], &["lat"])),
        ]);
        let dataset = Dataset::from_arrays("test".to_string(), arrays, HashSet::new()).unwrap();
        Arc::new(GriddedDataset::try_new(dataset, None).unwrap())
    }

    async fn produce(schema: SchemaRef, start: &[usize], shape: &[usize]) -> RecordBatch {
        let subset = ArraySubset::new(start.to_vec(), shape.to_vec());
        Box::new(GriddedChunkMorsel::new(grid(), subset, schema))
            .produce()
            .await
            .unwrap()
    }

    /// Produce the morsel of `subset`, then decode and broadcast it to flat rows.
    async fn read(schema: SchemaRef, start: &[usize], shape: &[usize]) -> RecordBatch {
        decode_all(&produce(schema, start, shape).await)
    }

    #[tokio::test]
    async fn a_chunk_broadcasts_onto_its_grid() {
        let schema = schema(&[("temp", DataType::Int32), ("lat", DataType::Int32)]);

        let batch = read(schema, &[1, 1], &[1, 2]).await;

        assert_eq!(i32_values(&batch, "temp"), vec![4, 5]);
        assert_eq!(i32_values(&batch, "lat"), vec![20, 30]);
    }

    #[tokio::test]
    async fn columns_follow_the_schema_order() {
        let schema = schema(&[("lat", DataType::Int32), ("temp", DataType::Int32)]);

        let batch = read(schema, &[0, 0], &[2, 3]).await;

        assert_eq!(batch.schema().field(0).name(), "lat");
        assert_eq!(i32_values(&batch, "lat"), vec![10, 20, 30, 10, 20, 30]);
    }

    #[tokio::test]
    async fn values_keep_the_file_type() {
        let schema = schema(&[("temp", DataType::Float64)]);

        let batch = read(schema, &[0, 0], &[1, 3]).await;

        assert_eq!(i32_values(&batch, "temp"), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_missing_column_is_null_on_every_row() {
        let schema = schema(&[("temp", DataType::Int32), ("salt", DataType::Float64)]);

        let batch = read(schema, &[0, 0], &[2, 3]).await;

        let salt = batch.column_by_name("salt").unwrap();
        assert_eq!(salt.len(), 6);
        assert_eq!(salt.null_count(), 6);
    }

    #[tokio::test]
    async fn an_empty_schema_gives_the_row_count_of_the_chunk() {
        let batch = produce(schema(&[]), &[0, 0], &[2, 3]).await;

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 6);
    }
}
