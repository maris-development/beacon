use std::{collections::HashMap, sync::Arc};

use arrow::array::ArrayRef;
use futures::{StreamExt, stream::BoxStream};

use crate::array::NdArrowArray;

pub struct NdRecordBatch {
    pub schema: Arc<arrow_schema::Schema>,
    pub arrays: Vec<Arc<dyn NdArrowArray>>,
    pub dimensions: Vec<String>,
    pub shape: Vec<usize>,
}

impl NdRecordBatch {
    pub fn new(
        schema: Arc<arrow_schema::Schema>,
        arrays: Vec<Arc<dyn NdArrowArray>>,
    ) -> anyhow::Result<Self> {
        // Validate that the number of arrays matches the number of fields in the schema
        if schema.fields().len() != arrays.len() {
            return Err(anyhow::anyhow!(
                "Number of arrays ({}) does not match number of fields in schema ({})",
                arrays.len(),
                schema.fields().len()
            ));
        }

        // Validate that each array's data type matches the corresponding field's data type
        for (field, array) in schema.fields().iter().zip(arrays.iter()) {
            if *field.data_type() != array.data_type() {
                return Err(anyhow::anyhow!(
                    "Data type mismatch for field '{}': expected {:?}, got {:?}",
                    field.name(),
                    field.data_type(),
                    array.data_type()
                ))?;
            }
        }

        let mut unique_dims: HashMap<_, _> = HashMap::new();

        for array in &arrays {
            for (dim, size) in array.dimensions().iter().zip(array.shape().iter()) {
                if let Some(existing_size) = unique_dims.get(dim) {
                    if *existing_size != *size {
                        return Err(anyhow::anyhow!(
                            "Dimension '{}' has conflicting sizes: {} and {}",
                            dim,
                            existing_size,
                            size
                        ))?;
                    }
                } else {
                    unique_dims.insert(dim.clone(), *size);
                }
            }
        }

        Ok(Self {
            schema,
            arrays,
            dimensions: unique_dims.keys().cloned().collect(),
            shape: unique_dims.values().cloned().collect(),
        })
    }

    pub fn schema(&self) -> &arrow_schema::Schema {
        &self.schema
    }

    pub async fn try_as_arrow_stream(
        &self,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<arrow::record_batch::RecordBatch>>> {
        // find the dimensions and shape to broadcast to.
        // Get the biggest dimensions across all arrays to determine broadcasting.
        let max_dims_opt = self
            .arrays
            .iter()
            .max_by_key(|a| a.dimensions().len())
            .map(|a| (a.dimensions(), a.shape()));

        match max_dims_opt {
            Some((max_dims, max_shape)) => {
                // Broadcast all arrays to the max dimensions and shape.
                let mut broadcasted_arrays: Vec<ArrayRef> = vec![];
                for array in &self.arrays {
                    let view = array.broadcast(&max_shape, &max_dims).await?;
                    let arrow_array = view.as_arrow_array_ref().await?;
                    broadcasted_arrays.push(arrow_array);
                }

                // Create an Arrow RecordBatch from the broadcasted arrays.
                let record_batch = arrow::record_batch::RecordBatch::try_new(
                    self.schema.clone(),
                    broadcasted_arrays,
                )?;

                Ok(futures::stream::once(async { Ok(record_batch) }).boxed())
            }
            None => {
                // No arrays, return an empty stream.
                Ok(futures::stream::empty().boxed())
            }
        }
    }
}
