use std::{collections::HashMap, sync::Arc};

use crate::array::{NdArrowArray, backend::ArrayBackend};

pub struct NdRecordBatch<B: ArrayBackend = Arc<dyn ArrayBackend>> {
    pub schema: Arc<arrow_schema::Schema>,
    pub arrays: Vec<NdArrowArray<B>>,
    pub dimensions: Vec<String>,
    pub shape: Vec<usize>,
}

impl<B: ArrayBackend> NdRecordBatch<B> {
    pub fn new(
        schema: Arc<arrow_schema::Schema>,
        arrays: Vec<NdArrowArray<B>>,
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
            if field.data_type() != array.data_type() {
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

    pub fn arrays(&self) -> &[NdArrowArray<B>] {
        &self.arrays
    }
}
