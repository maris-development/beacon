use std::sync::Arc;

use crate::{NdArrowArray, error};

pub struct NdRecordBatch {
    pub schema: Arc<arrow_schema::Schema>,
    pub arrays: Vec<NdArrowArray>,
}

impl NdRecordBatch {
    pub fn new(
        schema: Arc<arrow_schema::Schema>,
        arrays: Vec<NdArrowArray>,
    ) -> Result<Self, error::NdArrayError> {
        // Validate that the number of arrays matches the number of fields in the schema
        if schema.fields().len() != arrays.len() {
            return Err(error::NdArrayError::SchemaMismatch(format!(
                "Number of arrays ({}) does not match number of fields in schema ({})",
                arrays.len(),
                schema.fields().len()
            )));
        }

        // Validate that each array's data type matches the corresponding field's data type
        for (field, array) in schema.fields().iter().zip(arrays.iter()) {
            if field.data_type() != array.data_type() {
                return Err(error::NdArrayError::SchemaMismatch(format!(
                    "Data type mismatch for field '{}': expected {:?}, got {:?}",
                    field.name(),
                    field.data_type(),
                    array.data_type()
                )));
            }
        }

        Ok(Self { schema, arrays })
    }

    pub fn schema(&self) -> &arrow_schema::Schema {
        &self.schema
    }

    pub fn arrays(&self) -> &[NdArrowArray] {
        &self.arrays
    }

    // pub fn broadcast_streamable(&self) ->
}
