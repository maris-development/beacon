use std::{collections::HashMap, sync::Arc};

use arrow_schema::{ArrowError, FieldRef, SchemaRef};

pub struct ArrowTypeWidening {
    pub strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ArrowTypeWidening {
    pub fn new(strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        Self { strategy }
    }

    pub fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        self.strategy.merge_schemas(schema_refs)
    }
}

pub trait ArrowTypeWideningStrategy: Send + Sync {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError>;
}

pub struct DefaultArrowTypeWidening;

impl ArrowTypeWideningStrategy for DefaultArrowTypeWidening {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        if schema_refs.is_empty() {
            return Err(ArrowError::SchemaError(
                "No schemas provided for merging".to_string(),
            ));
        }

        let mut merged_fields = Vec::new();
        let mut field_map: HashMap<String, FieldRef> = HashMap::new();

        for schema_ref in schema_refs {
            for field in schema_ref.fields() {
                let field_name = field.name().clone();
                if let Some(existing_field) = field_map.get(&field_name) {
                    // If the field already exists, we need to check if the types are compatible
                    if existing_field.data_type() != field.data_type() {
                        return Err(ArrowError::SchemaError(format!(
                            "Incompatible types for field '{}': {:?} vs {:?}",
                            field_name,
                            existing_field.data_type(),
                            field.data_type()
                        )));
                    }
                } else {
                    // If the field does not exist, add it to the map and merged fields
                    field_map.insert(field_name.clone(), field.clone());
                    merged_fields.push(field.clone());
                }
            }
        }

        Ok(Arc::new(arrow_schema::Schema::new(merged_fields)))
    }
}
