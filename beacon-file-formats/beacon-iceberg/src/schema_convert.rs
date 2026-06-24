//! Conversion between Arrow schemas (used by DataFusion) and Iceberg schemas.
//!
//! DataFusion hands us plain Arrow schemas with no Iceberg field-id metadata.
//! Iceberg requires every field to carry a stable integer id, so we first attach
//! ids with [`new_fields_with_ids`] and then convert into an Iceberg [`Schema`].

use arrow::datatypes::Schema as ArrowSchema;
use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
use iceberg_rust::spec::schema::Schema as IcebergSchema;
use iceberg_rust::spec::types::StructType;

/// Convert an Arrow schema into an Iceberg schema, assigning fresh field ids.
pub fn arrow_schema_to_iceberg(arrow_schema: &ArrowSchema) -> anyhow::Result<IcebergSchema> {
    // Assign Iceberg/Parquet field ids (1-based, depth-first) to every field.
    let mut index = 0;
    let fields_with_ids = new_fields_with_ids(arrow_schema.fields(), &mut index);

    let struct_type = StructType::try_from(&fields_with_ids).map_err(|error| {
        anyhow::anyhow!("Failed to convert Arrow schema to Iceberg struct: {error}")
    })?;

    let mut builder = IcebergSchema::builder();
    builder.with_schema_id(0);
    for field in struct_type.iter() {
        builder.with_struct_field(field.clone());
    }
    builder
        .build()
        .map_err(|error| anyhow::anyhow!("Failed to build Iceberg schema: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn converts_primitive_schema_with_unique_ids() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let iceberg = arrow_schema_to_iceberg(&arrow_schema).expect("conversion should succeed");
        let fields = iceberg.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert!(fields[0].required, "non-nullable arrow field -> required");
        assert_eq!(fields[1].name, "name");
        assert!(!fields[1].required, "nullable arrow field -> optional");
        // Field ids must be unique (Iceberg rejects duplicates).
        assert_ne!(fields[0].id, fields[1].id);
    }
}
