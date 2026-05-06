use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::format::schema::ResolvedSchema;

/// Convert a [`ResolvedSchema`] (from the atlas global schema) into an Arrow [`SchemaRef`].
///
/// Each column becomes a nullable Arrow field with its type derived via the
/// `NdArrayDataType → arrow::datatypes::DataType` From impl.
pub fn global_schema_to_arrow(schema: &ResolvedSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| {
            let arrow_dt: DataType = col.data_type.clone().into();
            Field::new(&col.name, arrow_dt, true)
        })
        .collect();

    Arc::new(Schema::new(fields))
}
