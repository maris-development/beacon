/// Placeholder schema type for streaming context wiring.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtlasSchema {
    pub columns: Vec<AtlasColumn>,
}

impl AtlasSchema {
    pub fn new(columns: Vec<AtlasColumn>) -> Self {
        Self { columns }
    }

    pub fn from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> Self {
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| AtlasColumn::new(field.name().clone(), field.data_type().clone()))
            .collect();

        Self { columns }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtlasColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
}

impl AtlasColumn {
    pub fn new(name: String, data_type: arrow::datatypes::DataType) -> Self {
        Self { name, data_type }
    }
}
