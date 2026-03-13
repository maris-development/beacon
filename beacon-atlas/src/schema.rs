use arrow::datatypes::{DataType, Field};
use indexmap::IndexMap;

/// Placeholder schema type for streaming context wiring.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtlasSchema {
    pub columns: Vec<AtlasColumn>,
}

impl AtlasSchema {
    pub fn new(columns: Vec<AtlasColumn>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn from_arrow_schema(arrow_schema: &arrow::datatypes::Schema) -> Self {
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| AtlasColumn::new(field.name().clone(), field.data_type().clone()))
            .collect();

        Self { columns }
    }

    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|col| arrow::datatypes::Field::new(col.name.clone(), col.data_type.clone(), true))
            .collect();

        arrow::datatypes::Schema::new(fields)
    }

    pub fn merge_schemas(&self) -> anyhow::Result<Self> {
        let mut columns: IndexMap<String, DataType> = indexmap::IndexMap::new();

        for column in &self.columns {
            if let Some(existing_data_type) = columns.get(&column.name) {
                // Super type
                if existing_data_type != &column.data_type {
                    let merged_data_type = beacon_common::super_typing::super_type_arrow(
                        existing_data_type,
                        &column.data_type,
                    )
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "conflicting data types for column '{}': {:?} vs {:?}",
                            column.name,
                            existing_data_type,
                            column.data_type
                        )
                    })?;
                    columns.insert(column.name.clone(), merged_data_type);
                }
            } else {
                columns.insert(column.name.clone(), column.data_type.clone());
            }
        }

        Ok(Self {
            columns: columns
                .into_iter()
                .map(|(name, data_type)| AtlasColumn { name, data_type })
                .collect(),
        })
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
