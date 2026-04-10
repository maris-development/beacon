use arrow::datatypes::{DataType, Field};
use indexmap::IndexMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum AtlasSuperTypingMode {
    #[default]
    General,
    GroupBased,
}

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
        self.merge_schemas_with_mode(AtlasSuperTypingMode::General)
    }

    pub fn merge_schemas_with_mode(&self, mode: AtlasSuperTypingMode) -> anyhow::Result<Self> {
        AtlasSchema::merge_all_with_mode(std::slice::from_ref(self), mode)
    }

    pub fn merge_all_with_mode(
        schemas: &[AtlasSchema],
        mode: AtlasSuperTypingMode,
    ) -> anyhow::Result<Self> {
        let mut columns: IndexMap<String, DataType> = indexmap::IndexMap::new();

        for schema in schemas {
            for column in &schema.columns {
                if let Some(existing_data_type) = columns.get(&column.name) {
                    if existing_data_type != &column.data_type {
                        let merged_data_type = super_type_for_mode(
                            existing_data_type,
                            &column.data_type,
                            mode,
                        )
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "conflicting data types for column '{}': {:?} vs {:?} for mode {:?}",
                                column.name,
                                existing_data_type,
                                column.data_type,
                                mode
                            )
                        })?;
                        columns.insert(column.name.clone(), merged_data_type);
                    }
                } else {
                    columns.insert(column.name.clone(), column.data_type.clone());
                }
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

fn super_type_for_mode(
    left: &DataType,
    right: &DataType,
    mode: AtlasSuperTypingMode,
) -> Option<DataType> {
    // fast path for identical types, which should be common in practice and avoids unnecessary super type checks
    if left == right {
        return Some(left.clone());
    }

    match mode {
        AtlasSuperTypingMode::General => beacon_common::super_typing::super_type_arrow(left, right),
        AtlasSuperTypingMode::GroupBased => {
            if is_numeric_type(left) && is_numeric_type(right) {
                return beacon_common::super_typing::super_type_arrow(left, right);
            }

            if is_string_or_timestamp(left) && is_string_or_timestamp(right) {
                return beacon_common::super_typing::super_type_arrow(left, right);
            }

            None
        }
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn is_timestamp_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

fn is_string_or_timestamp(data_type: &DataType) -> bool {
    is_string_type(data_type) || is_timestamp_type(data_type)
}

pub enum AtlasDataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Timestamp,
    Binary,
    String,
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use crate::schema::{AtlasColumn, AtlasSchema, AtlasSuperTypingMode};

    #[test]
    fn general_mode_allows_numeric_and_utf8() {
        let left = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Int32)]);
        let right = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Utf8)]);

        let merged =
            AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::General)
                .unwrap();

        assert_eq!(merged.columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn group_mode_allows_utf8_and_timestamp() {
        let left = AtlasSchema::new(vec![AtlasColumn::new(
            "observed_at".to_string(),
            DataType::Utf8,
        )]);
        let right = AtlasSchema::new(vec![AtlasColumn::new(
            "observed_at".to_string(),
            DataType::Timestamp(TimeUnit::Second, None),
        )]);

        let merged =
            AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::GroupBased)
                .unwrap();

        assert_eq!(merged.columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn group_mode_rejects_numeric_and_utf8() {
        let left = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Int32)]);
        let right = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Utf8)]);

        let error =
            AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::GroupBased)
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("conflicting data types for column 'value'"),
        );
    }
}
