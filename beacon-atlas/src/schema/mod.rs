pub mod _type;
pub mod field;

use indexmap::IndexMap;

use crate::schema::_type::AtlasDataType;

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
        let mut columns: IndexMap<String, AtlasDataType> = indexmap::IndexMap::new();

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
    pub data_type: AtlasDataType,
}

impl AtlasColumn {
    pub fn new(name: String, data_type: AtlasDataType) -> Self {
        Self { name, data_type }
    }
}

fn super_type_for_mode(
    left: &AtlasDataType,
    right: &AtlasDataType,
    mode: AtlasSuperTypingMode,
) -> Option<AtlasDataType> {
    // fast path for identical types, which should be common in practice and avoids unnecessary super type checks
    if left == right {
        return Some(left.clone());
    }

    // match mode {
    //     AtlasSuperTypingMode::General => beacon_common::super_typing::super_type_arrow(left, right),
    //     AtlasSuperTypingMode::GroupBased => {
    //         if is_numeric_type(left) && is_numeric_type(right) {
    //             return beacon_common::super_typing::super_type_arrow(left, right);
    //         }

    //         if is_string_or_timestamp(left) && is_string_or_timestamp(right) {
    //             return beacon_common::super_typing::super_type_arrow(left, right);
    //         }

    //         None
    //     }
    // }
    None
}

fn is_numeric_type(data_type: &AtlasDataType) -> bool {
    matches!(
        data_type,
        AtlasDataType::I8
            | AtlasDataType::I16
            | AtlasDataType::I32
            | AtlasDataType::I64
            | AtlasDataType::U8
            | AtlasDataType::U16
            | AtlasDataType::U32
            | AtlasDataType::U64
            | AtlasDataType::F32
            | AtlasDataType::F64
    )
}

fn is_string_type(data_type: &AtlasDataType) -> bool {
    matches!(data_type, AtlasDataType::String)
}

fn is_timestamp_type(data_type: &AtlasDataType) -> bool {
    matches!(data_type, AtlasDataType::Timestamp)
}

fn is_string_or_timestamp(data_type: &AtlasDataType) -> bool {
    is_string_type(data_type) || is_timestamp_type(data_type)
}

// #[cfg(test)]
// mod tests {
//     use arrow::datatypes::{DataType, TimeUnit};

//     use crate::schema::{AtlasColumn, AtlasSchema, AtlasSuperTypingMode};

//     #[test]
//     fn general_mode_allows_numeric_and_utf8() {
//         let left = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Int32)]);
//         let right = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Utf8)]);

//         let merged =
//             AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::General)
//                 .unwrap();

//         assert_eq!(merged.columns[0].data_type, DataType::Utf8);
//     }

//     #[test]
//     fn group_mode_allows_utf8_and_timestamp() {
//         let left = AtlasSchema::new(vec![AtlasColumn::new(
//             "observed_at".to_string(),
//             DataType::Utf8,
//         )]);
//         let right = AtlasSchema::new(vec![AtlasColumn::new(
//             "observed_at".to_string(),
//             DataType::Timestamp(TimeUnit::Second, None),
//         )]);

//         let merged =
//             AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::GroupBased)
//                 .unwrap();

//         assert_eq!(merged.columns[0].data_type, DataType::Utf8);
//     }

//     #[test]
//     fn group_mode_rejects_numeric_and_utf8() {
//         let left = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Int32)]);
//         let right = AtlasSchema::new(vec![AtlasColumn::new("value".to_string(), DataType::Utf8)]);

//         let error =
//             AtlasSchema::merge_all_with_mode(&[left, right], AtlasSuperTypingMode::GroupBased)
//                 .unwrap_err();

//         assert!(
//             error
//                 .to_string()
//                 .contains("conflicting data types for column 'value'"),
//         );
//     }
// }
