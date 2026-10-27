//! The map from Arrow types to Croissant data types.

use arrow::datatypes::{DataType, Schema};
use beacon_core::comments::COMMENT_METADATA_KEY;

/// The Croissant data types that Beacon sends, as schema.org types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CroissantType {
    Boolean,
    Integer,
    Float,
    Text,
    Date,
    DateTime,
    Time,
}

impl CroissantType {
    /// The `dataType` value in a Croissant `Field`.
    pub fn name(self) -> &'static str {
        match self {
            CroissantType::Boolean => "sc:Boolean",
            CroissantType::Integer => "sc:Integer",
            CroissantType::Float => "sc:Float",
            CroissantType::Text => "sc:Text",
            CroissantType::Date => "sc:Date",
            CroissantType::DateTime => "sc:DateTime",
            CroissantType::Time => "sc:Time",
        }
    }

    /// The Croissant type of an Arrow type, or `None` when Croissant cannot describe it.
    pub fn of(data_type: &DataType) -> Option<CroissantType> {
        match data_type {
            DataType::Boolean => Some(CroissantType::Boolean),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(CroissantType::Integer),
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Some(CroissantType::Float),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(CroissantType::Text),
            DataType::Date32 | DataType::Date64 => Some(CroissantType::Date),
            DataType::Timestamp(_, _) => Some(CroissantType::DateTime),
            DataType::Time32(_) | DataType::Time64(_) => Some(CroissantType::Time),
            DataType::Dictionary(_, value) => CroissantType::of(value),
            _ => None,
        }
    }
}

/// A table column that Croissant describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedColumn {
    /// The Beacon column name.
    pub name: String,
    /// The Croissant type of the column.
    pub data_type: CroissantType,
    /// The `COMMENT ON COLUMN` text.
    pub comment: Option<String>,
}

/// The part of a table schema that Croissant describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedSchema {
    pub columns: Vec<ServedColumn>,
    /// Columns that Croissant cannot describe, in schema order.
    pub omitted: Vec<String>,
    /// The `COMMENT ON TABLE` text.
    pub table_comment: Option<String>,
}

impl ServedSchema {
    /// Splits `schema` into served and omitted columns.
    pub fn from_arrow(schema: &Schema) -> Self {
        let mut columns = Vec::new();
        let mut omitted = Vec::new();
        for field in schema.fields() {
            match CroissantType::of(field.data_type()) {
                Some(data_type) => columns.push(ServedColumn {
                    name: field.name().clone(),
                    data_type,
                    comment: field.metadata().get(COMMENT_METADATA_KEY).cloned(),
                }),
                None => {
                    tracing::debug!(
                        column = %field.name(),
                        data_type = %field.data_type(),
                        "Croissant cannot describe this column type"
                    );
                    omitted.push(field.name().clone());
                }
            }
        }
        Self {
            columns,
            omitted,
            table_comment: schema.metadata().get(COMMENT_METADATA_KEY).cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn arrow_types_map_to_croissant_types() {
        use arrow::datatypes::{DataType::*, TimeUnit};
        let cases = [
            (Boolean, Some(CroissantType::Boolean)),
            (Int8, Some(CroissantType::Integer)),
            (UInt64, Some(CroissantType::Integer)),
            (Float16, Some(CroissantType::Float)),
            (Float64, Some(CroissantType::Float)),
            (Decimal128(10, 2), Some(CroissantType::Float)),
            (Utf8, Some(CroissantType::Text)),
            (LargeUtf8, Some(CroissantType::Text)),
            (Utf8View, Some(CroissantType::Text)),
            (Date32, Some(CroissantType::Date)),
            (Date64, Some(CroissantType::Date)),
            (
                Timestamp(TimeUnit::Microsecond, None),
                Some(CroissantType::DateTime),
            ),
            (Time64(TimeUnit::Nanosecond), Some(CroissantType::Time)),
            (
                Dictionary(Box::new(Int32), Box::new(Utf8)),
                Some(CroissantType::Text),
            ),
            (Binary, None),
        ];
        for (arrow, expected) in cases {
            assert_eq!(CroissantType::of(&arrow), expected, "{arrow}");
        }
    }

    #[test]
    fn names_use_the_schema_org_prefix() {
        let cases = [
            (CroissantType::Boolean, "sc:Boolean"),
            (CroissantType::Integer, "sc:Integer"),
            (CroissantType::Float, "sc:Float"),
            (CroissantType::Text, "sc:Text"),
            (CroissantType::Date, "sc:Date"),
            (CroissantType::DateTime, "sc:DateTime"),
            (CroissantType::Time, "sc:Time"),
        ];
        for (data_type, name) in cases {
            assert_eq!(data_type.name(), name);
        }
    }

    #[test]
    fn from_arrow_splits_served_and_omitted_columns_and_reads_comments() {
        let comment = |text: &str| HashMap::from([("comment".to_string(), text.to_string())]);
        let schema = Schema::new(vec![
            Field::new("depth", DataType::Float64, true).with_metadata(comment("depth in m")),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ])
        .with_metadata(comment("obs"));

        let served = ServedSchema::from_arrow(&schema);

        assert_eq!(
            served.columns,
            vec![ServedColumn {
                name: "depth".to_string(),
                data_type: CroissantType::Float,
                comment: Some("depth in m".to_string()),
            }]
        );
        assert_eq!(served.omitted, vec!["tags".to_string()]);
        assert_eq!(served.table_comment.as_deref(), Some("obs"));
    }
}
