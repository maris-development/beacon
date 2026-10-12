//! The map from Arrow types to DAP2 types, and the value conversion.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::error::ArrowError;

/// The metadata key that `TableComments::apply` writes on the schema and on each field.
const COMMENT_KEY: &str = "comment";

/// The DAP2 types that Beacon sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DapType {
    Int32,
    UInt32,
    Float32,
    Float64,
    String,
}

impl DapType {
    /// The type name in a DDS or a DAS.
    pub fn name(self) -> &'static str {
        match self {
            DapType::Int32 => "Int32",
            DapType::UInt32 => "UInt32",
            DapType::Float32 => "Float32",
            DapType::Float64 => "Float64",
            DapType::String => "String",
        }
    }

    /// The `_FillValue` text for a null value. A null string is `""`, so strings have none.
    pub fn fill_value(self) -> Option<&'static str> {
        match self {
            DapType::Int32 => Some("2147483647"),
            DapType::UInt32 => Some("4294967295"),
            DapType::Float32 | DapType::Float64 => Some("NaN"),
            DapType::String => None,
        }
    }

    /// The DAP2 type of an Arrow type, or `None` when DAP2 cannot carry it.
    ///
    /// DAP2 `Byte` is unsigned, and libraries send `Int16` in different ways, so every
    /// small integer widens to `Int32`.
    pub fn of(data_type: &DataType) -> Option<DapType> {
        match data_type {
            DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::Boolean => Some(DapType::Int32),
            DataType::UInt32 => Some(DapType::UInt32),
            DataType::Float16 | DataType::Float32 => Some(DapType::Float32),
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64 => Some(DapType::Float64),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(DapType::String),
            DataType::Dictionary(_, value) => DapType::of(value),
            _ => None,
        }
    }
}

/// Whether DAP2 sends the type as seconds since 1970-01-01T00:00:00Z.
pub fn is_time(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => true,
        DataType::Dictionary(_, value) => is_time(value),
        _ => false,
    }
}

/// A table column that DAP2 serves.
#[derive(Debug, Clone, PartialEq)]
pub struct ServedColumn {
    /// The Beacon column name.
    pub name: String,
    /// The Arrow type in the table.
    pub data_type: DataType,
    /// The type on the wire.
    pub dap_type: DapType,
    /// The `COMMENT ON COLUMN` text.
    pub comment: Option<String>,
}

impl ServedColumn {
    /// Whether DAP2 sends the column as seconds.
    pub fn is_time(&self) -> bool {
        is_time(&self.data_type)
    }

    /// Whether the column holds floats, the only columns where `NaN` is a value.
    pub fn is_float(&self) -> bool {
        matches!(
            self.data_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    }
}

/// The part of a table schema that DAP2 serves.
#[derive(Debug, Clone, PartialEq)]
pub struct ServedSchema {
    pub columns: Vec<ServedColumn>,
    /// Columns that DAP2 cannot carry, in schema order.
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
            match DapType::of(field.data_type()) {
                Some(dap_type) => columns.push(ServedColumn {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    dap_type,
                    comment: field.metadata().get(COMMENT_KEY).cloned(),
                }),
                None => {
                    tracing::debug!(
                        column = %field.name(),
                        data_type = %field.data_type(),
                        "DAP2 cannot serve this column type"
                    );
                    omitted.push(field.name().clone());
                }
            }
        }
        Self {
            columns,
            omitted,
            table_comment: schema.metadata().get(COMMENT_KEY).cloned(),
        }
    }

    /// The served column with the Beacon name `name`.
    pub fn column(&self, name: &str) -> Option<&ServedColumn> {
        self.columns.iter().find(|column| column.name == name)
    }
}

/// Converts one Arrow column into the array that the XDR writer reads.
///
/// The result is an `Int32Array`, `UInt32Array`, `Float32Array`, `Float64Array` or
/// `StringArray`. Null values stay null.
pub fn convert(array: &ArrayRef, dap_type: DapType) -> Result<ArrayRef, ArrowError> {
    if is_time(array.data_type()) {
        return seconds(array);
    }
    let target = match dap_type {
        DapType::Int32 => DataType::Int32,
        DapType::UInt32 => DataType::UInt32,
        DapType::Float32 => DataType::Float32,
        DapType::Float64 => DataType::Float64,
        DapType::String => DataType::Utf8,
    };
    cast(array, &target)
}

/// Seconds since 1970-01-01T00:00:00Z as `Float64`.
fn seconds(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let array = match array.data_type() {
        DataType::Dictionary(_, value) => cast(array, value)?,
        _ => array.clone(),
    };
    // seconds = raw * factor / divisor. Division keeps sub-second units exact.
    let (raw_type, factor, divisor) = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => (DataType::Int64, 1.0, 1.0),
        DataType::Timestamp(TimeUnit::Millisecond, _) => (DataType::Int64, 1.0, 1e3),
        DataType::Timestamp(TimeUnit::Microsecond, _) => (DataType::Int64, 1.0, 1e6),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => (DataType::Int64, 1.0, 1e9),
        DataType::Date64 => (DataType::Int64, 1.0, 1e3),
        DataType::Date32 => (DataType::Int32, 86_400.0, 1.0),
        other => {
            return Err(ArrowError::CastError(format!("{other} is not a time type")));
        }
    };
    let raw = cast(&cast(&array, &raw_type)?, &DataType::Float64)?;
    let raw = raw
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ArrowError::CastError("cast to Float64 gave another type".into()))?;
    let out: Float64Array = arrow::compute::kernels::arity::unary(raw, |v| v * factor / divisor);
    Ok(Arc::new(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Int16Array, Int32Array, StringArray, StringViewArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Fields};
    use std::collections::HashMap;

    #[test]
    fn small_integers_and_booleans_widen_to_int32() {
        for data_type in [
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::Boolean,
        ] {
            assert_eq!(DapType::of(&data_type), Some(DapType::Int32), "{data_type}");
        }
        assert_eq!(DapType::of(&DataType::UInt32), Some(DapType::UInt32));
        assert_eq!(DapType::of(&DataType::Float32), Some(DapType::Float32));
    }

    #[test]
    fn wide_numbers_and_time_become_float64() {
        for data_type in [
            DataType::Int64,
            DataType::UInt64,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
        ] {
            assert_eq!(
                DapType::of(&data_type),
                Some(DapType::Float64),
                "{data_type}"
            );
        }
    }

    #[test]
    fn text_and_dictionaries_of_text_become_string() {
        for data_type in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            assert_eq!(
                DapType::of(&data_type),
                Some(DapType::String),
                "{data_type}"
            );
        }
    }

    #[test]
    fn nested_and_binary_types_are_not_served() {
        for data_type in [
            DataType::Binary,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::Struct(Fields::empty()),
        ] {
            assert_eq!(DapType::of(&data_type), None, "{data_type}");
        }
    }

    #[test]
    fn from_arrow_keeps_comments_and_lists_omitted_columns() {
        let comment = |text: &str| HashMap::from([("comment".to_string(), text.to_string())]);
        let schema = Schema::new(vec![
            Field::new("depth", DataType::Float64, true).with_metadata(comment("depth in meters")),
            Field::new("raw", DataType::Binary, true),
        ])
        .with_metadata(comment("about obs"));

        let served = ServedSchema::from_arrow(&schema);

        assert_eq!(served.columns.len(), 1);
        assert_eq!(served.columns[0].name, "depth");
        assert_eq!(
            served.columns[0].comment.as_deref(),
            Some("depth in meters")
        );
        assert_eq!(served.omitted, vec!["raw".to_string()]);
        assert_eq!(served.table_comment.as_deref(), Some("about obs"));
        assert!(served.column("depth").is_some());
        assert!(served.column("raw").is_none());
    }

    #[test]
    fn convert_widens_and_keeps_nulls() {
        let input: ArrayRef = Arc::new(Int16Array::from(vec![Some(-3), None]));

        let out = convert(&input, DapType::Int32).unwrap();

        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.value(0), -3);
        assert!(out.is_null(1));
    }

    #[test]
    fn convert_booleans_to_zero_and_one() {
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));

        let out = convert(&input, DapType::Int32).unwrap();

        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.values().to_vec(), vec![1, 0]);
    }

    #[test]
    fn convert_timestamps_to_seconds() {
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_704_067_200_500_000),
            None,
        ]));

        let out = convert(&input, DapType::Float64).unwrap();

        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 1_704_067_200.5);
        assert!(out.is_null(1));
    }

    #[test]
    fn convert_dates_to_seconds() {
        let input: ArrayRef = Arc::new(Date32Array::from(vec![19_723]));

        let out = convert(&input, DapType::Float64).unwrap();

        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 1_704_067_200.0);
    }

    #[test]
    fn convert_text_views_to_utf8() {
        let input: ArrayRef = Arc::new(StringViewArray::from(vec![Some("a"), None]));

        let out = convert(&input, DapType::String).unwrap();

        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "a");
        assert!(out.is_null(1));
    }
}
