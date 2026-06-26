use arrow::datatypes::{DataType, Fields, Schema, TimeUnit};

#[derive(Debug, Clone, thiserror::Error)]
pub enum SuperTypeError {
    #[error("Cannot find a common super type for {left} and {right} in column {column_name}")]
    NoCommonSuperType {
        left: DataType,
        right: DataType,
        column_name: String,
    },
    #[error("No schemas provided")]
    NoSchemasProvided,
}

pub type Result<T> = std::result::Result<T, SuperTypeError>;

pub fn super_type_schema(schemas: &[arrow::datatypes::SchemaRef]) -> Result<Schema> {
    if schemas.is_empty() {
        return Err(SuperTypeError::NoSchemasProvided);
    }

    let mut fields = indexmap::IndexMap::new();
    for schema in schemas {
        for field in schema.fields.iter() {
            let name = field.name().to_string();
            let dtype = field.data_type().clone();
            match fields.get_mut(&name) {
                Some(existing_dtype) => {
                    if let Some(supert_type) = super_type_arrow(existing_dtype, &dtype) {
                        *existing_dtype = supert_type;
                    } else {
                        return Err(SuperTypeError::NoCommonSuperType {
                            left: existing_dtype.clone(),
                            right: dtype,
                            column_name: field.name().to_string(),
                        });
                    }
                }
                None => {
                    fields.insert(name, dtype.into());
                }
            }
        }
    }

    Ok(arrow::datatypes::Schema::new(Fields::from(
        fields
            .into_iter()
            .map(|(name, dtype)| arrow::datatypes::Field::new(&name, dtype.into(), true))
            .collect::<Vec<_>>(),
    )))
}

pub fn super_type_arrow_schema(
    schemas: &[arrow::datatypes::Schema],
) -> Option<arrow::datatypes::Schema> {
    let mut fields = indexmap::IndexMap::new();
    for schema in schemas {
        for field in schema.fields.iter() {
            let name = field.name().to_string();
            let dtype = field.data_type().clone();
            match fields.get_mut(&name) {
                Some(existing_dtype) => {
                    if let Some(supert_type) = super_type_arrow(existing_dtype, &dtype) {
                        *existing_dtype = supert_type;
                    } else {
                        return None;
                    }
                }
                None => {
                    fields.insert(name, dtype.into());
                }
            }
        }
    }

    Some(arrow::datatypes::Schema::new(Fields::from(
        fields
            .into_iter()
            .map(|(name, dtype)| arrow::datatypes::Field::new(&name, dtype.into(), false))
            .collect::<Vec<_>>(),
    )))
}

/// Determine the smallest common super type for two Arrow data types.
///
/// This function takes two data types (`left` and `right`) and returns an option
/// containing the common super type if one exists.
/// If both data types are equal, it returns a clone of that type.
/// For certain type combinations, the super type is defined to follow conventions
/// from libraries such as Polars and Numpy.
///
/// # Parameters
///
/// - `left`: A reference to the first Arrow data type.
/// - `right`: A reference to the second Arrow data type.
///
/// # Returns
///
/// An `Option<DataType>` with the common super type if a valid one exists, otherwise `None`.
pub fn super_type_arrow(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }

    let super_type = match (left.clone(), right.clone()) {
        (DataType::Null, _) => right.clone(),
        (_, DataType::Null) => left.clone(),
        (DataType::Int8, DataType::Boolean) => DataType::Int8,
        (DataType::Int8, DataType::Int16) => DataType::Int16,
        (DataType::Int8, DataType::Int32) => DataType::Int32,
        (DataType::Int8, DataType::Int64) => DataType::Int64,
        (DataType::Int8, DataType::UInt8) => DataType::Int16,
        (DataType::Int8, DataType::UInt16) => DataType::Int32,
        (DataType::Int8, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int8, DataType::UInt64) => DataType::Float64,
        (DataType::Int8, DataType::Float32) => DataType::Float32,
        (DataType::Int8, DataType::Float64) => DataType::Float64,
        (DataType::Int8, DataType::Utf8) => DataType::Utf8,
        (DataType::Int8, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int16, DataType::Boolean) => DataType::Int16,
        (DataType::Int16, DataType::Int8) => DataType::Int16,
        (DataType::Int16, DataType::Int32) => DataType::Int32,
        (DataType::Int16, DataType::Int64) => DataType::Int64,
        (DataType::Int16, DataType::UInt8) => DataType::Int16,
        (DataType::Int16, DataType::UInt16) => DataType::Int32,
        (DataType::Int16, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int16, DataType::UInt64) => DataType::Float64,
        (DataType::Int16, DataType::Float32) => DataType::Float32,
        (DataType::Int16, DataType::Float64) => DataType::Float64,
        (DataType::Int16, DataType::Utf8) => DataType::Utf8,
        (DataType::Int16, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int32, DataType::Boolean) => DataType::Int32,
        (DataType::Int32, DataType::Int8) => DataType::Int32,
        (DataType::Int32, DataType::Int16) => DataType::Int32,
        (DataType::Int32, DataType::Int64) => DataType::Int64,
        (DataType::Int32, DataType::UInt8) => DataType::Int32,
        (DataType::Int32, DataType::UInt16) => DataType::Int32,
        (DataType::Int32, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int32, DataType::UInt64) => DataType::Float64,
        (DataType::Int32, DataType::Float32) => DataType::Float32,
        (DataType::Int32, DataType::Float64) => DataType::Float64,
        (DataType::Int32, DataType::Utf8) => DataType::Utf8,
        (DataType::Int32, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int64, DataType::Boolean) => DataType::Int64,
        (DataType::Int64, DataType::Int8) => DataType::Int64,
        (DataType::Int64, DataType::Int16) => DataType::Int64,
        (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Int64, DataType::UInt8) => DataType::Int64,
        (DataType::Int64, DataType::UInt16) => DataType::Int64,
        (DataType::Int64, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int64, DataType::UInt64) => DataType::Float64,
        (DataType::Int64, DataType::Float32) => DataType::Float64,
        (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::Int64, DataType::Utf8) => DataType::Utf8,
        (DataType::Int64, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt8, DataType::Boolean) => DataType::UInt8,
        (DataType::UInt8, DataType::Int8) => DataType::Int16,
        (DataType::UInt8, DataType::Int16) => DataType::Int16,
        (DataType::UInt8, DataType::Int32) => DataType::Int32,
        (DataType::UInt8, DataType::Int64) => DataType::Int64,
        (DataType::UInt8, DataType::UInt16) => DataType::UInt16,
        (DataType::UInt8, DataType::UInt32) => DataType::UInt32,
        (DataType::UInt8, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt8, DataType::Float32) => DataType::Float32,
        (DataType::UInt8, DataType::Float64) => DataType::Float64,
        (DataType::UInt8, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt8, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt16, DataType::Boolean) => DataType::UInt16,
        (DataType::UInt16, DataType::Int8) => DataType::Int32,
        (DataType::UInt16, DataType::Int16) => DataType::Int32,
        (DataType::UInt16, DataType::Int32) => DataType::Int32,
        (DataType::UInt16, DataType::Int64) => DataType::Int64,
        (DataType::UInt16, DataType::UInt8) => DataType::UInt16,
        (DataType::UInt16, DataType::UInt32) => DataType::UInt32,
        (DataType::UInt16, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt16, DataType::Float32) => DataType::Float32,
        (DataType::UInt16, DataType::Float64) => DataType::Float64,
        (DataType::UInt16, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt16, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt32, DataType::Boolean) => DataType::UInt32,
        (DataType::UInt32, DataType::Int8) => DataType::Int64,
        (DataType::UInt32, DataType::Int16) => DataType::Int64,
        (DataType::UInt32, DataType::Int32) => DataType::Int64,
        (DataType::UInt32, DataType::Int64) => DataType::Int64,
        (DataType::UInt32, DataType::UInt8) => DataType::UInt32,
        (DataType::UInt32, DataType::UInt16) => DataType::UInt32,
        (DataType::UInt32, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt32, DataType::Float32) => DataType::Float32,
        (DataType::UInt32, DataType::Float64) => DataType::Float64,
        (DataType::UInt32, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt32, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt64, DataType::Boolean) => DataType::UInt64,
        (DataType::UInt64, DataType::Int8) => DataType::Float64,
        (DataType::UInt64, DataType::Int16) => DataType::Float64,
        (DataType::UInt64, DataType::Int32) => DataType::Float64,
        (DataType::UInt64, DataType::Int64) => DataType::Float64,
        (DataType::UInt64, DataType::UInt8) => DataType::UInt64,
        (DataType::UInt64, DataType::UInt16) => DataType::UInt64,
        (DataType::UInt64, DataType::UInt32) => DataType::UInt64,
        (DataType::UInt64, DataType::Float32) => DataType::Float64,
        (DataType::UInt64, DataType::Float64) => DataType::Float64,
        (DataType::UInt64, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt64, DataType::Timestamp(_, _)) => DataType::Float64,

        (DataType::Float32, DataType::Boolean) => DataType::Float32,
        (DataType::Float32, DataType::Int8) => DataType::Float32,
        (DataType::Float32, DataType::Int16) => DataType::Float32,
        (DataType::Float32, DataType::Int32) => DataType::Float64,
        (DataType::Float32, DataType::Int64) => DataType::Float64,
        (DataType::Float32, DataType::UInt8) => DataType::Float32,
        (DataType::Float32, DataType::UInt16) => DataType::Float32,
        (DataType::Float32, DataType::UInt32) => DataType::Float64,
        (DataType::Float32, DataType::UInt64) => DataType::Float64,
        (DataType::Float32, DataType::Float64) => DataType::Float64,
        (DataType::Float32, DataType::Utf8) => DataType::Utf8,
        (DataType::Float32, DataType::Timestamp(_, _)) => DataType::Float64,

        (DataType::Float64, DataType::Utf8) => DataType::Utf8,
        (DataType::Float64, _) => DataType::Float64,

        (DataType::LargeUtf8, _) => DataType::LargeUtf8,
        (_, DataType::LargeUtf8) => DataType::LargeUtf8,
        (DataType::Utf8, _) => DataType::Utf8,
        (_, DataType::Utf8) => DataType::Utf8,

        (DataType::Timestamp(_, _), DataType::Int8) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int16) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int32) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int64) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt8) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt16) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt32) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt64) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Float32) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Float64) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Utf8) => DataType::Utf8,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => {
            DataType::Timestamp(TimeUnit::Second, None)
        }

        _ => return None,
    };

    Some(super_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, SchemaRef};
    use std::sync::Arc;

    fn ts() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }

    #[test]
    fn equal_types_return_themselves() {
        for dt in [
            DataType::Boolean,
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            ts(),
        ] {
            assert_eq!(super_type_arrow(&dt, &dt), Some(dt.clone()));
        }
    }

    #[test]
    fn null_is_absorbed_by_the_other_type() {
        assert_eq!(
            super_type_arrow(&DataType::Null, &DataType::Int8),
            Some(DataType::Int8)
        );
        assert_eq!(
            super_type_arrow(&DataType::Float64, &DataType::Null),
            Some(DataType::Float64)
        );
        // Null + Null hits the equality short-circuit.
        assert_eq!(
            super_type_arrow(&DataType::Null, &DataType::Null),
            Some(DataType::Null)
        );
    }

    #[test]
    fn integer_widening() {
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::Int16),
            Some(DataType::Int16)
        );
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            super_type_arrow(&DataType::Int32, &DataType::Int16),
            Some(DataType::Int32)
        );
    }

    #[test]
    fn signed_unsigned_mix_widens_to_hold_both() {
        // A signed/unsigned pair needs the next wider signed type.
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::UInt8),
            Some(DataType::Int16)
        );
        assert_eq!(
            super_type_arrow(&DataType::UInt8, &DataType::Int8),
            Some(DataType::Int16)
        );
        assert_eq!(
            super_type_arrow(&DataType::UInt16, &DataType::Int8),
            Some(DataType::Int32)
        );
    }

    #[test]
    fn uint64_with_signed_or_64bit_float_follows_polars_numpy_to_float64() {
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::UInt64),
            Some(DataType::Float64)
        );
        assert_eq!(
            super_type_arrow(&DataType::UInt64, &DataType::Int64),
            Some(DataType::Float64)
        );
        assert_eq!(
            super_type_arrow(&DataType::Int64, &DataType::Float32),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn float_promotion_rules() {
        // small ints + f32 stay f32
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::Float32),
            Some(DataType::Float32)
        );
        assert_eq!(
            super_type_arrow(&DataType::Float32, &DataType::Int16),
            Some(DataType::Float32)
        );
        // 32-bit ints + f32 need f64 to avoid precision loss
        assert_eq!(
            super_type_arrow(&DataType::Float32, &DataType::Int32),
            Some(DataType::Float64)
        );
        // f64 absorbs everything numeric
        assert_eq!(
            super_type_arrow(&DataType::Float64, &DataType::Int32),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn utf8_and_large_utf8_absorb_other_types() {
        assert_eq!(
            super_type_arrow(&DataType::Int32, &DataType::Utf8),
            Some(DataType::Utf8)
        );
        assert_eq!(
            super_type_arrow(&DataType::Utf8, &DataType::Int32),
            Some(DataType::Utf8)
        );
        // LargeUtf8 takes precedence over Utf8 from either side.
        assert_eq!(
            super_type_arrow(&DataType::LargeUtf8, &DataType::Utf8),
            Some(DataType::LargeUtf8)
        );
        assert_eq!(
            super_type_arrow(&DataType::Utf8, &DataType::LargeUtf8),
            Some(DataType::LargeUtf8)
        );
    }

    #[test]
    fn timestamp_combinations() {
        assert_eq!(
            super_type_arrow(&ts(), &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            super_type_arrow(&DataType::Int32, &ts()),
            Some(DataType::Int64)
        );
        assert_eq!(
            super_type_arrow(&ts(), &DataType::UInt64),
            Some(DataType::Float64)
        );
        assert_eq!(
            super_type_arrow(&ts(), &DataType::Utf8),
            Some(DataType::Utf8)
        );
        // Two timestamps normalise to second precision, no timezone.
        assert_eq!(
            super_type_arrow(
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                &DataType::Timestamp(TimeUnit::Second, None)
            ),
            Some(DataType::Timestamp(TimeUnit::Second, None))
        );
    }

    #[test]
    fn relation_is_not_commutative_for_some_pairs() {
        // (Int8, Boolean) is defined, but the reverse is not.
        assert_eq!(
            super_type_arrow(&DataType::Int8, &DataType::Boolean),
            Some(DataType::Int8)
        );
        assert_eq!(super_type_arrow(&DataType::Boolean, &DataType::Int8), None);
    }

    #[test]
    fn unsupported_pairs_return_none() {
        assert_eq!(super_type_arrow(&DataType::Boolean, &DataType::Int8), None);
        assert_eq!(
            super_type_arrow(&DataType::Date32, &DataType::Int8),
            None
        );
        assert_eq!(
            super_type_arrow(&DataType::Binary, &DataType::Float64),
            None
        );
    }

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, dt)| Field::new(*n, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn super_type_schema_errors_on_empty_input() {
        let err = super_type_schema(&[]).unwrap_err();
        assert!(matches!(err, SuperTypeError::NoSchemasProvided));
    }

    #[test]
    fn super_type_schema_widens_shared_columns_and_unions_the_rest() {
        let s1 = schema(&[("a", DataType::Int32), ("b", DataType::Float32)]);
        let s2 = schema(&[("b", DataType::Float64), ("c", DataType::Utf8)]);

        let merged = super_type_schema(&[s1, s2]).unwrap();

        // Field order follows first-seen insertion order: a, b, c.
        let names: Vec<&str> = merged.fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);

        assert_eq!(merged.field_with_name("a").unwrap().data_type(), &DataType::Int32);
        assert_eq!(merged.field_with_name("b").unwrap().data_type(), &DataType::Float64);
        assert_eq!(merged.field_with_name("c").unwrap().data_type(), &DataType::Utf8);

        // super_type_schema marks every output field nullable.
        assert!(merged.fields.iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn super_type_schema_errors_when_columns_have_no_common_type() {
        let s1 = schema(&[("a", DataType::Boolean)]);
        let s2 = schema(&[("a", DataType::Int8)]);

        let err = super_type_schema(&[s1, s2]).unwrap_err();
        match err {
            SuperTypeError::NoCommonSuperType { column_name, .. } => {
                assert_eq!(column_name, "a");
            }
            other => panic!("expected NoCommonSuperType, got {other:?}"),
        }
    }

    #[test]
    fn super_type_arrow_schema_returns_non_nullable_fields() {
        let s1 = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let s2 = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let merged = super_type_arrow_schema(&[s1, s2]).unwrap();
        let field = merged.field_with_name("a").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
        // This variant marks fields non-nullable, unlike super_type_schema.
        assert!(!field.is_nullable());
    }

    #[test]
    fn super_type_arrow_schema_returns_none_on_conflict() {
        let s1 = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let s2 = Schema::new(vec![Field::new("a", DataType::Int8, true)]);
        assert_eq!(super_type_arrow_schema(&[s1, s2]), None);
    }
}
