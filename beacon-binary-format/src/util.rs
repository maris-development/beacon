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
