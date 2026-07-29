use std::sync::Arc;

use datafusion::{logical_expr::ScalarUDF, prelude::create_udf, scalar::ScalarValue};

pub fn cast_int8_as_char() -> ScalarUDF {
    create_udf(
        "cast_int8_as_char",
        vec![datafusion::arrow::datatypes::DataType::Int8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(cast_int8_as_char_impl),
    )
}

fn cast_int8_as_char_impl(
    parameters: &[datafusion::logical_expr::ColumnarValue],
) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
    match &parameters[0] {
        datafusion::logical_expr::ColumnarValue::Array(array) => {
            let int8_array =
                crate::util::downcast_arg::<arrow::array::Int8Array>(array, "cast_int8_as_char")?;

            let array = arrow::array::StringArray::from_iter(
                int8_array
                    .iter()
                    .map(|i| i.map(|i| (i as u8 as char).to_string())),
            );

            Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
                array,
            )))
        }
        datafusion::logical_expr::ColumnarValue::Scalar(ScalarValue::Int8(value)) => {
            Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                ScalarValue::Utf8(value.map(|i| (i as u8 as char).to_string())),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input to cast_int8_as_char".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use datafusion::logical_expr::ColumnarValue;

    #[test]
    fn udf_metadata_is_stable() {
        let udf = cast_int8_as_char();
        assert_eq!(udf.name(), "cast_int8_as_char");
    }

    /// The array path maps each Int8 to the character of its byte value, keeping
    /// NULLs as NULL (an ASCII code point stays that character).
    #[test]
    fn array_path_maps_bytes_to_chars_and_preserves_nulls() {
        let input = arrow::array::Int8Array::from(vec![Some(65), Some(97), None, Some(48)]);
        let out = cast_int8_as_char_impl(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        let ColumnarValue::Array(array) = out else {
            panic!("array input must yield an array output");
        };
        let strings = array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), "A");
        assert_eq!(strings.value(1), "a");
        assert!(strings.is_null(2), "NULL input stays NULL");
        assert_eq!(strings.value(3), "0");
    }

    #[test]
    fn scalar_path_maps_value_and_null() {
        let out = cast_int8_as_char_impl(&[ColumnarValue::Scalar(ScalarValue::Int8(Some(66)))])
            .unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "B"),
            other => panic!("unexpected output: {other:?}"),
        }
        let out =
            cast_int8_as_char_impl(&[ColumnarValue::Scalar(ScalarValue::Int8(None))]).unwrap();
        assert!(matches!(
            out,
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
    }

    /// A non-Int8 input is rejected rather than silently mis-cast.
    #[test]
    fn wrong_scalar_type_is_an_error() {
        let err = cast_int8_as_char_impl(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert!(err.is_err());
    }
}
