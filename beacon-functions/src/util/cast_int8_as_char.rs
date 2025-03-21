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
            let int8_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap();

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
