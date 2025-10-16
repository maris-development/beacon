use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_measuring_area_type_feature_type() -> ScalarUDF {
    create_udf(
        "map_measuring_area_type_feature_type",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_measuring_area_type_feature_type_impl),
    )
}

fn map_measuring_area_type_feature_type_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array
                .iter()
                .map(|flag| flag.and_then(map_str_feature_type));

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value.as_ref().and_then(map_str_feature_type);

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}

fn map_str_feature_type<S: AsRef<str>>(input: S) -> Option<String> {
    let input = input.as_ref();
    if input.contains("curve") {
        Some("trajectory".to_string())
    } else if input.contains("point") {
        Some("profile".to_string())
    } else {
        None
    }
}
