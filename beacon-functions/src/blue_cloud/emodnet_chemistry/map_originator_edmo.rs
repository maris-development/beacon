use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_emodnet_chemistry_originator_edmo() -> ScalarUDF {
    create_udf(
        "map_emodnet_chemistry_originator_edmo",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_emodnet_chemistry_originator_edmo_impl),
    )
}

fn map_emodnet_chemistry_originator_edmo_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    fn extract_last_value(s: &str) -> Option<String> {
        if let Some(start) = s.rfind('(') {
            if let Some(end) = s[start..].find(')') {
                let originator_edmo = &s[start + 1..start + end];
                return Some(originator_edmo.to_string());
            }
        }
        None
    }

    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array
                .iter()
                .map(|flag| flag.and_then(extract_last_value));

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let edmo = value
                .as_ref()
                .and_then(|value| extract_last_value(value).map(|s| s.to_string()));
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(edmo),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}
