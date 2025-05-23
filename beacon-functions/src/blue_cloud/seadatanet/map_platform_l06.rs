use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_seadatanet_platform_l06() -> ScalarUDF {
    create_udf(
        "map_seadatanet_platform_l06",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_seadatanet_platform_l06_impl),
    )
}

fn map_seadatanet_platform_l06_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    fn extract_first_value(s: &str) -> Option<String> {
        if let Some(start) = s.find('(') {
            if let Some(end) = s[start..].find(')') {
                let l06_code = &s[start + 1..start + end];
                return Some(format!("SDN:L06::{}", l06_code));
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
                .map(|flag| flag.map(|value| extract_first_value(value)).flatten());

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .map(|value| extract_first_value(value).map(|s| s.to_string()))
                .flatten();

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => {
            return Err(datafusion::error::DataFusionError::Execution(
                "Invalid input type".to_string(),
            ))
        }
    }
}
