use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn extract_parenthesized_values_ref(input: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut start = 0;

    while let Some(open) = input[start..].find('(') {
        let open_idx = start + open + 1;

        if let Some(close) = input[open_idx..].find(')') {
            let close_idx = open_idx + close;
            let value = input[open_idx..close_idx].trim();

            if !value.is_empty() {
                results.push(format!("SDN:L05::{}", value));
            }

            start = close_idx + 1;
        } else {
            break;
        }
    }

    results
}

pub fn map_emodnet_chemistry_instrument_l05_multi() -> ScalarUDF {
    create_udf(
        "map_emodnet_chemistry_instrument_l05_multi",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_emodnet_chemistry_instrument_l05_multi_impl),
    )
}

fn map_emodnet_chemistry_instrument_l05_multi_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|value| {
                    extract_parenthesized_values_ref(value)
                        .join("|")
                        .to_string()
                })
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value.as_ref().map(|value| {
                extract_parenthesized_values_ref(value)
                    .join("|")
                    .to_string()
            });

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}
