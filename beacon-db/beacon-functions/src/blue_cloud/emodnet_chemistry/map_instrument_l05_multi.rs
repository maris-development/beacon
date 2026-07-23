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

const SEPARATOR: &str = " | ";

fn map_emodnet_chemistry_instrument_l05_multi_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = crate::util::downcast_arg::<arrow::array::StringArray>(
                flag,
                "map_emodnet_chemistry_instrument_l05_multi",
            )?;

            let array = flag_array.iter().map(|flag| {
                flag.map(|value| {
                    extract_parenthesized_values_ref(value)
                        .join(SEPARATOR)
                        .to_string()
                })
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value.as_ref().map(|value| {
                extract_parenthesized_values_ref(value)
                    .join(SEPARATOR)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn extracts_every_parenthesised_group() {
        let values = extract_parenthesized_values_ref("CTD (130) and XBT (132)");
        assert_eq!(values, vec!["SDN:L05::130", "SDN:L05::132"]);
    }

    #[test]
    fn skips_empty_groups_and_stops_at_unclosed_parenthesis() {
        // Empty () is dropped; an unclosed '(' ends the scan.
        assert_eq!(extract_parenthesized_values_ref("a () b (7)"), vec!["SDN:L05::7"]);
        assert_eq!(extract_parenthesized_values_ref("a (7) b (oops"), vec!["SDN:L05::7"]);
        assert!(extract_parenthesized_values_ref("no groups").is_empty());
    }

    #[test]
    fn impl_scalar_joins_with_separator() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("A (1) B (2)".into())));
        match map_emodnet_chemistry_instrument_l05_multi_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "SDN:L05::1 | SDN:L05::2")
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn impl_array_path() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("X (5)")])));
        let ColumnarValue::Array(arr) =
            map_emodnet_chemistry_instrument_l05_multi_impl(&[input]).unwrap()
        else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "SDN:L05::5");
    }

    #[test]
    fn impl_rejects_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(map_emodnet_chemistry_instrument_l05_multi_impl(&[input]).is_err());
    }
}
