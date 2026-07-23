use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_seadatanet_instrument_l05() -> ScalarUDF {
    create_udf(
        "map_seadatanet_instrument_l05",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_seadatanet_instrument_l05_impl),
    )
}

fn map_seadatanet_instrument_l05_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    fn extract_first_value(s: &str) -> Option<String> {
        if let Some(start) = s.find('(') {
            if let Some(end) = s[start..].find(')') {
                let l05_code = &s[start + 1..start + end];
                return Some(format!("SDN:L05::{}", l05_code));
            }
        }
        None
    }

    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = crate::util::downcast_arg::<arrow::array::StringArray>(
                flag,
                "map_seadatanet_instrument_l05",
            )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn extracts_l05_code_from_parenthesised_text() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Sensor (130)".into())));
        match map_seadatanet_instrument_l05_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "SDN:L05::130"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn no_parentheses_yields_null() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("plain".into())));
        assert!(matches!(
            map_seadatanet_instrument_l05_impl(&[input]).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
    }

    #[test]
    fn impl_array_path() {
        let input =
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("Z (44)"), Some("x")])));
        let ColumnarValue::Array(arr) = map_seadatanet_instrument_l05_impl(&[input]).unwrap()
        else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "SDN:L05::44");
        assert!(arr.is_null(1));
    }

    #[test]
    fn impl_rejects_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(map_seadatanet_instrument_l05_impl(&[input]).is_err());
    }
}
