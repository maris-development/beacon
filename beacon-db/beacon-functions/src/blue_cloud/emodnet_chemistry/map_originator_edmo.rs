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
            let flag_array = crate::util::downcast_arg::<arrow::array::StringArray>(
                flag,
                "map_emodnet_chemistry_originator_edmo",
            )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn extracts_the_last_parenthesised_edmo_code() {
        // rfind locates the final '(' — so the EDMO code, not earlier groups.
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Origin (1) Lab (486)".into())));
        match map_emodnet_chemistry_originator_edmo_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "486"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn no_parentheses_yields_null() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("plain".into())));
        assert!(matches!(
            map_emodnet_chemistry_originator_edmo_impl(&[input]).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
    }

    #[test]
    fn impl_array_path() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("Lab (42)"),
            Some("none"),
        ])));
        let ColumnarValue::Array(arr) =
            map_emodnet_chemistry_originator_edmo_impl(&[input]).unwrap()
        else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "42");
        assert!(arr.is_null(1));
    }

    #[test]
    fn impl_rejects_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(map_emodnet_chemistry_originator_edmo_impl(&[input]).is_err());
    }
}
