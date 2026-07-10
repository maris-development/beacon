use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_originator_edmo() -> ScalarUDF {
    create_udf(
        "map_originator_edmo",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_originator_edmo_impl),
    )
}

fn map_originator_edmo_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    fn extract_first_value(s: &str) -> Option<String> {
        if let Some(start) = s.rfind('(') {
            if let Some(end) = s[start..].find(')') {
                let edmo_code = &s[start + 1..start + end];
                return Some(edmo_code.to_string());
            }
        }
        None
    }

    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = crate::util::downcast_arg::<arrow::array::StringArray>(
                flag,
                "map_originator_edmo",
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

    fn scalar_result(input: Option<&str>) -> Option<String> {
        let value = ColumnarValue::Scalar(ScalarValue::Utf8(input.map(|s| s.to_string())));
        match map_originator_edmo_impl(&[value]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v,
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn scalar_uses_last_parentheses() {
        // The originator string contains several parenthesized groups; the EDMO
        // code lives in the last one.
        assert_eq!(
            scalar_result(Some("National Oceanography Centre (NOC) (43)")),
            Some("43".to_string())
        );
    }

    #[test]
    fn scalar_single_parentheses() {
        assert_eq!(
            scalar_result(Some("Some Institute (123)")),
            Some("123".to_string())
        );
    }

    #[test]
    fn scalar_no_parentheses_is_none() {
        assert_eq!(scalar_result(Some("No code here")), None);
    }

    #[test]
    fn scalar_null_is_none() {
        assert_eq!(scalar_result(None), None);
    }

    #[test]
    fn array_uses_last_parentheses() {
        let input = StringArray::from(vec![
            Some("Institute A (foo) (1)"),
            Some("Institute B (2)"),
            Some("No parens"),
            None,
        ]);
        let result = map_originator_edmo_impl(&[ColumnarValue::Array(Arc::new(input))]).unwrap();

        let array = match result {
            ColumnarValue::Array(array) => array,
            other => panic!("unexpected result: {:?}", other),
        };
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(array.value(0), "1");
        assert_eq!(array.value(1), "2");
        assert!(array.is_null(2));
        assert!(array.is_null(3));
    }
}
