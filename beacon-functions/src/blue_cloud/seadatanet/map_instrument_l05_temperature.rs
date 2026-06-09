use std::collections::HashSet;

use lazy_static::lazy_static;

lazy_static! {
    /// Fetched from: https://vocab.nerc.ac.uk/collection/P25/current/WTEMP/
    static ref L05_CODES: HashSet<&'static str> = {
        let mut m = HashSet::new();

        m.insert("308");
        m.insert("132");
        m.insert("130");
        m.insert("131");
        m.insert("354");
        m.insert("MOD07");
        m.insert("MOD02");
        m.insert("135");
        m.insert("133");
        m.insert("302");
        m.insert("134");

        m
    };
}

use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_seadatanet_instrument_l05_temperature() -> ScalarUDF {
    create_udf(
        "map_seadatanet_instrument_l05_temperature",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_seadatanet_instrument_l05_temperature_impl),
    )
}

fn map_seadatanet_instrument_l05_temperature_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    fn map_value(s: &str) -> Option<String> {
        map_until_first(&unpack_values(s))
    }

    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array
                .iter()
                .map(|flag| flag.and_then(|value| map_value(value)));

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value.as_ref().and_then(|value| map_value(value));

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}

fn unpack_values(s: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0;
    while let Some(open) = s[start..].find('(') {
        if let Some(close) = s[start + open..].find(')') {
            let value = &s[start + open + 1..start + open + close];
            values.push(value.to_string());
            start += open + close + 1;
        } else {
            break;
        }
    }
    values
}

fn map_until_first(values: &[String]) -> Option<String> {
    for value in values {
        if L05_CODES.contains(value.as_str()) {
            return Some(format!("SDN:L05::{}", value));
        }
    }
    match values.first() {
        Some(val) => Some(format!("SDN:L05::{}", val)),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_simple() {
        let input = "some text (130) some more text (131)";
        let values = unpack_values(input);
        assert_eq!(values, vec!["130".to_string(), "131".to_string()]);

        let mapped = map_until_first(&values);
        assert_eq!(mapped, Some("SDN:L05::130".to_string()));
    }

    #[test]
    fn test_fallback_first_value() {
        // None of the values are temperature L05 codes, so the first value is used.
        let input = "some sensor(999)|other sensor(888)";
        let values = unpack_values(input);
        assert_eq!(values, vec!["999".to_string(), "888".to_string()]);

        let mapped = map_until_first(&values);
        assert_eq!(mapped, Some("SDN:L05::999".to_string()));
    }

    #[test]
    fn test_impl_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "water temperature sensor(134)|salinity sensor(350)|CTD(130)".to_string(),
        )));

        let result = map_seadatanet_instrument_l05_temperature_impl(&[input]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                assert_eq!(value, Some("SDN:L05::134".to_string()));
            }
            _ => panic!("expected Utf8 scalar"),
        }
    }

    #[test]
    fn test_impl_array() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("water temperature sensor(134)|CTD(130)"),
            Some("no codes here"),
            None,
        ])));

        let result = map_seadatanet_instrument_l05_temperature_impl(&[input]).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(array.value(0), "SDN:L05::134");
                // No parenthesized values, so nothing is mapped.
                assert!(array.is_null(1));
                assert!(array.is_null(2));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_example() {
        let input = "water temperature sensor(134)|salinity sensor(350)|dissolved gas sensors(351)|CTD(130)|water pressure sensors(WPS)";
        let values = unpack_values(input);
        assert_eq!(
            values,
            vec![
                "134".to_string(),
                "350".to_string(),
                "351".to_string(),
                "130".to_string(),
                "WPS".to_string()
            ]
        );

        let mapped = map_until_first(&values);
        assert_eq!(mapped, Some("SDN:L05::134".to_string()));
    }
}
