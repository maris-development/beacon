use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

static PARSE_CACHE: Lazy<Mutex<LruCache<String, Arc<HashMap<String, String>>>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(128).unwrap())));

fn parse_p35_to_p01(line: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for entry in line.split(',') {
        let parts: Vec<&str> = entry.split('=').collect();
        if parts.len() != 2 {
            continue;
        }

        let left = parts[0].trim();
        let p35 = left
            .split_whitespace()
            .find(|token| token.starts_with("SDN:P35::"))
            .map(|s| s.trim());

        let right = parts[1].trim();
        let right = right.trim_start_matches('[').trim_end_matches(']');

        let mut p01_value = None;
        for token in right.split_whitespace() {
            if token.starts_with("SDN:P01::") {
                p01_value = Some(token.trim());
                break;
            }
        }

        if let (Some(p35), Some(p01)) = (p35, p01_value) {
            map.insert(p35.to_string(), p01.to_string());
        }
    }

    map
}

/// Public lookup function with LRU caching
pub fn get_p01_for_p35(line: &str, target_p35: &str) -> Option<String> {
    // 1️⃣ Try cache
    if let Some(result) = {
        let mut cache = PARSE_CACHE.lock().unwrap();
        cache.get(line).cloned()
    } {
        return result.get(target_p35).cloned();
    }

    // 2️⃣ Parse and insert
    let parsed = parse_p35_to_p01(line);

    {
        let mut cache = PARSE_CACHE.lock().unwrap();
        cache.put(line.to_string(), Arc::new(parsed.clone()));
    }

    parsed.get(target_p35).cloned()
}

pub fn map_emodnet_chemistry_p35_contributor_codes_p01() -> ScalarUDF {
    create_udf(
        "map_emodnet_chemistry_p35_contributor_codes_p01",
        vec![
            datafusion::arrow::datatypes::DataType::Utf8,
            datafusion::arrow::datatypes::DataType::Utf8,
        ],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_p35_contributor_codes_p01_impl),
    )
}

fn map_p35_contributor_codes_p01_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match (&parameters[0], &parameters[1]) {
        (ColumnarValue::Array(line_array), ColumnarValue::Array(p35_array)) => {
            let line_array = line_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let p35_array = p35_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array =
                line_array
                    .iter()
                    .zip(p35_array.iter())
                    .map(|(line, p35)| match (line, p35) {
                        (Some(line), Some(p35)) => get_p01_for_p35(line, p35),
                        _ => None,
                    });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Array(line_array), ColumnarValue::Scalar(ScalarValue::Utf8(p35))) => {
            let line_array = line_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = line_array.iter().map(|line| match (line, p35.as_ref()) {
                (Some(line), Some(p35)) => get_p01_for_p35(line, p35),
                _ => None,
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Scalar(ScalarValue::Utf8(line)), ColumnarValue::Array(p35_array)) => {
            let p35_array = p35_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = p35_array.iter().map(|p35| match (line.as_ref(), p35) {
                (Some(line), Some(p35)) => get_p01_for_p35(line, p35),
                _ => None,
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(line)),
            ColumnarValue::Scalar(ScalarValue::Utf8(p35)),
        ) => {
            let result = match (line.as_ref(), p35.as_ref()) {
                (Some(line), Some(p35)) => get_p01_for_p35(line, p35),
                _ => None,
            };

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(result),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input types".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_p35_contributor_codes_p01() {
        let input = "SDN:P35::WATERTEMP = [ SDN:P01::TEMPPR01 SDN:P06::UPAA ], SDN:P35::EPC00001 = [ SDN:P01::PSLTZZ01 SDN:P06::UUUU ], SDN:P35::EPC00002 = [ SDN:P01::DOXYZZXX SDN:P06::UMLL | SDN:P01::DOXMZZXX SDN:P06::KGUM ]";

        assert_eq!(
            get_p01_for_p35(input, "SDN:P35::WATERTEMP"),
            Some("SDN:P01::TEMPPR01".to_string())
        );
        assert_eq!(
            get_p01_for_p35(input, "SDN:P35::EPC00001"),
            Some("SDN:P01::PSLTZZ01".to_string())
        );
    }
}
