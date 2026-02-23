use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF};
use datafusion::prelude::create_udf;
use datafusion::scalar::ScalarValue;
use lru::LruCache;
use once_cell::sync::Lazy;

/// Cache: raw line -> parsed P01 token → L22 token
static P01_L22_CACHE: Lazy<Mutex<LruCache<String, HashMap<String, String>>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(128).unwrap())));

fn parse_p01_to_l22_tokens(line: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for entry in line.split('|') {
        let mut p01_token = None;
        let mut l22_token = None;

        for token in entry.split_whitespace() {
            if token.starts_with("SDN:P01::") {
                p01_token = Some(token.trim());
            } else if token.starts_with("SDN:L22::") {
                l22_token = Some(token.trim());
            }
        }

        if let (Some(p01), Some(l22)) = (p01_token, l22_token) {
            map.insert(p01.to_string(), l22.to_string());
        }
    }

    map
}

/// Lookup: full P01 token → full L22 token
fn get_l22_for_p01_token(line: &str, p01_token: &str) -> Option<String> {
    // Cache lookup
    if let Some(parsed) = {
        let mut cache = P01_L22_CACHE.lock().unwrap();
        cache.get(line).cloned()
    } {
        return parsed.get(p01_token).cloned();
    }

    // Parse + cache
    let parsed = parse_p01_to_l22_tokens(line);

    {
        let mut cache = P01_L22_CACHE.lock().unwrap();
        cache.put(line.to_string(), parsed.clone());
    }

    parsed.get(p01_token).cloned()
}

pub fn map_instrument_info_l22() -> ScalarUDF {
    create_udf(
        "map_instrument_info_l22",
        vec![
            datafusion::arrow::datatypes::DataType::Utf8,
            datafusion::arrow::datatypes::DataType::Utf8,
        ],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_instrument_info_l22_impl),
    )
}

fn map_instrument_info_l22_impl(
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
                        (Some(line), Some(p35)) => get_l22_for_p01_token(line, p35),
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
                (Some(line), Some(p35)) => get_l22_for_p01_token(line, p35),
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
                (Some(line), Some(p35)) => get_l22_for_p01_token(line, p35),
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
                (Some(line), Some(p35)) => get_l22_for_p01_token(line, p35),
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
