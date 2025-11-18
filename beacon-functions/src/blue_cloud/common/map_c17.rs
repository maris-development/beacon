use std::{collections::HashMap, sync::Arc};

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const C17_MAPPINGS_JSON: &[u8] = include_bytes!("c17_codes.json");

lazy_static! {
    static ref C17_MAP: HashMap<String, String> = {
        let mappings: HashMap<String, String> =
            serde_json::from_slice(C17_MAPPINGS_JSON).expect("Failed to parse C17 mappings");
        mappings
    };
}

pub fn map_c17() -> ScalarUDF {
    create_udf(
        "map_c17",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_c17_impl),
    )
}

fn map_c17_impl(parameters: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(value) => {
            let string_array = value
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let c17 = string_array
                .iter()
                .map(|flag| flag.and_then(|value| C17_MAP.get(value).cloned()));

            let array = StringArray::from_iter(c17);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let c17 = value
                .as_ref()
                .and_then(|value| C17_MAP.get(value.as_str()).map(|s| s.to_string()));

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(c17),
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

    #[test]
    fn test_loading_c17_mappings() {
        let mappings = C17_MAP.clone();
        assert!(!mappings.is_empty(), "C17 mappings should not be empty");
        println!("Loaded {:?} C17 mappings", mappings.len());
    }
}
