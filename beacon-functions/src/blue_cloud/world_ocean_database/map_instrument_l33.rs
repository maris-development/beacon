use std::{collections::HashMap, sync::Arc};

use crate::blue_cloud::util;
use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const L33_MAPPINGS_CSV: &[u8] = include_bytes!("l33.csv");

lazy_static! {
    static ref L33_MAP: HashMap<String, String> =
        util::read_mappings_from_reader(L33_MAPPINGS_CSV, "L33").unwrap();
}

pub fn map_wod_instrument_l33() -> ScalarUDF {
    create_udf(
        "map_wod_instrument_l33",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_wod_instrument_l33_impl),
    )
}

fn map_wod_instrument_l33_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|value| L33_MAP.get(value).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .map(|value| L33_MAP.get(value.as_str()).map(|s| s.to_string()))
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

    #[test]
    fn test_loading_l33_mappings() {
        let mappings = L33_MAP.clone();
        assert!(!mappings.is_empty());
    }
}
