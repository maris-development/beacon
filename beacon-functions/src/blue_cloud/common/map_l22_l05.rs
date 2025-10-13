use std::{collections::HashMap, sync::Arc};

use crate::blue_cloud::util;
use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const L22_L05_MAPPINGS_CSV: &[u8] = include_bytes!("l22_l05.csv");

lazy_static! {
    static ref L22_L05_MAP: HashMap<String, String> =
        util::read_from_to_mappings_from_reader(L22_L05_MAPPINGS_CSV, "L22", "L05").unwrap();
}

pub fn map_l22_l05() -> ScalarUDF {
    create_udf(
        "map_l22_l05",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_l22_l05_impl),
    )
}

fn map_l22_l05_impl(parameters: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array
                .iter()
                .map(|flag| flag.and_then(|value| L22_L05_MAP.get(value).cloned()));

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .and_then(|value| L22_L05_MAP.get(value.as_str()).map(|s| s.to_string()));

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

    #[test]
    fn test_loading_l22_l05_mappings() {
        let mappings = L22_L05_MAP.clone();
        assert!(
            !mappings.is_empty(),
            "L22 to L05 mappings should not be empty"
        );
    }
}
