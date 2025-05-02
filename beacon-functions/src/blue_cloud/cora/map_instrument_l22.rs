use std::{collections::HashMap, sync::Arc};

use crate::blue_cloud::util;
use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const L22_MAPPINGS_CSV: &[u8] = include_bytes!("l22.csv");

lazy_static! {
    static ref L22_MAP: HashMap<String, String> =
        util::read_mappings_from_reader(L22_MAPPINGS_CSV, "L22").unwrap_or_default();
}

pub fn map_cora_instrument_l22() -> ScalarUDF {
    create_udf(
        "map_cora_instrument_l22",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cora_instrument_l22_impl),
    )
}

fn map_cora_instrument_l22_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|wmo_code| L22_MAP.get(wmo_code).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .map(|wmo_code| L22_MAP.get(wmo_code).map(|s| s.to_string()))
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
