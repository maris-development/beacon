use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    common::HashMap,
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

lazy_static! {
    static ref WOD_FLAG_TO_SDN: HashMap<i64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0, "1");
        map.insert(1, "3");
        map.insert(2, "3");
        map.insert(3, "3");
        map.insert(4, "3");
        map.insert(5, "3");
        map.insert(6, "4");
        map.insert(7, "4");
        map.insert(8, "4");
        map.insert(9, "4");

        map
    };
}

pub fn map_wod_quality_flag() -> ScalarUDF {
    create_udf(
        "map_wod_quality_flag",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_wod_quality_flag_impl),
    )
}

fn map_wod_quality_flag_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|wod_flag| WOD_FLAG_TO_SDN.get(&wod_flag).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
            let sdn_flag = value
                .map(|wod_flag| WOD_FLAG_TO_SDN.get(&wod_flag).map(|s| s.to_string()))
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
