use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{PrimitiveArray, StringArray},
    datatypes::Float64Type,
};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

lazy_static! {
    static ref L06_MAP: HashMap<i64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(835, "SDN:L06::46");
        map.insert(836, "SDN:L06::46");
        map.insert(837, "SDN:L06::46");
        map.insert(838, "SDN:L06::46");
        map.insert(841, "SDN:L06::46");
        map.insert(844, "SDN:L06::46");
        map.insert(845, "SDN:L06::46");
        map.insert(846, "SDN:L06::46");
        map.insert(849, "SDN:L06::46");
        map.insert(851, "SDN:L06::46");
        map.insert(853, "SDN:L06::46");
        map.insert(854, "SDN:L06::46");
        map.insert(860, "SDN:L06::46");
        map.insert(862, "SDN:L06::46");
        map.insert(863, "SDN:L06::46");
        map.insert(864, "SDN:L06::46");
        map.insert(865, "SDN:L06::46");
        map.insert(869, "SDN:L06::46");
        map.insert(870, "SDN:L06::46");
        map.insert(872, "SDN:L06::46");
        map.insert(873, "SDN:L06::46");
        map.insert(874, "SDN:L06::46");
        map.insert(877, "SDN:L06::46");
        map.insert(995, "SDN:L06::46");

        map
    };
}

pub fn map_cora_instrument_l06() -> ScalarUDF {
    create_udf(
        "map_cora_instrument_l06",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cora_instrument_l06_impl),
    )
}

fn map_cora_instrument_l06_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|wmo_code| L06_MAP.get(&wmo_code).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
            let sdn_flag = value
                .map(|wmo_code| L06_MAP.get(&wmo_code).map(|s| s.to_string()))
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
