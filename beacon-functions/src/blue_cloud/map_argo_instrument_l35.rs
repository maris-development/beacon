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
    static ref L35_MAP: HashMap<i64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(834, "SDN:L35::MAN0013");
        map.insert(835, "SDN:L35::MAN0002");
        map.insert(836, "SDN:L35::MAN0002");
        map.insert(837, "SDN:L35::MAN0013");
        map.insert(838, "SDN:L35::MAN0013");
        map.insert(841, "SDN:L35::MAN0013");
        map.insert(844, "SDN:L35::MAN0013");
        map.insert(846, "SDN:L35::MAN0013");
        map.insert(860, "SDN:L35::MAN0013");
        map.insert(865, "SDN:L35::MAN0013");
        map.insert(878, "SDN:L35::MAN0049");

        map
    };
}

pub fn map_argo_instrument_l35() -> ScalarUDF {
    create_udf(
        "map_argo_instrument_l35",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_argo_instrument_l35_impl),
    )
}

fn map_argo_instrument_l35_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|wmo_code| L35_MAP.get(&wmo_code).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
            let sdn_flag = value
                .map(|wmo_code| L35_MAP.get(&wmo_code).map(|s| s.to_string()))
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
