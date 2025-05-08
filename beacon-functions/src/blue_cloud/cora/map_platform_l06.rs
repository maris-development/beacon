use std::{collections::HashMap, sync::Arc};

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

lazy_static! {
    static ref L06_MAP: HashMap<String, String> = {
        let mut map = HashMap::new();
        map.insert("FB".to_string(), "SDN:L06::35".to_string());
        map.insert("MO".to_string(), "SDN:L06::48".to_string());
        map.insert("PF".to_string(), "SDN:L06::46".to_string());
        map.insert("SM".to_string(), "SDN:L06::70".to_string());
        map.insert("XX".to_string(), "SDN:L06::0".to_string());

        map
    };
}

pub fn map_cora_platform_l06() -> ScalarUDF {
    create_udf(
        "map_cora_platform_l06",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cora_platform_l06_impl),
    )
}

fn map_cora_platform_l06_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|value| L06_MAP.get(value).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .map(|value| L06_MAP.get(value.as_str()).map(|s| s.to_string()))
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
