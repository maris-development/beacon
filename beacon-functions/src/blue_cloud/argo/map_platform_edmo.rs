use std::{collections::HashMap, sync::Arc};

use crate::blue_cloud::util;
use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const ARGO_PLATFORM_EDMO: &[u8] = include_bytes!("argo_platform_edmo.csv");

lazy_static! {
    static ref ARGO_PLATFORM_EDMO_MAP: HashMap<String, String> =
        util::read_mappings_from_reader(ARGO_PLATFORM_EDMO, "EDMO_CODE").unwrap();
}

pub fn map_argo_platform_edmo() -> ScalarUDF {
    create_udf(
        "map_argo_platform_edmo",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_argo_platform_edmo_impl),
    )
}

fn map_argo_platform_edmo_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = flag
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let array = flag_array.iter().map(|flag| {
                flag.map(|value| ARGO_PLATFORM_EDMO_MAP.get(value).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value
                .as_ref()
                .map(|value| {
                    ARGO_PLATFORM_EDMO_MAP
                        .get(value.as_str())
                        .map(|s| s.to_string())
                })
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
    fn test_loading_argo_platform_edmo_mappings() {
        let mappings = ARGO_PLATFORM_EDMO_MAP.clone();
        assert!(
            !mappings.is_empty(),
            "ARGO_PLATFORM_EDMO mappings should not be empty"
        );
    }
}
