use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_cmems_bigram_l05() -> ScalarUDF {
    create_udf(
        "map_cmems_bigram_l05",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cmems_bigram_l05_impl),
    )
}

fn map_cmems_bigram_l05_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    if parameters.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "map_cmems_bigram_l05 function requires 1 parameter. The Bigram".to_string(),
        ));
    }

    match &parameters[0] {
        ColumnarValue::Array(array) => {
            let bigram_array = array.as_any().downcast_ref::<StringArray>().unwrap();

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array.iter().map(|bigram| map_bigram_l05(bigram)),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(bigram)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(map_bigram_l05(bigram.as_deref()).map(|s| s.to_string())),
        )),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "map_cora_platform_l06 function requires 1 parameter. The Bigram".to_string(),
        )),
    }
}

fn map_bigram_l05(platform_bigram: Option<&str>) -> Option<&'static str> {
    match platform_bigram {
        Some("BO") => Some("SDN:L05::30"),
        Some("CT") => Some("SDN:L05::130"),
        Some("XB") => Some("SDN:L05::132"),
        Some("TX") => Some("SDN:L05::135"),
        Some("TS") => Some("SDN:L05::133"),
        Some("ML") => Some("SDN:L05::134"),
        Some("SF") => Some("SDN:L05::131"),
        _ => None,
    }
}
