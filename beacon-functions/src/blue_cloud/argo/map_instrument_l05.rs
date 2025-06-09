use std::sync::Arc;

use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_argo_instrument_l05() -> ScalarUDF {
    create_udf(
        "map_argo_instrument_l05",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_argo_instrument_l05_impl),
    )
}

fn map_argo_instrument_l05_impl(_: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        "SDN:L05::130".to_string(),
    ))));
}
