use std::sync::Arc;

use arrow::{array::PrimitiveArray, datatypes::Float64Type};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_argo_instrument_l06() -> ScalarUDF {
    create_udf(
        "map_argo_instrument_l06",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_argo_instrument_l06_impl),
    )
}

fn map_argo_instrument_l06_impl(_: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        "SDN:L06::46".to_string(),
    ))));
}
