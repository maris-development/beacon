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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_returns_the_constant_l05_code() {
        // The mapping is a constant, so it ignores its inputs entirely.
        let out = map_argo_instrument_l05_impl(&[]).unwrap();
        assert!(matches!(
            out,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(ref s))) if s == "SDN:L05::130"
        ));
    }
}
