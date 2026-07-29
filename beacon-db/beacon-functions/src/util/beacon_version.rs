use std::sync::Arc;

use datafusion::{logical_expr::ScalarUDF, prelude::create_udf, scalar::ScalarValue};

pub fn beacon_version() -> ScalarUDF {
    create_udf(
        "beacon_version",
        vec![],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(beacon_version_impl),
    )
}

fn beacon_version_impl(
    _parameters: &[datafusion::logical_expr::ColumnarValue],
) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
    Ok(datafusion::logical_expr::ColumnarValue::Scalar(
        ScalarValue::Utf8(Some(env!("CARGO_PKG_VERSION").to_string())),
    ))
}
