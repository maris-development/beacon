use std::sync::Arc;

use arrow::{array::PrimitiveArray, datatypes::Float64Type};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn pressure_to_depth_teos_10() -> ScalarUDF {
    create_udf(
        "pressure_to_depth_teos_10",
        vec![
            datafusion::arrow::datatypes::DataType::Float64,
            datafusion::arrow::datatypes::DataType::Float64,
        ],
        datafusion::arrow::datatypes::DataType::Float64,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(pressure_to_depth_teos_10_impl),
    )
}

fn pressure_to_depth_teos_10_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    //Should accept 1 parameter that is a float
    match (&parameters[0], &parameters[1]) {
        (ColumnarValue::Array(pressure), ColumnarValue::Array(latitude)) => {
            let float_array = pressure
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            let lat = latitude
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            let array = PrimitiveArray::<Float64Type>::from_iter(
                float_array
                    .iter()
                    .zip(lat.iter())
                    .map(|(p, l)| p.zip(l).map(|(p, l)| gsw_depth_from_pressure(p, l))),
            );

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Scalar(ScalarValue::Float64(pressure)), ColumnarValue::Array(latitude)) => {
            let lat = latitude
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            let array = PrimitiveArray::<Float64Type>::from_iter(lat.iter().map(|l| {
                l.zip(pressure.clone())
                    .map(|(l, p)| gsw_depth_from_pressure(p, l))
            }));

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Array(pressure), ColumnarValue::Scalar(ScalarValue::Float64(latitude))) => {
            let float_array = pressure
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            let array = PrimitiveArray::<Float64Type>::from_iter(float_array.iter().map(|p| {
                p.zip(latitude.clone())
                    .map(|(p, l)| gsw_depth_from_pressure(p, l))
            }));

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Float64(pressure)),
            ColumnarValue::Scalar(ScalarValue::Float64(latitude)),
        ) => {
            let value = pressure
                .zip(latitude.clone())
                .map(|(p, l)| gsw_depth_from_pressure(p, l));

            Ok(ColumnarValue::Scalar(ScalarValue::Float64(value)))
        }
        _ => Err(datafusion::error::DataFusionError::Internal(
            "Invalid input types".to_string(),
        )),
    }
}

fn gsw_depth_from_pressure(pressure: f64, latitude: f64) -> f64 {
    -gsw::conversions::z_from_p(pressure, latitude, 0.0, 0.0)
}
