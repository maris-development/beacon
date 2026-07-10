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
            let float_array = crate::util::downcast_arg::<arrow::array::Float64Array>(
                pressure,
                "pressure_to_depth_teos_10",
            )?;
            let lat = crate::util::downcast_arg::<arrow::array::Float64Array>(
                latitude,
                "pressure_to_depth_teos_10",
            )?;

            let array = PrimitiveArray::<Float64Type>::from_iter(
                float_array
                    .iter()
                    .zip(lat.iter())
                    .map(|(p, l)| p.zip(l).map(|(p, l)| gsw_depth_from_pressure(p, l))),
            );

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Scalar(ScalarValue::Float64(pressure)), ColumnarValue::Array(latitude)) => {
            let lat = crate::util::downcast_arg::<arrow::array::Float64Array>(
                latitude,
                "pressure_to_depth_teos_10",
            )?;

            let array = PrimitiveArray::<Float64Type>::from_iter(lat.iter().map(|l| {
                l.zip(pressure.clone())
                    .map(|(l, p)| gsw_depth_from_pressure(p, l))
            }));

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Array(pressure), ColumnarValue::Scalar(ScalarValue::Float64(latitude))) => {
            let float_array = crate::util::downcast_arg::<arrow::array::Float64Array>(
                pressure,
                "pressure_to_depth_teos_10",
            )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array};

    #[test]
    fn zero_pressure_is_zero_depth() {
        assert!(gsw_depth_from_pressure(0.0, 0.0).abs() < 1e-9);
    }

    #[test]
    fn positive_pressure_gives_positive_depth_near_one_metre_per_dbar() {
        // ~100 dbar is roughly ~99 m of depth; assert it is positive and close.
        let depth = gsw_depth_from_pressure(100.0, 30.0);
        assert!(depth > 0.0, "depth should be positive, got {depth}");
        assert!(
            (depth - 99.0).abs() < 5.0,
            "depth out of expected range: {depth}"
        );
    }

    fn f64_array(out: ColumnarValue) -> Float64Array {
        match out {
            ColumnarValue::Array(arr) => {
                arr.as_any().downcast_ref::<Float64Array>().unwrap().clone()
            }
            other => panic!("expected array output, got {other:?}"),
        }
    }

    #[test]
    fn array_array_path() {
        let pressure = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(100.0),
            None,
        ])));
        let latitude = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(30.0),
            Some(30.0),
        ])));
        let arr = f64_array(pressure_to_depth_teos_10_impl(&[pressure, latitude]).unwrap());
        assert!(arr.value(0).abs() < 1e-9);
        assert!(arr.value(1) > 0.0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn scalar_pressure_array_latitude_path() {
        let pressure = ColumnarValue::Scalar(ScalarValue::Float64(Some(100.0)));
        let latitude = ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(30.0)])));
        let arr = f64_array(pressure_to_depth_teos_10_impl(&[pressure, latitude]).unwrap());
        assert!(arr.value(0) > 0.0);
    }

    #[test]
    fn array_pressure_scalar_latitude_path() {
        let pressure = ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(100.0)])));
        let latitude = ColumnarValue::Scalar(ScalarValue::Float64(Some(30.0)));
        let arr = f64_array(pressure_to_depth_teos_10_impl(&[pressure, latitude]).unwrap());
        assert!(arr.value(0) > 0.0);
    }

    #[test]
    fn scalar_scalar_path() {
        let pressure = ColumnarValue::Scalar(ScalarValue::Float64(Some(100.0)));
        let latitude = ColumnarValue::Scalar(ScalarValue::Float64(Some(30.0)));
        let out = pressure_to_depth_teos_10_impl(&[pressure, latitude]).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(d))) => assert!(d > 0.0),
            other => panic!("unexpected output: {other:?}"),
        }
    }
}
