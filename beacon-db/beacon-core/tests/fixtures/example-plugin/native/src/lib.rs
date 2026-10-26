//! Example Beacon plugin with scalar functions, two of them with planner hooks.

use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Float64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;
use beacon_plugin_sdk::datafusion::common::{Result, ScalarValue, exec_err};
use beacon_plugin_sdk::datafusion::logical_expr::interval_arithmetic::Interval;
use beacon_plugin_sdk::datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use beacon_plugin_sdk::scalar_functions::{FunctionBuilder, Implied, function};

/// `example_add_one(x)` returns `x + 1`. It uses the closure helper.
fn add_one() -> ScalarUDF {
    function("example_add_one")
        .args([DataType::Float64])
        .returns(DataType::Float64)
        .alias("example_inc")
        .invoke(|args| match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(value.map(|v| v + 1.0)),
            )),
            ColumnarValue::Array(array) => {
                let Some(values) = array.as_any().downcast_ref::<Float64Array>() else {
                    return exec_err!("example_add_one expects a Float64 argument");
                };
                let result: Float64Array = values.iter().map(|v| v.map(|v| v + 1.0)).collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => exec_err!("example_add_one expects a Float64 argument"),
        })
}

/// `example_panic()` always panics. It tests the panic guard.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Panic {
    signature: Signature,
}

impl Panic {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for Panic {
    fn name(&self) -> &str {
        "example_panic"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("example_panic always panics")
    }
}

/// The box side of arguments `low` and `high`.
fn box_side(args: &[&Interval], low: usize, high: usize) -> Result<Interval> {
    Interval::try_new(args[low].lower().clone(), args[high].upper().clone())
}

/// `lon` and `lat` are in the box `[min_lon, max_lon] x [min_lat, max_lat]`.
fn in_box_body(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let mut columns = Vec::new();
    for array in arrays {
        let Some(values) = array.as_any().downcast_ref::<Float64Array>() else {
            return exec_err!("example_in_box expects Float64 arguments");
        };
        columns.push(values);
    }
    let result: BooleanArray = (0..arrays[0].len())
        .map(|row| {
            let v = |i: usize| columns[i].value(row);
            Some(v(0) >= v(2) && v(0) <= v(4) && v(1) >= v(3) && v(1) <= v(5))
        })
        .collect();
    Ok(Arc::new(result))
}

fn box_function(name: &str) -> FunctionBuilder {
    function(name)
        .args(vec![DataType::Float64; 6])
        .returns(DataType::Boolean)
}

/// `example_in_box(lon, lat, min_lon, min_lat, max_lon, max_lat)` with all three hooks.
fn in_box() -> ScalarUDF {
    box_function("example_in_box")
        .implies(|args| {
            let lon = args[0].clone().between(args[2].clone(), args[4].clone());
            let lat = args[1].clone().between(args[3].clone(), args[5].clone());
            Ok(Some(Implied::Necessary(lon.and(lat))))
        })
        .bounds(|_| Ok(Some(Interval::TRUE_OR_FALSE)))
        .propagate(|output, args| {
            if output != &Interval::TRUE {
                return Ok(None);
            }
            let (Some(lon), Some(lat)) = (
                args[0].intersect(box_side(args, 2, 4)?)?,
                args[1].intersect(box_side(args, 3, 5)?)?,
            ) else {
                return Ok(None);
            };
            let mut result = vec![lon, lat];
            result.extend(args[2..].iter().map(|a| (*a).clone()));
            Ok(Some(result))
        })
        .invoke_arrays(in_box_body)
}

/// `example_bad_box(...)` works like `example_in_box`, but its hooks panic.
fn bad_box() -> ScalarUDF {
    box_function("example_bad_box")
        .implies(|_| panic!("example_bad_box implies panics"))
        .propagate(|_, _| panic!("example_bad_box propagate panics"))
        .invoke_arrays(in_box_body)
}

fn functions() -> Vec<ScalarUDF> {
    vec![
        add_one(),
        ScalarUDF::from(Panic::new()),
        in_box(),
        bad_box(),
    ]
}

#[cfg(feature = "wrong-abi")]
beacon_plugin_sdk::export_plugin!(@abi beacon_plugin_sdk::BEACON_PLUGIN_SDK_ABI, 0, scalar = functions);

#[cfg(not(feature = "wrong-abi"))]
beacon_plugin_sdk::export_plugin!(scalar = functions);
