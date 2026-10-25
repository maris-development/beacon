//! Example Beacon plugin with two scalar functions.

use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{Array, Float64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;
use beacon_plugin_sdk::datafusion::common::{Result, ScalarValue, exec_err};
use beacon_plugin_sdk::datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use beacon_plugin_sdk::function;

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

fn functions() -> Vec<ScalarUDF> {
    vec![add_one(), ScalarUDF::from(Panic::new())]
}

#[cfg(feature = "wrong-abi")]
beacon_plugin_sdk::export_plugin!(@abi 1, 0, functions);

#[cfg(not(feature = "wrong-abi"))]
beacon_plugin_sdk::export_plugin!(functions);
