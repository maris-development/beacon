//! A helper that builds a scalar function from a closure.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};

/// Starts a [`FunctionBuilder`] for a function with the given name.
pub fn function(name: &str) -> FunctionBuilder {
    FunctionBuilder {
        name: name.to_string(),
        args: Vec::new(),
        returns: None,
        aliases: Vec::new(),
        volatility: Volatility::Immutable,
    }
}

/// Builds a [`ScalarUDF`] from a name, exact argument types, a return type and a closure.
///
/// The function is `Immutable` unless you call [`FunctionBuilder::volatile`].
#[derive(Debug, Clone)]
pub struct FunctionBuilder {
    name: String,
    args: Vec<DataType>,
    returns: Option<DataType>,
    aliases: Vec<String>,
    volatility: Volatility,
}

impl FunctionBuilder {
    /// Sets the exact argument types.
    pub fn args(mut self, types: impl IntoIterator<Item = DataType>) -> Self {
        self.args = types.into_iter().collect();
        self
    }

    /// Sets the return type. `invoke` needs it.
    pub fn returns(mut self, t: DataType) -> Self {
        self.returns = Some(t);
        self
    }

    /// Adds an alias name.
    pub fn alias(mut self, name: &str) -> Self {
        self.aliases.push(name.to_string());
        self
    }

    /// Marks the function as `Volatile`.
    pub fn volatile(mut self) -> Self {
        self.volatility = Volatility::Volatile;
        self
    }

    /// Builds the function with `f` as its body.
    ///
    /// # Panics
    ///
    /// Panics when [`FunctionBuilder::returns`] was not called.
    pub fn invoke<F>(self, f: F) -> ScalarUDF
    where
        F: Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync + 'static,
    {
        let Some(returns) = self.returns else {
            panic!(
                "function '{}' has no return type: call returns()",
                self.name
            );
        };
        let udf = create_udf(&self.name, self.args, returns, self.volatility, Arc::new(f));
        // DataFusion needs static alias names. The leak is small and happens once at init.
        let aliases = self
            .aliases
            .into_iter()
            .map(|a| &*Box::leak(a.into_boxed_str()));
        udf.with_aliases(aliases)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{ScalarFunctionArgs, TypeSignature};
    use datafusion::scalar::ScalarValue;

    use super::*;

    fn add_one() -> ScalarUDF {
        function("h_add_one")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .alias("h_inc")
            .invoke(|args| match &args[0] {
                ColumnarValue::Array(array) => {
                    let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    let out: Float64Array = values.iter().map(|v| v.map(|v| v + 1.0)).collect();
                    Ok(ColumnarValue::Array(Arc::new(out)))
                }
                other => Ok(other.clone()),
            })
    }

    fn panicking() -> ScalarUDF {
        function("h_boom")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .invoke(|_| panic!("closure exploded"))
    }

    fn make_add_one() -> Vec<ScalarUDF> {
        vec![add_one()]
    }

    fn make_panicking() -> Vec<ScalarUDF> {
        vec![panicking()]
    }

    fn call_args() -> ScalarFunctionArgs {
        let array = Float64Array::from(vec![Some(1.5), None, Some(-1.0)]);
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(array))],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Float64, true))],
            number_rows: 3,
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn values(value: ColumnarValue) -> Vec<Option<f64>> {
        let ColumnarValue::Array(array) = value else {
            panic!("expected an array");
        };
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        array.iter().collect()
    }

    #[test]
    fn builds_a_function_with_name_args_and_aliases() {
        let udf = add_one();
        assert_eq!(udf.name(), "h_add_one");
        assert_eq!(udf.aliases(), ["h_inc".to_string()]);
        assert_eq!(
            udf.signature().type_signature,
            TypeSignature::Exact(vec![DataType::Float64])
        );
        assert_eq!(
            udf.return_type(&[DataType::Float64]).unwrap(),
            DataType::Float64
        );
        assert_eq!(udf.signature().volatility, Volatility::Immutable);
        let volatile = function("h_v")
            .returns(DataType::Float64)
            .volatile()
            .invoke(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))));
        assert_eq!(volatile.signature().volatility, Volatility::Volatile);
    }

    #[test]
    #[should_panic(expected = "no return type")]
    fn invoke_without_a_return_type_panics() {
        let _ = function("h_none").invoke(|args| Ok(args[0].clone()));
    }

    #[test]
    fn invoke_runs_the_closure() {
        let result = add_one().invoke_with_args(call_args()).unwrap();
        assert_eq!(values(result), vec![Some(2.5), None, Some(0.0)]);
    }

    #[test]
    fn a_helper_function_crosses_the_ffi() {
        let udf = crate::tests::foreign_udf_from(make_add_one);
        assert_eq!(udf.name(), "h_add_one");
        assert_eq!(udf.aliases(), ["h_inc".to_string()]);
        let result = udf.invoke_with_args(call_args()).unwrap();
        assert_eq!(values(result), vec![Some(2.5), None, Some(0.0)]);
    }

    #[test]
    fn a_panic_in_the_closure_is_an_error() {
        let udf = crate::tests::foreign_udf_from(make_panicking);
        let err = udf.invoke_with_args(call_args()).unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
        assert!(err.to_string().contains("closure exploded"), "{err}");
    }
}
