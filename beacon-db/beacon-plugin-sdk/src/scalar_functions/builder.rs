//! A helper that builds a scalar function from a closure.

use std::sync::Arc;

use std::fmt;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarUDF, Volatility, create_udf};
use datafusion::scalar::ScalarValue;

use super::hooks::{FunctionHooks, HookedUdf, Implied};

/// Starts a [`FunctionBuilder`] for a function with the given name.
pub fn function(name: &str) -> FunctionBuilder {
    FunctionBuilder {
        name: name.to_string(),
        args: Vec::new(),
        returns: None,
        aliases: Vec::new(),
        volatility: Volatility::Immutable,
        hooks: ClosureHooks::default(),
    }
}

type ImpliesFn = dyn Fn(&[Expr]) -> Result<Option<Implied>> + Send + Sync;
type BoundsFn = dyn Fn(&[&Interval]) -> Result<Option<Interval>> + Send + Sync;
type PropagateFn = dyn Fn(&Interval, &[&Interval]) -> Result<Option<Vec<Interval>>> + Send + Sync;

/// The hooks that a [`FunctionBuilder`] got as closures.
#[derive(Clone, Default)]
struct ClosureHooks {
    implies: Option<Arc<ImpliesFn>>,
    bounds: Option<Arc<BoundsFn>>,
    propagate: Option<Arc<PropagateFn>>,
}

impl ClosureHooks {
    fn is_empty(&self) -> bool {
        self.implies.is_none() && self.bounds.is_none() && self.propagate.is_none()
    }
}

impl fmt::Debug for ClosureHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClosureHooks")
            .field("implies", &self.implies.is_some())
            .field("bounds", &self.bounds.is_some())
            .field("propagate", &self.propagate.is_some())
            .finish()
    }
}

impl FunctionHooks for ClosureHooks {
    fn implied(&self, args: &[Expr], _ctx: &Arc<TaskContext>) -> Result<Option<Implied>> {
        self.implies.as_ref().map_or(Ok(None), |f| f(args))
    }

    fn bounds(&self, args: &[&Interval]) -> Result<Option<Interval>> {
        self.bounds.as_ref().map_or(Ok(None), |f| f(args))
    }

    fn propagate(&self, output: &Interval, args: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        self.propagate
            .as_ref()
            .map_or(Ok(None), |f| f(output, args))
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
    hooks: ClosureHooks,
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

    /// Marks the function as `Stable`: the result can change between queries, but not
    /// inside one query.
    pub fn stable(mut self) -> Self {
        self.volatility = Volatility::Stable;
        self
    }

    /// Marks the function as `Volatile`.
    pub fn volatile(mut self) -> Self {
        self.volatility = Volatility::Volatile;
        self
    }

    /// Sets the pushdown rule, see [`FunctionHooks::implied`].
    ///
    /// `f` gets the call arguments and returns the predicate that the call implies.
    pub fn implies<F>(mut self, f: F) -> Self
    where
        F: Fn(&[Expr]) -> Result<Option<Implied>> + Send + Sync + 'static,
    {
        self.hooks.implies = Some(Arc::new(f));
        self
    }

    /// Sets the output bounds hook, see [`FunctionHooks::bounds`].
    pub fn bounds<F>(mut self, f: F) -> Self
    where
        F: Fn(&[&Interval]) -> Result<Option<Interval>> + Send + Sync + 'static,
    {
        self.hooks.bounds = Some(Arc::new(f));
        self
    }

    /// Sets the constraint propagation hook, see [`FunctionHooks::propagate`].
    pub fn propagate<F>(mut self, f: F) -> Self
    where
        F: Fn(&Interval, &[&Interval]) -> Result<Option<Vec<Interval>>> + Send + Sync + 'static,
    {
        self.hooks.propagate = Some(Arc::new(f));
        self
    }

    /// Builds the function with `f` as its body. `f` gets arrays only.
    ///
    /// A scalar argument arrives as an array. When every argument is a scalar, as for
    /// `add_one(1.5)`, the function returns the first value of the result as a scalar.
    /// So a function without arguments gives one value for all rows. Use
    /// [`FunctionBuilder::invoke`] when that is not correct.
    ///
    /// # Panics
    ///
    /// Panics when [`FunctionBuilder::returns`] was not called.
    pub fn invoke_arrays<F>(self, f: F) -> ScalarUDF
    where
        F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync + 'static,
    {
        self.invoke(move |args| {
            let all_scalars = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
            let result = f(&ColumnarValue::values_to_arrays(args)?)?;
            if all_scalars {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            }
        })
    }

    /// Builds the function with `f` as its body. `f` gets the arguments as DataFusion
    /// passes them, so each argument is an array or a scalar.
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
        let udf = udf.with_aliases(aliases);
        if self.hooks.is_empty() {
            udf
        } else {
            ScalarUDF::from(HookedUdf::new(Arc::new(udf), Arc::new(self.hooks)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::exec_err;
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
                ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(v.map(|v| v + 1.0)),
                )),
                other => exec_err!("h_add_one expects Float64, got {other:?}"),
            })
    }

    fn add_arrays(name: &str) -> FunctionBuilder {
        function(name)
            .args([DataType::Float64, DataType::Float64])
            .returns(DataType::Float64)
    }

    fn sum() -> ScalarUDF {
        add_arrays("h_sum").invoke_arrays(|arrays| {
            let a = arrays[0].as_any().downcast_ref::<Float64Array>().unwrap();
            let b = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();
            let out: Float64Array = a.iter().zip(b.iter()).map(|(a, b)| Some(a? + b?)).collect();
            Ok(Arc::new(out))
        })
    }

    fn make_sum() -> Vec<ScalarUDF> {
        vec![sum()]
    }

    fn scalar(v: f64) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v)))
    }

    fn array(values: Vec<Option<f64>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Float64Array::from(values)))
    }

    fn sum_args(args: Vec<ColumnarValue>, number_rows: usize) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            arg_fields: args
                .iter()
                .map(|_| Arc::new(Field::new("a", DataType::Float64, true)))
                .collect(),
            args,
            number_rows,
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
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
        let stable = function("h_s")
            .returns(DataType::Float64)
            .stable()
            .invoke(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))));
        assert_eq!(stable.signature().volatility, Volatility::Stable);
    }

    #[test]
    fn invoke_handles_a_scalar_argument() {
        let mut args = call_args();
        args.args = vec![scalar(1.5)];
        args.number_rows = 1;
        let result = add_one().invoke_with_args(args).unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) if v == 2.5
        ));
    }

    #[test]
    fn invoke_arrays_returns_a_scalar_for_scalar_arguments() {
        let result = sum()
            .invoke_with_args(sum_args(vec![scalar(1.5), scalar(2.0)], 1))
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) if v == 3.5
        ));
    }

    #[test]
    fn invoke_arrays_expands_a_scalar_next_to_an_array() {
        let result = sum()
            .invoke_with_args(sum_args(
                vec![array(vec![Some(1.0), None, Some(3.0)]), scalar(10.0)],
                3,
            ))
            .unwrap();
        assert_eq!(values(result), vec![Some(11.0), None, Some(13.0)]);
    }

    #[test]
    fn invoke_arrays_crosses_the_ffi() {
        let udf = crate::scalar_functions::tests::foreign_udf_from(make_sum);
        let result = udf
            .invoke_with_args(sum_args(
                vec![array(vec![Some(1.0)]), array(vec![Some(2.0)])],
                1,
            ))
            .unwrap();
        assert_eq!(values(result), vec![Some(3.0)]);
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
        let udf = crate::scalar_functions::tests::foreign_udf_from(make_add_one);
        assert_eq!(udf.name(), "h_add_one");
        assert_eq!(udf.aliases(), ["h_inc".to_string()]);
        let result = udf.invoke_with_args(call_args()).unwrap();
        assert_eq!(values(result), vec![Some(2.5), None, Some(0.0)]);
    }

    #[test]
    fn a_panic_in_the_closure_is_an_error() {
        let udf = crate::scalar_functions::tests::foreign_udf_from(make_panicking);
        let err = udf.invoke_with_args(call_args()).unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
        assert!(err.to_string().contains("closure exploded"), "{err}");
    }
}
