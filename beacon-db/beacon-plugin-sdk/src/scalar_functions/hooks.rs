//! Optional planner hooks of a scalar function.
//!
//! DataFusion cannot see into a plugin function, so a filter on it prunes no files.
//! A function can give Beacon three hooks:
//!
//! - [`FunctionHooks::implied`]: a plain predicate that the call implies.
//! - [`FunctionHooks::bounds`]: the output interval for the argument intervals.
//! - [`FunctionHooks::propagate`]: the argument intervals for an output interval.
//!
//! Every hook is optional. A hook that returns `Ok(None)`, an error or a panic gives
//! no answer, and Beacon plans the query as if the hook did not exist.

use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};

use crate::panic_safe::panic_message;

/// A predicate that a function call implies.
#[derive(Debug, Clone, PartialEq)]
pub enum Implied {
    /// The call is true only when `expr` is true. Beacon adds `expr` next to the call.
    ///
    /// The rule must be correct: a row where the call is true and `expr` is not
    /// true disappears from the result.
    Necessary(Expr),
    /// The call has the same value as `expr`. Beacon replaces the call with `expr`.
    ///
    /// Beacon trusts this rule and does not check it.
    Exact(Expr),
}

/// The optional planner hooks of one scalar function.
///
/// Each method has a default that gives no answer, so implement only the hooks you need.
pub trait FunctionHooks: Debug + Send + Sync {
    /// The predicate that the call `f(args)` implies, or `None`.
    ///
    /// Beacon folds a constant argument, such as `CAST(2 AS DOUBLE)`, to a literal first.
    /// It also applies the `Exact` rules of calls inside the arguments first.
    ///
    /// `ctx` has every function that Beacon knows, also the functions of other plugins.
    /// The arguments and the result can call any of them. A function from another
    /// library is an FFI wrapper, so find it by its name and not with a downcast.
    fn implied(&self, _args: &[Expr], _ctx: &Arc<TaskContext>) -> Result<Option<Implied>> {
        Ok(None)
    }

    /// The output interval for the argument intervals, or `None`.
    ///
    /// The result must hold every value that the function can return for these arguments.
    fn bounds(&self, _args: &[&Interval]) -> Result<Option<Interval>> {
        Ok(None)
    }

    /// The argument intervals when the output is in `output`, or `None`.
    ///
    /// The result has one interval per argument. Each interval must hold every
    /// argument value that can give an output in `output`.
    fn propagate(&self, _output: &Interval, _args: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        Ok(None)
    }
}

/// A [`ScalarUDFImpl`] that adds [`FunctionHooks`] to an inner UDF.
///
/// DataFusion calls `evaluate_bounds` and `propagate_constraints` for interval
/// analysis, and this UDF answers them with the hooks. Beacon finds the hooks
/// of a function with [`hooks_of`].
#[derive(Debug)]
pub struct HookedUdf {
    inner: Arc<ScalarUDF>,
    hooks: Arc<dyn FunctionHooks>,
}

impl HookedUdf {
    /// Adds `hooks` to `inner`.
    pub fn new(inner: Arc<ScalarUDF>, hooks: Arc<dyn FunctionHooks>) -> Self {
        Self { inner, hooks }
    }

    /// Returns the wrapped UDF.
    pub fn inner(&self) -> &Arc<ScalarUDF> {
        &self.inner
    }

    /// Returns the hooks.
    pub fn hooks(&self) -> &Arc<dyn FunctionHooks> {
        &self.hooks
    }

    fn imp(&self) -> &dyn ScalarUDFImpl {
        self.inner.inner().as_ref()
    }

    fn guard<T>(&self, hook: &str, call: impl FnOnce() -> Result<Option<T>>) -> Result<Option<T>> {
        catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|payload| {
            Err(DataFusionError::Execution(format!(
                "{hook} hook of function '{}' panicked: {}",
                self.inner.name(),
                panic_message(payload.as_ref())
            )))
        })
    }

    /// Calls [`FunctionHooks::implied`]. A panic becomes an error.
    pub fn implied(&self, args: &[Expr], ctx: &Arc<TaskContext>) -> Result<Option<Implied>> {
        self.guard("implied", || self.hooks.implied(args, ctx))
    }

    /// Calls [`FunctionHooks::bounds`]. A panic becomes an error.
    pub fn bounds(&self, args: &[&Interval]) -> Result<Option<Interval>> {
        self.guard("bounds", || self.hooks.bounds(args))
    }

    /// Calls [`FunctionHooks::propagate`]. A panic becomes an error.
    pub fn propagate(
        &self,
        output: &Interval,
        args: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let result = self.guard("propagate", || self.hooks.propagate(output, args))?;
        // A wrong number of intervals gives no answer.
        Ok(result.filter(|intervals| intervals.len() == args.len()))
    }
}

/// Returns the hooked UDF of `udf`, or `None` when the function has no hooks.
pub fn hooks_of(udf: &ScalarUDF) -> Option<&HookedUdf> {
    let imp: &dyn Any = udf.inner().as_ref();
    imp.downcast_ref::<HookedUdf>()
}

impl PartialEq for HookedUdf {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
            && std::ptr::addr_eq(Arc::as_ptr(&self.hooks), Arc::as_ptr(&other.hooks))
    }
}

impl Eq for HookedUdf {}

impl Hash for HookedUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl ScalarUDFImpl for HookedUdf {
    fn name(&self) -> &str {
        self.imp().name()
    }

    fn aliases(&self) -> &[String] {
        self.imp().aliases()
    }

    #[allow(deprecated)]
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        self.imp().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.imp().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.imp().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.imp().return_type(arg_types)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        // Keep the hooks on the reconfigured UDF.
        self.imp()
            .with_updated_config(config)
            .map(|udf| ScalarUDF::from(HookedUdf::new(Arc::new(udf), Arc::clone(&self.hooks))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.imp().return_field_from_args(args)
    }

    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.imp().is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.imp().invoke_with_args(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.imp().simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.imp().preimage(args, lit_expr, info)
    }

    fn short_circuits(&self) -> bool {
        self.imp().short_circuits()
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.imp().conditional_arguments(args)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        match self.bounds(input) {
            Ok(Some(interval)) => Ok(interval),
            _ => self.imp().evaluate_bounds(input),
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        match self.propagate(interval, inputs) {
            Ok(Some(intervals)) => Ok(Some(intervals)),
            _ => self.imp().propagate_constraints(interval, inputs),
        }
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.imp().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.imp().preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.imp().coerce_types(arg_types)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.imp().struct_field_mapping(literal_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.imp().documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.imp().placement(args)
    }
}
