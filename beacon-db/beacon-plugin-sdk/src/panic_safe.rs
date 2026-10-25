use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};

/// A [`ScalarUDFImpl`] that delegates to an inner UDF and turns a panic into an error.
///
/// A panic must not unwind across the `extern "C"` boundary to Beacon.
/// The calls that Beacon makes through the FFI catch the panic and return
/// [`DataFusionError::Execution`] instead.
///
/// The getters (`name`, `aliases`, `signature`, `short_circuits`) and `Drop` of a plugin UDF
/// must not panic: the FFI clone and release wrappers call them without a guard.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PanicSafeUdf {
    inner: Arc<ScalarUDF>,
}

impl PanicSafeUdf {
    /// Wraps `inner` in a panic-safe UDF.
    pub fn new(inner: Arc<ScalarUDF>) -> Self {
        Self { inner }
    }

    /// Returns the wrapped UDF.
    pub fn inner(&self) -> &Arc<ScalarUDF> {
        &self.inner
    }

    fn imp(&self) -> &dyn ScalarUDFImpl {
        self.inner.inner().as_ref()
    }

    fn guard<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
        catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|payload| {
            Err(DataFusionError::Execution(format!(
                "plugin function '{}' panicked: {}",
                self.inner.name(),
                panic_message(payload.as_ref())
            )))
        })
    }
}

/// Returns the message of a panic payload.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

impl ScalarUDFImpl for PanicSafeUdf {
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
        self.guard(|| self.imp().return_type(arg_types))
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        // Keep the panic guard on the reconfigured UDF.
        self.imp()
            .with_updated_config(config)
            .map(|udf| ScalarUDF::from(PanicSafeUdf::new(Arc::new(udf))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.guard(|| self.imp().return_field_from_args(args))
    }

    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.imp().is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.guard(|| self.imp().invoke_with_args(args))
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
        self.imp().evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.imp().propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.imp().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.imp().preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.guard(|| self.imp().coerce_types(arg_types))
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
