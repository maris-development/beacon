//! Calls a plugin function with each scalar argument as a scalar.
//!
//! `ForeignScalarUDF` in `datafusion-ffi` changes each scalar argument to an array
//! before the call crosses. So Beacon calls a plugin function through [`InvokeFn`],
//! which sends each argument as [`FFI_ColumnarValue`]. A scalar crosses as a
//! one-row array with the `Scalar` tag.

use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::ffi::FFI_ArrowSchema;
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
use datafusion_ffi::arrow_wrappers::WrappedSchema;
use datafusion_ffi::config::FFI_ConfigOptions;
use datafusion_ffi::expr::columnar_value::FFI_ColumnarValue;
use datafusion_ffi::udf::{FFI_ScalarUDF, ScalarUDFPrivateData};
use datafusion_ffi::util::FFI_Result;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use crate::panic_safe::panic_message;

/// Calls the plugin UDF of `udf`, with each argument as [`FFI_ColumnarValue`].
///
/// `udf` must be an [`FFI_ScalarUDF`] that the plugin exported, or a clone of one.
pub type InvokeFn = unsafe extern "C" fn(
    udf: &FFI_ScalarUDF,
    args: SVec<FFI_ColumnarValue>,
    arg_fields: SVec<WrappedSchema>,
    num_rows: usize,
    return_field: WrappedSchema,
    config_options: FFI_ConfigOptions,
) -> FFI_Result<FFI_ColumnarValue>;

/// The plugin side of [`InvokeFn`].
///
/// # Safety
///
/// The private data of `udf` must be a [`ScalarUDFPrivateData`] of this library.
/// [`super::export_scalar_functions`] makes each UDF so.
pub unsafe extern "C" fn invoke_fn(
    udf: &FFI_ScalarUDF,
    args: SVec<FFI_ColumnarValue>,
    arg_fields: SVec<WrappedSchema>,
    num_rows: usize,
    return_field: WrappedSchema,
    config_options: FFI_ConfigOptions,
) -> FFI_Result<FFI_ColumnarValue> {
    // SAFETY: the caller guarantees the type of the private data.
    let inner = unsafe { &(*(udf.private_data as *const ScalarUDFPrivateData)).udf };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let args = args
            .into_iter()
            .map(ColumnarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let arg_fields = arg_fields
            .iter()
            .map(|field| Ok(Arc::new(Field::try_from(&field.0)?)))
            .collect::<Result<Vec<FieldRef>>>()?;
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field: Arc::new(Field::try_from(&return_field.0)?),
            config_options: Arc::new(ConfigOptions::try_from(config_options)?),
        };
        FFI_ColumnarValue::try_from(inner.invoke_with_args(args)?)
    }));
    match result {
        Ok(Ok(value)) => FFI_Result::Ok(value),
        Ok(Err(error)) => FFI_Result::Err(SString::from(error.to_string().as_str())),
        Err(payload) => FFI_Result::Err(SString::from(
            format!(
                "plugin function '{}' panicked: {}",
                inner.name(),
                panic_message(payload.as_ref())
            )
            .as_str(),
        )),
    }
}

/// A plugin UDF that Beacon calls through [`InvokeFn`].
///
/// `invoke_with_args` calls `invoke` with the arguments as DataFusion gives them.
/// All other calls go to `inner`, the `ForeignScalarUDF` of the same function.
#[derive(Debug)]
pub struct ScalarArgsUdf {
    inner: Arc<ScalarUDF>,
    udf: FFI_ScalarUDF,
    invoke: InvokeFn,
}

impl ScalarArgsUdf {
    /// Wraps `inner`, made from `udf`, and calls `udf` through `invoke`.
    pub fn new(inner: Arc<ScalarUDF>, udf: FFI_ScalarUDF, invoke: InvokeFn) -> Self {
        Self { inner, udf, invoke }
    }

    /// Returns the wrapped UDF.
    pub fn inner(&self) -> &Arc<ScalarUDF> {
        &self.inner
    }

    fn imp(&self) -> &dyn ScalarUDFImpl {
        self.inner.inner().as_ref()
    }
}

impl PartialEq for ScalarArgsUdf {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for ScalarArgsUdf {}

impl Hash for ScalarArgsUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl ScalarUDFImpl for ScalarArgsUdf {
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.imp().return_field_from_args(args)
    }

    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.imp().is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        let args = args
            .into_iter()
            .map(FFI_ColumnarValue::try_from)
            .collect::<Result<SVec<_>>>()?;
        let arg_fields = arg_fields
            .iter()
            .map(|field| Ok(WrappedSchema(FFI_ArrowSchema::try_from(field.as_ref())?)))
            .collect::<Result<SVec<_>>>()?;
        let return_field = WrappedSchema(FFI_ArrowSchema::try_from(return_field.as_ref())?);
        let config_options = FFI_ConfigOptions::from(config_options.as_ref());
        // SAFETY: `udf` is the exported UDF that `invoke` belongs to.
        let result = unsafe {
            (self.invoke)(
                &self.udf,
                args,
                arg_fields,
                number_rows,
                return_field,
                config_options,
            )
        };
        let result: std::result::Result<FFI_ColumnarValue, SString> = result.into();
        let result = result.map_err(|message| DataFusionError::Execution(message.to_string()))?;
        ColumnarValue::try_from(result)
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::{Result, exec_err};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::scalar::ScalarValue;

    use super::ScalarArgsUdf;
    use crate::scalar_functions::tests::{
        arg_kinds, arg_kinds_body, foreign_scalar_udf_from, foreign_udf_from,
    };
    use crate::scalar_functions::{Implied, function, hooks_of};

    fn float_array(values: Vec<Option<f64>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Float64Array::from(values)))
    }

    fn float_scalar(value: Option<f64>) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Float64(value))
    }

    fn call(args: Vec<ColumnarValue>, number_rows: usize, returns: DataType) -> ScalarFunctionArgs {
        let arg_fields = (0..args.len())
            .map(|i| Arc::new(Field::new(format!("a{i}"), DataType::Float64, true)))
            .collect();
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("r", returns, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn text(value: ColumnarValue) -> String {
        match value {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) => text,
            other => panic!("expected a Utf8 scalar, got {other:?}"),
        }
    }

    fn invoke_kinds(make: fn() -> Vec<ScalarUDF>, args: Vec<ColumnarValue>, rows: usize) -> String {
        let udf = foreign_udf_from(make);
        text(
            udf.invoke_with_args(call(args, rows, DataType::Utf8))
                .unwrap(),
        )
    }

    #[test]
    fn an_imported_function_is_a_scalar_args_udf() {
        let udf = foreign_udf_from(|| vec![arg_kinds()]);
        let imp: &dyn Any = udf.as_ref();
        let wrapper = imp
            .downcast_ref::<ScalarArgsUdf>()
            .expect("a ScalarArgsUdf");
        assert!(
            wrapper
                .inner()
                .inner()
                .as_ref()
                .is::<datafusion_ffi::udf::ForeignScalarUDF>()
        );
    }

    #[test]
    fn a_scalar_argument_stays_a_scalar() {
        let args = vec![
            float_array(vec![Some(1.0), Some(2.0)]),
            float_scalar(Some(2.0)),
        ];
        assert_eq!(invoke_kinds(|| vec![arg_kinds()], args, 2), "array,scalar");
    }

    #[test]
    fn a_null_scalar_stays_a_null_scalar() {
        fn null_check() -> Vec<ScalarUDF> {
            vec![
                function("t_null_check")
                    .args([DataType::Float64, DataType::Float64])
                    .returns(DataType::Utf8)
                    .invoke(|args| match &args[1] {
                        ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("ok".into()))))
                        }
                        other => exec_err!("expected a null Float64 scalar, got {other:?}"),
                    }),
            ]
        }
        let args = vec![float_array(vec![Some(1.0)]), float_scalar(None)];
        assert_eq!(invoke_kinds(null_check, args, 1), "ok");
    }

    #[test]
    fn a_hooked_function_keeps_scalar_arguments() {
        fn hooked() -> Vec<ScalarUDF> {
            vec![
                function("t_hooked_kinds")
                    .args([DataType::Float64, DataType::Float64])
                    .returns(DataType::Utf8)
                    .implies(|args| Ok(Some(Implied::Necessary(args[0].clone().is_not_null()))))
                    .invoke(arg_kinds_body),
            ]
        }
        let udf = foreign_scalar_udf_from(hooked);
        assert!(hooks_of(&udf).is_some());
        let args = vec![float_array(vec![Some(1.0)]), float_scalar(Some(2.0))];
        let result = udf.invoke_with_args(call(args, 1, DataType::Utf8)).unwrap();
        assert_eq!(text(result), "array,scalar");
    }

    /// `t_rows()` returns `number_rows` as a scalar.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct RowCount {
        signature: Signature,
    }

    impl ScalarUDFImpl for RowCount {
        fn name(&self) -> &str {
            "t_rows"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let rows = args.number_rows as i64;
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(rows))))
        }
    }

    #[test]
    fn number_rows_crosses_the_ffi() {
        fn rows() -> Vec<ScalarUDF> {
            vec![ScalarUDF::from(RowCount {
                signature: Signature::exact(vec![], Volatility::Immutable),
            })]
        }
        let udf = foreign_udf_from(rows);
        for count in [0, 1, 7] {
            let result = udf
                .invoke_with_args(call(vec![], count, DataType::Int64))
                .unwrap();
            let ColumnarValue::Scalar(value) = result else {
                panic!("expected a scalar");
            };
            assert_eq!(value, ScalarValue::Int64(Some(count as i64)));
        }
    }

    #[test]
    fn an_array_result_crosses_the_ffi() {
        fn first() -> Vec<ScalarUDF> {
            vec![
                function("t_first")
                    .args([DataType::Float64, DataType::Float64])
                    .returns(DataType::Float64)
                    .invoke_arrays(|arrays| Ok(Arc::clone(&arrays[0]))),
            ]
        }
        let udf = foreign_udf_from(first);
        let args = vec![
            float_array(vec![Some(1.0), Some(2.0)]),
            float_scalar(Some(9.0)),
        ];
        let result = udf
            .invoke_with_args(call(args, 2, DataType::Float64))
            .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("expected an array");
        };
        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(1.0), Some(2.0)]);
    }
}
