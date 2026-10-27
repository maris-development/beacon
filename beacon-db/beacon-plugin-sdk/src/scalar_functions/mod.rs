//! Scalar plugin functions: the builder, the planner hooks and the FFI export.

mod builder;
mod hooks;
pub mod hooks_ffi;
mod panic_safe_udf;
pub mod scalar_args;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::util::{FFI_Option, FFI_Result};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

pub use builder::{FunctionBuilder, function};
pub use hooks::{FunctionHooks, HookedUdf, Implied, hooks_of};
use hooks_ffi::{FFI_FunctionHooks, ForeignFunctionHooks};
pub use panic_safe_udf::PanicSafeUdf;
pub use scalar_args::ScalarArgsUdf;
use scalar_args::{InvokeFn, invoke_fn};

use crate::panic_safe::panic_message;

/// The name of the scalar functions symbol, with a trailing NUL.
pub const SCALAR_FUNCTIONS_SYMBOL: &[u8] = b"beacon_plugin_scalar_functions\0";

/// The type of the [`SCALAR_FUNCTIONS_SYMBOL`] function.
pub type ScalarFunctionsFn = extern "C" fn() -> FFI_Result<SVec<FFI_ScalarFunction>>;

/// One scalar function as `beacon_plugin_scalar_functions` returns it.
#[repr(C)]
pub struct FFI_ScalarFunction {
    /// The function itself.
    pub udf: FFI_ScalarUDF,
    /// Calls `udf` and keeps each scalar argument as a scalar, see [`ScalarArgsUdf`].
    pub invoke: InvokeFn,
    /// The planner hooks of the function, see [`FunctionHooks`].
    pub hooks: FFI_Option<FFI_FunctionHooks>,
}

/// Calls `make` and converts its UDFs and their hooks to FFI structs.
///
/// Each UDF is wrapped in a [`PanicSafeUdf`]. A panic in `make` or in the
/// conversion gives [`FFI_Result::Err`] with the panic message.
pub fn export_scalar_functions(
    make: fn() -> Vec<ScalarUDF>,
) -> FFI_Result<SVec<FFI_ScalarFunction>> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut functions = SVec::new();
        for udf in make() {
            let hooks = hooks_of(&udf)
                .map(|hooked| FFI_FunctionHooks::new(udf.name(), Arc::clone(hooked.hooks())));
            // The hook table carries the hooks, so the UDF crosses without its wrapper.
            let udf = match hooks_of(&udf) {
                Some(hooked) => Arc::clone(hooked.inner()),
                None => Arc::new(udf),
            };
            // `invoke_fn` reads the private data, so the UDF must not be a foreign UDF.
            let safe = ScalarUDF::from(PanicSafeUdf::new(udf));
            functions.push(FFI_ScalarFunction {
                udf: FFI_ScalarUDF::from(Arc::new(safe)),
                invoke: invoke_fn,
                hooks: hooks.into(),
            });
        }
        functions
    }));
    match result {
        Ok(functions) => FFI_Result::Ok(functions),
        Err(payload) => FFI_Result::Err(SString::from(
            format!(
                "plugin scalar functions panicked: {}",
                panic_message(payload.as_ref())
            )
            .as_str(),
        )),
    }
}

/// Converts an exported scalar function back to a UDF, with its hooks.
///
/// The UDF is a [`ScalarArgsUdf`], in a [`HookedUdf`] when the function has hooks.
pub fn import_scalar_function(function: FFI_ScalarFunction) -> ScalarUDF {
    let FFI_ScalarFunction {
        udf: ffi_udf,
        invoke,
        hooks,
    } = function;
    let foreign = ScalarUDF::new_from_shared_impl(Arc::<dyn ScalarUDFImpl>::from(&ffi_udf));
    let udf = ScalarUDF::from(ScalarArgsUdf::new(Arc::new(foreign), ffi_udf, invoke));
    match hooks {
        FFI_Option::Some(table) => ScalarUDF::from(HookedUdf::new(
            Arc::new(udf),
            Arc::new(ForeignFunctionHooks::from(table)),
        )),
        FFI_Option::None => udf,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::scalar::ScalarValue;
    use datafusion_ffi::util::FFI_Result;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestUdf {
        signature: Signature,
        aliases: Vec<String>,
    }

    impl TestUdf {
        fn new() -> Self {
            Self {
                // User-defined, so type coercion calls coerce_types.
                signature: Signature::user_defined(Volatility::Immutable),
                aliases: vec!["test_alias".to_string()],
            }
        }
    }

    impl ScalarUDFImpl for TestUdf {
        fn name(&self) -> &str {
            "test_udf"
        }

        fn aliases(&self) -> &[String] {
            &self.aliases
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            panic!("invoke exploded")
        }

        fn coerce_types(
            &self,
            _arg_types: &[DataType],
        ) -> datafusion::common::Result<Vec<DataType>> {
            panic!("coerce exploded")
        }
    }

    pub(crate) fn make_functions() -> Vec<ScalarUDF> {
        vec![ScalarUDF::from(TestUdf::new())]
    }

    fn make_panics() -> Vec<ScalarUDF> {
        panic!("boom")
    }

    fn invoke_args() -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Int64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn a_panic_in_invoke_becomes_an_execution_error() {
        let udf = PanicSafeUdf::new(Arc::new(ScalarUDF::from(TestUdf::new())));
        let err = udf.invoke_with_args(invoke_args()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("panicked"), "{message}");
        assert!(message.contains("test_udf"), "{message}");
        assert!(message.contains("invoke exploded"), "{message}");
    }

    #[test]
    fn a_panic_in_make_becomes_ffi_err() {
        match export_scalar_functions(make_panics) {
            FFI_Result::Err(message) => assert!(message.contains("boom"), "{message}"),
            FFI_Result::Ok(_) => panic!("expected an error"),
        }
    }

    extern "C" fn foreign_marker_id() -> usize {
        datafusion_ffi::get_library_marker_id() + 1
    }

    /// Exports the only UDF of `make` and imports it as Beacon does.
    pub(crate) fn foreign_scalar_udf_from(make: fn() -> Vec<ScalarUDF>) -> ScalarUDF {
        let FFI_Result::Ok(functions) = export_scalar_functions(make) else {
            panic!("expected functions");
        };
        assert_eq!(functions.len(), 1);
        let mut function = functions.into_iter().next().unwrap();
        // A different marker makes the conversion go through the FFI function pointers.
        function.udf.library_marker_id = foreign_marker_id;
        import_scalar_function(function)
    }

    /// Like [`foreign_scalar_udf_from`], but returns the outer impl.
    pub(crate) fn foreign_udf_from(make: fn() -> Vec<ScalarUDF>) -> Arc<dyn ScalarUDFImpl> {
        Arc::clone(foreign_scalar_udf_from(make).inner())
    }

    /// Returns `"array"` or `"scalar"` per argument, joined with `,`.
    pub(crate) fn arg_kinds_body(
        args: &[ColumnarValue],
    ) -> datafusion::common::Result<ColumnarValue> {
        let kinds: Vec<&str> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(_) => "array",
                ColumnarValue::Scalar(_) => "scalar",
            })
            .collect();
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            kinds.join(","),
        ))))
    }

    /// `t_arg_kinds(a, b)`, see [`arg_kinds_body`].
    pub(crate) fn arg_kinds() -> ScalarUDF {
        function("t_arg_kinds")
            .args([DataType::Float64, DataType::Float64])
            .returns(DataType::Utf8)
            .invoke(arg_kinds_body)
    }

    fn foreign_udf() -> Arc<dyn ScalarUDFImpl> {
        foreign_udf_from(make_functions)
    }

    #[test]
    fn exported_functions_keep_name_and_aliases() {
        let udf = foreign_udf();
        assert_eq!(udf.name(), "test_udf");
        assert_eq!(udf.aliases(), ["test_alias".to_string()]);
    }

    #[test]
    fn a_panic_behind_the_ffi_becomes_an_error() {
        let udf = foreign_udf();
        let err = udf.invoke_with_args(invoke_args()).unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
        assert!(err.to_string().contains("invoke exploded"), "{err}");
        let err = udf.coerce_types(&[DataType::Int64]).unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
        assert!(err.to_string().contains("coerce exploded"), "{err}");
    }
}
