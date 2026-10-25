#![doc = include_str!("../README.md")]
#[cfg(panic = "abort")]
compile_error!("beacon plugins need panic = \"unwind\"");

pub mod function;
mod panic_safe;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

pub use datafusion;
use datafusion::logical_expr::ScalarUDF;
pub use datafusion_ffi;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::util::FFI_Result;
pub use stabby;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

pub use function::{FunctionBuilder, function};
pub use panic_safe::PanicSafeUdf;

/// The ABI version of this SDK. Beacon refuses a plugin with a different value.
pub const BEACON_PLUGIN_SDK_ABI: u32 = 1;

/// The ABI versions of a plugin, as `beacon_plugin_abi` returns them.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeaconPluginAbi {
    /// The value of [`BEACON_PLUGIN_SDK_ABI`] in the plugin.
    pub sdk_abi: u32,
    /// The value of [`datafusion_ffi::version`] in the plugin.
    pub ffi_major: u64,
}

/// The name of the ABI symbol, with a trailing NUL.
pub const ABI_SYMBOL: &[u8] = b"beacon_plugin_abi\0";

/// The name of the functions symbol, with a trailing NUL.
pub const FUNCTIONS_SYMBOL: &[u8] = b"beacon_plugin_functions\0";

/// The type of the [`ABI_SYMBOL`] function.
pub type AbiFn = extern "C" fn() -> BeaconPluginAbi;

/// The type of the [`FUNCTIONS_SYMBOL`] function.
pub type FunctionsFn = extern "C" fn() -> FFI_Result<SVec<FFI_ScalarUDF>>;

/// Calls `make` and converts its UDFs to FFI structs.
///
/// Each UDF is wrapped in a [`PanicSafeUdf`]. A panic in `make` or in the
/// conversion gives [`FFI_Result::Err`] with the panic message.
pub fn export_functions(make: fn() -> Vec<ScalarUDF>) -> FFI_Result<SVec<FFI_ScalarUDF>> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut functions = SVec::new();
        for udf in make() {
            let safe = ScalarUDF::from(PanicSafeUdf::new(Arc::new(udf)));
            functions.push(FFI_ScalarUDF::from(Arc::new(safe)));
        }
        functions
    }));
    match result {
        Ok(functions) => FFI_Result::Ok(functions),
        Err(payload) => FFI_Result::Err(SString::from(
            format!(
                "plugin functions panicked: {}",
                panic_safe::panic_message(payload.as_ref())
            )
            .as_str(),
        )),
    }
}

/// Generates the `extern "C"` symbols that Beacon loads from a plugin.
///
/// `$make` is a `fn() -> Vec<ScalarUDF>` that returns the plugin functions.
///
/// ```ignore
/// beacon_plugin_sdk::export_plugin!(functions);
/// ```
#[macro_export]
macro_rules! export_plugin {
    // Hidden arm: sets the ABI values, for mismatch tests.
    (@abi $sdk_abi:expr, $ffi_major:expr, $make:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_abi() -> $crate::BeaconPluginAbi {
            $crate::BeaconPluginAbi {
                sdk_abi: $sdk_abi,
                ffi_major: $ffi_major,
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_functions() -> $crate::datafusion_ffi::util::FFI_Result<
            $crate::stabby::vec::Vec<$crate::datafusion_ffi::udf::FFI_ScalarUDF>,
        > {
            $crate::export_functions($make)
        }
    };
    ($make:path) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version(),
            $make
        );
    };
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

    fn make_functions() -> Vec<ScalarUDF> {
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
        match export_functions(make_panics) {
            FFI_Result::Err(message) => assert!(message.contains("boom"), "{message}"),
            FFI_Result::Ok(_) => panic!("expected an error"),
        }
    }

    extern "C" fn foreign_marker_id() -> usize {
        datafusion_ffi::get_library_marker_id() + 1
    }

    /// Exports the first UDF of `make` and converts it back as a `ForeignScalarUDF`.
    pub(crate) fn foreign_udf_from(make: fn() -> Vec<ScalarUDF>) -> Arc<dyn ScalarUDFImpl> {
        let FFI_Result::Ok(functions) = export_functions(make) else {
            panic!("expected functions");
        };
        assert_eq!(functions.len(), 1);
        let mut ffi = functions[0].clone();
        // A different marker makes the conversion go through the FFI function pointers.
        ffi.library_marker_id = foreign_marker_id;
        let udf = Arc::<dyn ScalarUDFImpl>::from(&ffi);
        assert!(udf.as_ref().is::<datafusion_ffi::udf::ForeignScalarUDF>());
        udf
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

    crate::export_plugin!(make_functions);

    #[test]
    fn export_plugin_generates_the_symbols() {
        let abi: AbiFn = beacon_plugin_abi;
        let functions: FunctionsFn = beacon_plugin_functions;
        assert_eq!(
            abi(),
            BeaconPluginAbi {
                sdk_abi: BEACON_PLUGIN_SDK_ABI,
                ffi_major: 54
            }
        );
        let FFI_Result::Ok(functions) = functions() else {
            panic!("expected functions");
        };
        assert_eq!(functions.len(), 1);
    }
}
