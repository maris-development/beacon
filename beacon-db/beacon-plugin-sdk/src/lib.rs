#![doc = include_str!("../README.md")]

#[cfg(panic = "abort")]
compile_error!("beacon plugins need panic = \"unwind\"");

pub mod file_format;
pub mod object_store;
mod panic_safe;
pub mod scalar_functions;

pub use beacon_nd_encoding as nd;
pub use datafusion;
pub use datafusion_ffi;
pub use datafusion_proto;
pub use stabby;

/// The ABI version of this SDK. Beacon refuses a plugin with a different value.
pub const BEACON_PLUGIN_SDK_ABI: u32 = 5;

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

/// The type of the [`ABI_SYMBOL`] function.
pub type AbiFn = extern "C" fn() -> BeaconPluginAbi;

/// Generates the `extern "C"` symbols that Beacon loads from a plugin.
///
/// `scalar` names a `fn() -> Vec<ScalarUDF>` that returns the scalar functions.
/// `formats` names a `fn() -> Vec<Arc<dyn FormatFactory>>` that returns the file formats.
/// Give one key or both.
///
/// ```ignore
/// beacon_plugin_sdk::export_plugin!(scalar = functions, formats = file_formats);
/// ```
#[macro_export]
macro_rules! export_plugin {
    // Hidden arm: sets the ABI values, for mismatch tests.
    (@abi $sdk_abi:expr, $ffi_major:expr $(, scalar = $scalar:path)? $(, formats = $formats:path)?) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_abi() -> $crate::BeaconPluginAbi {
            $crate::BeaconPluginAbi {
                sdk_abi: $sdk_abi,
                ffi_major: $ffi_major,
            }
        }

        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn beacon_plugin_scalar_functions()
            -> $crate::datafusion_ffi::util::FFI_Result<
                $crate::stabby::vec::Vec<$crate::scalar_functions::FFI_ScalarFunction>,
            > {
                $crate::scalar_functions::export_scalar_functions($scalar)
            }
        )?

        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn beacon_plugin_file_formats()
            -> $crate::datafusion_ffi::util::FFI_Result<
                $crate::stabby::vec::Vec<$crate::file_format::FFI_FileFormatFactory>,
            > {
                $crate::file_format::export_file_formats($formats)
            }
        )?
    };
    (scalar = $scalar:path) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version(),
            scalar = $scalar
        );
    };
    (formats = $formats:path) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version(),
            formats = $formats
        );
    };
    (scalar = $scalar:path, formats = $formats:path) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version(),
            scalar = $scalar,
            formats = $formats
        );
    };
}

#[cfg(test)]
mod tests {
    use datafusion_ffi::util::FFI_Result;

    use super::*;
    use crate::file_format::FileFormatsFn;
    use crate::file_format::factory_tests::make_formats;
    use crate::scalar_functions::ScalarFunctionsFn;
    use crate::scalar_functions::tests::make_functions;

    crate::export_plugin!(scalar = make_functions, formats = make_formats);

    #[test]
    fn export_plugin_generates_the_symbols() {
        let abi: AbiFn = beacon_plugin_abi;
        let functions: ScalarFunctionsFn = beacon_plugin_scalar_functions;
        let formats: FileFormatsFn = beacon_plugin_file_formats;
        let FFI_Result::Ok(formats) = formats() else {
            panic!("expected formats");
        };
        assert_eq!(formats.len(), 2);
        assert_eq!(
            abi(),
            BeaconPluginAbi {
                sdk_abi: BEACON_PLUGIN_SDK_ABI,
                ffi_major: datafusion_ffi::version()
            }
        );
        let FFI_Result::Ok(functions) = functions() else {
            panic!("expected functions");
        };
        assert_eq!(functions.len(), 1);
    }
}
