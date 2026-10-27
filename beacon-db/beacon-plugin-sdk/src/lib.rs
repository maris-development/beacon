#![doc = include_str!("../README.md")]

#[cfg(panic = "abort")]
compile_error!("beacon plugins need panic = \"unwind\"");

mod panic_safe;
pub mod scalar_functions;

pub use datafusion;
pub use datafusion_ffi;
pub use datafusion_proto;
pub use stabby;

/// The ABI version of this SDK. Beacon refuses a plugin with a different value.
pub const BEACON_PLUGIN_SDK_ABI: u32 = 4;

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
///
/// ```ignore
/// beacon_plugin_sdk::export_plugin!(scalar = functions);
/// ```
#[macro_export]
macro_rules! export_plugin {
    // Hidden arm: sets the ABI values, for mismatch tests.
    (@abi $sdk_abi:expr, $ffi_major:expr, scalar = $scalar:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_abi() -> $crate::BeaconPluginAbi {
            $crate::BeaconPluginAbi {
                sdk_abi: $sdk_abi,
                ffi_major: $ffi_major,
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_scalar_functions()
        -> $crate::datafusion_ffi::util::FFI_Result<
            $crate::stabby::vec::Vec<$crate::scalar_functions::FFI_ScalarFunction>,
        > {
            $crate::scalar_functions::export_scalar_functions($scalar)
        }
    };
    (scalar = $scalar:path) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version(),
            scalar = $scalar
        );
    };
}

#[cfg(test)]
mod tests {
    use datafusion_ffi::util::FFI_Result;

    use super::*;
    use crate::scalar_functions::ScalarFunctionsFn;
    use crate::scalar_functions::tests::make_functions;

    crate::export_plugin!(scalar = make_functions);

    #[test]
    fn export_plugin_generates_the_symbols() {
        let abi: AbiFn = beacon_plugin_abi;
        let functions: ScalarFunctionsFn = beacon_plugin_scalar_functions;
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
