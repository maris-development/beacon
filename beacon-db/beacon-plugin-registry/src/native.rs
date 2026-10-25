//! Opens a native plugin library and reads its functions through datafusion-ffi.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use beacon_plugin_sdk::datafusion_ffi::{self, util::FFI_Result};
use beacon_plugin_sdk::{AbiFn, FunctionsFn, ABI_SYMBOL, BEACON_PLUGIN_SDK_ABI, FUNCTIONS_SYMBOL};
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};

/// The file name of native library `name` on this platform, such as `libexample.so`.
pub fn library_file_name(name: &str) -> String {
    format!(
        "{}{name}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    )
}

/// Opens the library at `path`, checks its ABI and returns its functions.
///
/// The caller must keep the returned [`libloading::Library`] for the life of the
/// process, also when a later check fails: the functions point into its code.
/// A library that fails a check here is never unloaded.
pub fn load_native(path: &Path) -> anyhow::Result<(libloading::Library, Vec<ScalarUDF>)> {
    // SAFETY: a plugin library is trusted code that a super-user installed.
    let library = unsafe { libloading::Library::new(path) }
        .with_context(|| format!("cannot open native library {}", path.display()))?;
    match functions(&library) {
        Ok(udfs) => Ok((library, udfs)),
        Err(error) => {
            // The library ran code at load, so it stays loaded.
            std::mem::forget(library);
            Err(error)
        }
    }
}

fn functions(library: &libloading::Library) -> anyhow::Result<Vec<ScalarUDF>> {
    // SAFETY: the SDK exports both symbols with these types.
    let (abi, functions) = unsafe {
        let abi = *library
            .get::<AbiFn>(ABI_SYMBOL)
            .context("the library has no beacon_plugin_abi symbol")?;
        let functions = *library
            .get::<FunctionsFn>(FUNCTIONS_SYMBOL)
            .context("the library has no beacon_plugin_functions symbol")?;
        (abi, functions)
    };
    let abi = abi();
    anyhow::ensure!(
        abi.sdk_abi == BEACON_PLUGIN_SDK_ABI,
        "plugin SDK ABI {}, Beacon needs {BEACON_PLUGIN_SDK_ABI}",
        abi.sdk_abi
    );
    let ffi_major = datafusion_ffi::version();
    anyhow::ensure!(
        abi.ffi_major == ffi_major,
        "plugin built for DataFusion FFI {}, Beacon has {ffi_major}",
        abi.ffi_major
    );
    match functions() {
        FFI_Result::Ok(list) => Ok(list
            .iter()
            .map(|ffi| ScalarUDF::new_from_shared_impl(Arc::<dyn ScalarUDFImpl>::from(ffi)))
            .collect()),
        FFI_Result::Err(message) => anyhow::bail!("plugin init failed: {message}"),
    }
}
