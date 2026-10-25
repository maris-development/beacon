//! Opens a native plugin library and reads its functions through datafusion-ffi.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use beacon_plugin_sdk::datafusion_ffi::{self, util::FFI_Result};
use beacon_plugin_sdk::{AbiFn, FunctionsFn, ABI_SYMBOL, BEACON_PLUGIN_SDK_ABI, FUNCTIONS_SYMBOL};
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};
use parking_lot::Mutex;

/// Every native library that opened. Never dropped: plugin code may still run.
static LIBRARIES: Mutex<Vec<libloading::Library>> = Mutex::new(Vec::new());

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
/// A library that opens stays loaded until the process stops, also when a check fails:
/// the functions point into its code, and the library ran code at load.
pub fn load_native(path: &Path) -> anyhow::Result<Vec<ScalarUDF>> {
    // SAFETY: a plugin library is trusted code that a super-user installed.
    let library = unsafe { libloading::Library::new(path) }
        .with_context(|| format!("cannot open native library {}", path.display()))?;
    let result = functions(&library);
    LIBRARIES.lock().push(library);
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_that_is_not_a_library_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(library_file_name("fake"));
        std::fs::write(&path, b"not a library").unwrap();
        let err = load_native(&path).unwrap_err().to_string();
        assert!(err.contains("cannot open native library"), "{err}");
    }

    #[test]
    fn library_file_name_uses_the_platform_names() {
        let expected = format!(
            "{}x{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );
        assert_eq!(library_file_name("x"), expected);
    }
}
