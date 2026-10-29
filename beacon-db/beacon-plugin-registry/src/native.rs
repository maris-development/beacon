//! Opens a native plugin library and reads its functions through datafusion-ffi.

use std::path::Path;

use anyhow::Context as _;
use beacon_plugin_sdk::datafusion_ffi::{self, util::FFI_Result};
use beacon_plugin_sdk::file_format::{FFI_FileFormatFactory, FileFormatsFn, FILE_FORMATS_SYMBOL};
use beacon_plugin_sdk::scalar_functions::{
    import_scalar_function, ScalarFunctionsFn, SCALAR_FUNCTIONS_SYMBOL,
};
use beacon_plugin_sdk::{AbiFn, ABI_SYMBOL, BEACON_PLUGIN_SDK_ABI};
use datafusion::logical_expr::ScalarUDF;
use parking_lot::Mutex;

/// What a native library exports. A library has functions, file formats or both.
pub struct NativeExports {
    /// The scalar functions.
    pub functions: Vec<ScalarUDF>,
    /// The file format factories. The host imports them with its runtime handle.
    pub formats: Vec<FFI_FileFormatFactory>,
}

impl std::fmt::Debug for NativeExports {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeExports")
            .field("functions", &self.functions.len())
            .field("formats", &self.formats.len())
            .finish()
    }
}

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
pub fn load_native(path: &Path) -> anyhow::Result<NativeExports> {
    if !path.is_file() {
        return Err(missing_library(path));
    }
    // SAFETY: a plugin library is trusted code that a super-user installed.
    let library = unsafe { libloading::Library::new(path) }
        .with_context(|| format!("cannot open native library {}", path.display()))?;
    let result = exports(&library);
    LIBRARIES.lock().push(library);
    result
}

/// Names the files next to a missing library, such as a `.so` file on Windows.
fn missing_library(path: &Path) -> anyhow::Error {
    let expected = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut found: Vec<String> = path
        .parent()
        .and_then(|dir| std::fs::read_dir(dir).ok())
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    if found.is_empty() {
        anyhow::anyhow!("native library {expected} is missing: the plugin has no library file")
    } else {
        anyhow::anyhow!(
            "native library {expected} is missing: the plugin has {}. \
             The plugin is for another platform, or [native] library names another file",
            found.join(", ")
        )
    }
}

fn require_exports(has_functions: bool, has_formats: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        has_functions || has_formats,
        "plugin exports no functions and no file formats"
    );
    Ok(())
}

fn exports(library: &libloading::Library) -> anyhow::Result<NativeExports> {
    // SAFETY: the SDK exports these symbols with these types.
    let (abi, functions, formats) = unsafe {
        let abi = *library
            .get::<AbiFn>(ABI_SYMBOL)
            .context("the library has no beacon_plugin_abi symbol")?;
        let functions = library
            .get::<ScalarFunctionsFn>(SCALAR_FUNCTIONS_SYMBOL)
            .ok()
            .map(|symbol| *symbol);
        let formats = library
            .get::<FileFormatsFn>(FILE_FORMATS_SYMBOL)
            .ok()
            .map(|symbol| *symbol);
        (abi, functions, formats)
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
    require_exports(functions.is_some(), formats.is_some())?;
    let functions = match functions.map(|make| make()) {
        None => Vec::new(),
        Some(FFI_Result::Ok(list)) => list.into_iter().map(import_scalar_function).collect(),
        Some(FFI_Result::Err(message)) => anyhow::bail!("plugin init failed: {message}"),
    };
    let formats = match formats.map(|make| make()) {
        None => Vec::new(),
        Some(FFI_Result::Ok(list)) => list.into_iter().collect(),
        Some(FFI_Result::Err(message)) => anyhow::bail!("plugin file formats failed: {message}"),
    };
    Ok(NativeExports { functions, formats })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_need_functions_or_formats() {
        let err = require_exports(false, false).unwrap_err().to_string();
        assert_eq!(err, "plugin exports no functions and no file formats");
        assert!(require_exports(true, false).is_ok());
        assert!(require_exports(false, true).is_ok());
        assert!(require_exports(true, true).is_ok());
    }

    #[test]
    fn a_file_that_is_not_a_library_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(library_file_name("fake"));
        std::fs::write(&path, b"not a library").unwrap();
        let err = load_native(&path).unwrap_err().to_string();
        assert!(err.contains("cannot open native library"), "{err}");
    }

    #[test]
    fn a_library_for_another_platform_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let other = if cfg!(windows) { "libx.so" } else { "x.dll" };
        std::fs::write(dir.path().join(other), b"x").unwrap();
        let err = load_native(&dir.path().join(library_file_name("x")))
            .unwrap_err()
            .to_string();
        assert!(err.contains(&library_file_name("x")), "{err}");
        assert!(err.contains(other), "{err}");
        assert!(err.contains("another platform"), "{err}");
    }

    #[test]
    fn a_missing_library_in_an_empty_folder_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let err = load_native(&dir.path().join(library_file_name("x")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no library file"), "{err}");
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
