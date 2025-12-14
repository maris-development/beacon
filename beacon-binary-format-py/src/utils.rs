use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub(crate) fn init_store(base_dir: String) -> PyResult<Arc<dyn ObjectStore>> {
    let mut base = PathBuf::from(base_dir);
    if base.to_string_lossy().is_empty() {
        base = std::env::temp_dir();
    }
    std::fs::create_dir_all(&base)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to create base dir: {err}")))?;
    let fs = LocalFileSystem::new_with_prefix(base)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to create store: {err}")))?;
    Ok(Arc::new(fs))
}

pub(crate) fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
