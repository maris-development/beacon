use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;

pub(crate) fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
