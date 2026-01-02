//! Python bindings for the Beacon Binary Format writer APIs.

mod collection_builder;
mod collection_reader;
mod numpy_arrays;
mod object_store_handle;
mod utils;

use collection_builder::{Collection, CollectionBuilder, PartitionBuilder};
use collection_reader::{CollectionReaderHandle, PartitionReaderHandle};
use object_store_handle::ObjectStoreHandle;
use pyo3::prelude::*;
use pyo3::types::PyModule;

#[pyfunction]
fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn beacon_binary_format(_py: Python<'_>, m: Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ObjectStoreHandle>()?;
    m.add_class::<Collection>()?;
    m.add_class::<PartitionBuilder>()?;
    m.add_class::<CollectionBuilder>()?;
    m.add_class::<CollectionReaderHandle>()?;
    m.add_class::<PartitionReaderHandle>()?;
    m.add_function(wrap_pyfunction!(crate_version, &m)?)?;
    Ok(())
}
