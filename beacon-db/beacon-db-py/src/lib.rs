//! `beacondb` — Python bindings for beacon-db, an embeddable DuckDB-class database.
//!
//! This crate is the thin layer between CPython and [`beacon_core::embedded`]: it owns the
//! Tokio runtime, maps engine errors onto the PEP 249 exception tree, and moves results across
//! the Arrow C Data Interface. Engine policy (what a mode means, what an identity may do)
//! lives in beacon-core, so it is shared with every other embedder.
//!
//! The compiled module is `beacondb._beacondb`; the user-facing surface is the `beacondb`
//! Python package that wraps it.

use pyo3::prelude::*;

mod connection;
mod errors;
mod exec;
mod params;
mod relation;
mod result;
mod runtime;

/// The engine version, so `beacondb.__version__` and the Rust crate can never disagree.
#[pyfunction]
fn engine_version() -> &'static str {
    beacon_core::runtime::Runtime::version()
}

#[pymodule]
fn _beacondb(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(module)?;
    module.add_class::<connection::Connection>()?;
    module.add_class::<relation::Relation>()?;
    module.add_class::<result::ResultSet>()?;
    module.add_function(wrap_pyfunction!(connection::connect, module)?)?;
    module.add_function(wrap_pyfunction!(engine_version, module)?)?;
    Ok(())
}
