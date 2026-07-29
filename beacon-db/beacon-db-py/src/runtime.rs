//! The process-wide Tokio runtime, and the rule for crossing into it from Python.
//!
//! beacon-core is async top to bottom and hands its `Handle` to long-lived subsystems (table
//! functions, the crawler manager, `RedbStore`'s `spawn_blocking` calls), so the runtime must
//! outlive every `Database`. One `OnceLock` runtime per process does that, and is also what
//! keeps a notebook from accumulating a thread pool per `connect()`.
//!
//! Every blocking call goes through [`block_on`], which releases the GIL for the duration.
//! Without that, a long scan freezes every other Python thread in the process.

use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;

use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};

use crate::errors::{interface_error, InternalError};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// The shared runtime, built on first use.
pub fn runtime() -> PyResult<&'static Runtime> {
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }
    let rt = Builder::new_multi_thread()
        .enable_all()
        .thread_name("beacondb")
        .build()
        .map_err(|e| interface_error(format!("failed to start the beacondb runtime: {e}")))?;
    // A concurrent initializer may have won; either runtime is equally valid, and the loser is
    // dropped here rather than leaked.
    let _ = RUNTIME.set(rt);
    RUNTIME
        .get()
        .ok_or_else(|| interface_error("the beacondb runtime disappeared after initialization"))
}

/// Runs `future` to completion on the shared runtime with the GIL released.
///
/// Safe to call from any Python thread: Python threads are never Tokio worker threads, so this
/// cannot deadlock a worker by blocking inside the runtime.
///
/// A panic inside the future is **caught and turned into an [`InternalError`]** rather than
/// allowed to unwind across the FFI boundary — an engine assertion failure must surface as a
/// catchable Python exception, never as an interpreter abort.
pub fn block_on<F>(py: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    let rt = runtime()?;
    let outcome =
        py.detach(|| std::panic::catch_unwind(AssertUnwindSafe(|| rt.block_on(future))));
    outcome.map_err(panic_to_error)
}

/// Renders a caught panic payload as an [`InternalError`], recovering the message where the
/// payload is the usual `&str`/`String`.
fn panic_to_error(payload: Box<dyn std::any::Any + Send>) -> PyErr {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    InternalError::new_err(format!("internal engine error (panic caught): {message}"))
}
