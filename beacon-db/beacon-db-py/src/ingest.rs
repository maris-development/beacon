//! Pulling in-memory Python data into Arrow, for `Connection.register(...)`.
//!
//! Any of pandas / pyarrow / polars, a beacondb [`Relation`](crate::relation::Relation), or
//! anything exposing the Arrow PyCapsule protocol is normalized to a `pyarrow.Table`, serialized
//! to an Arrow IPC stream, and read back into `RecordBatch`es on the Rust side.
//!
//! Why route through IPC bytes rather than the C Data Interface: it needs no version-coupled
//! `arrow`↔`pyo3` FFI bridge (the `arrow` `pyarrow` feature pins a pyo3 that predates ours), and
//! `register()` copies the data into an owned `MemTable` regardless, so the serialize/parse pass
//! costs nothing the operation wasn't already paying. The one consequence: `register()` needs
//! `pyarrow` installed — reasonable for a method whose whole job is ingesting Python data.

use std::io::Cursor;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use pyo3::prelude::*;

use crate::errors::{map_engine_error, DataError};
use crate::result::import_or_hint;

/// Converts a Python object holding tabular data into an Arrow schema and batches.
pub fn arrow_from_py(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    let bytes = to_ipc_bytes(py, obj)?;
    read_ipc(&bytes)
}

/// Serializes any supported Python object to an Arrow IPC stream via pyarrow.
///
/// `pyarrow.table(obj)` accepts a pandas DataFrame, a pyarrow `Table`, a mapping of arrays, and
/// (pyarrow ≥ 14) any object implementing `__arrow_c_stream__` — which is polars frames and
/// beacondb `Relation`s. Inputs it rejects surface pyarrow's own error, which names the type.
fn to_ipc_bytes(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;

    let table = pyarrow.call_method1("table", (obj,)).map_err(|err| {
        DataError::new_err(format!(
            "could not read `{}` as a table; register accepts a pandas/pyarrow/polars frame, a \
             beacondb relation, or an Arrow-C-stream object ({err})",
            type_name(obj)
        ))
    })?;

    // Write the table to an in-memory Arrow IPC stream: new_stream(sink, schema) -> write -> close.
    let sink = pyarrow.call_method0("BufferOutputStream")?;
    let schema = table.getattr("schema")?;
    let writer = pyarrow
        .getattr("ipc")?
        .call_method1("new_stream", (&sink, schema))?;
    writer.call_method1("write_table", (&table,))?;
    writer.call_method0("close")?;

    let buffer = sink.call_method0("getvalue")?;
    buffer.call_method0("to_pybytes")?.extract::<Vec<u8>>()
}

/// Reads an Arrow IPC stream into its schema and batches.
fn read_ipc(bytes: &[u8]) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| map_engine_error(anyhow::anyhow!("invalid Arrow IPC data: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| map_engine_error(anyhow::anyhow!("failed to read Arrow IPC batches: {e}")))?;
    Ok((schema, batches))
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "?".to_string())
}
