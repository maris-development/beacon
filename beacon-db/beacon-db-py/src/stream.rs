//! Streaming a relation's result to Python one batch at a time.
//!
//! Unlike the collected [`crate::result::ResultSet`], this never holds the whole result: each
//! `__next__` polls the engine for the next batch with the GIL released (via [`block_on`]), so a
//! query over a huge file stays memory-bounded. Batches cross into pyarrow over the Arrow C data
//! interface (`__arrow_c_array__` / `__arrow_c_schema__`), so there is no hard pyarrow build
//! dependency — only a runtime one, for the `pyarrow.RecordBatchReader` this hands back.
//!
//! The stream is a beacon [`ArrowOutputStream`], so pulling batches this way still records the
//! query's output metrics, exactly as the collected path does.

use std::sync::Arc;

use arrow::array::{Array, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use beacon_core::embedded::Database;
use beacon_core::query_result::ArrowOutputStream;
use beacon_core::AuthIdentity;
use futures::StreamExt;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::errors::{interface_error, map_engine_error, programming_error};
use crate::result::import_or_hint;
use crate::runtime::block_on;

/// Builds a streaming `pyarrow.RecordBatchReader` over `sql`.
///
/// The query is planned and a lazy stream opened here (no rows are pulled yet); the reader pulls
/// batches on demand as it is iterated. `batch_size`, if given, re-chunks to roughly that many
/// rows per batch; omitted, the engine's native batches are handed through untouched.
pub fn record_batch_reader<'py>(
    py: Python<'py>,
    database: &Arc<Database>,
    identity: &AuthIdentity,
    sql: String,
    batch_size: Option<usize>,
) -> PyResult<Bound<'py, PyAny>> {
    if batch_size == Some(0) {
        return Err(programming_error("batch_size must be a positive integer"));
    }

    let database = database.clone();
    let identity = identity.clone();
    let stream = block_on(py, async move {
        database.sql(sql, identity).await?.into_record_stream()
    })?
    .map_err(map_engine_error)?;
    let schema = stream.schema();

    let batch_stream = Bound::new(
        py,
        BatchStream {
            stream: Some(stream),
            schema: schema.clone(),
            target: batch_size,
            buffer: Vec::new(),
            buffered_rows: 0,
        },
    )?;

    let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;
    let pa_schema = schema_to_pyarrow(py, &schema)?;
    // `from_batches` pulls from our iterator lazily, so the reader is genuinely streaming.
    pyarrow
        .getattr("RecordBatchReader")?
        .call_method1("from_batches", (pa_schema, batch_stream))
}

/// A lazily-pulled stream of record batches, optionally re-chunked to a target row count.
///
/// `unsendable`: the underlying engine stream is `Send` but not `Sync`, and a batch cursor is a
/// single-consumer object anyway (two threads pulling from one stream would interleave batches
/// nonsensically), so it is pinned to the thread that created it.
#[pyclass(module = "beacondb", name = "BatchStream", unsendable)]
struct BatchStream {
    /// The engine stream, or `None` once it is exhausted (or errored).
    stream: Option<ArrowOutputStream>,
    schema: SchemaRef,
    /// Rows per output batch; `None` means hand back native batches unchanged.
    target: Option<usize>,
    /// Native batches accumulated toward the next `target`-sized output batch.
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
}

impl BatchStream {
    /// Pulls the next native batch from the engine, releasing the GIL while it runs.
    fn pull_native(&mut self, py: Python<'_>) -> PyResult<Option<RecordBatch>> {
        let next = {
            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            block_on(py, stream.next())?
        };
        match next {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                self.stream = None;
                Err(map_engine_error(e.into()))
            }
            None => {
                self.stream = None;
                Ok(None)
            }
        }
    }

    /// The next output batch (honoring `target`), or `None` at end of stream.
    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<RecordBatch>> {
        let Some(target) = self.target else {
            // No re-chunking: hand back the engine's native batches (zero-copy).
            return self.pull_native(py);
        };
        while self.buffered_rows < target && self.stream.is_some() {
            match self.pull_native(py)? {
                Some(batch) => {
                    self.buffered_rows += batch.num_rows();
                    self.buffer.push(batch);
                }
                None => break,
            }
        }
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let combined =
            concat_batches(&self.schema, &self.buffer).map_err(|e| map_engine_error(e.into()))?;
        self.buffer.clear();
        self.buffered_rows = 0;
        let take = target.min(combined.num_rows());
        let out = combined.slice(0, take);
        // Carry the overflow (a native batch can exceed `target`) into the next pull.
        if take < combined.num_rows() {
            let rest = combined.slice(take, combined.num_rows() - take);
            self.buffered_rows = rest.num_rows();
            self.buffer.push(rest);
        }
        Ok(Some(out))
    }
}

#[pymethods]
impl BatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(mut slf: PyRefMut<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        match slf.next_batch(py)? {
            Some(batch) => {
                let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;
                let holder = Bound::new(py, BatchCapsule { batch: Some(batch) })?;
                pyarrow.call_method1("record_batch", (holder,))
            }
            None => Err(PyStopIteration::new_err(())),
        }
    }
}

/// A single record batch, exported to pyarrow via the Arrow C array interface. `pa.record_batch()`
/// consumes `__arrow_c_array__` (a struct array is a record batch), so no pyarrow build dependency
/// is needed.
#[pyclass(module = "beacondb")]
struct BatchCapsule {
    /// Taken on the first (and only) export — the C array interface moves ownership out to Python.
    batch: Option<RecordBatch>,
}

#[pymethods]
impl BatchCapsule {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &mut self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let _ = requested_schema;
        let batch = self
            .batch
            .take()
            .ok_or_else(|| interface_error("this record batch has already been consumed"))?;
        let array = StructArray::from(batch);
        let (ffi_array, ffi_schema) =
            to_ffi(&array.to_data()).map_err(|e| map_engine_error(e.into()))?;
        // Capsule names are fixed by the Arrow PyCapsule specification; the consumer dispatches on
        // them, and each capsule owns its FFI struct (freeing it if nobody consumes it).
        let schema_capsule = PyCapsule::new_with_value(py, ffi_schema, c"arrow_schema")?;
        let array_capsule = PyCapsule::new_with_value(py, ffi_array, c"arrow_array")?;
        PyTuple::new(
            py,
            [schema_capsule.into_any(), array_capsule.into_any()],
        )
    }
}

/// Turns an Arrow schema into a `pyarrow.Schema` over the C schema interface.
fn schema_to_pyarrow<'py>(py: Python<'py>, schema: &SchemaRef) -> PyResult<Bound<'py, PyAny>> {
    let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;
    let holder = Bound::new(py, SchemaCapsule { schema: schema.clone() })?;
    pyarrow.call_method1("schema", (holder,))
}

/// An Arrow schema, exported to pyarrow via the Arrow C schema interface.
#[pyclass(module = "beacondb")]
struct SchemaCapsule {
    schema: SchemaRef,
}

#[pymethods]
impl SchemaCapsule {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi = FFI_ArrowSchema::try_from(self.schema.as_ref())
            .map_err(|e| map_engine_error(e.into()))?;
        PyCapsule::new_with_value(py, ffi, c"arrow_schema")
    }
}
