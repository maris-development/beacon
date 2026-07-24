//! The materialized result of a statement, and the two ways Python gets at it.
//!
//! - **Columnar** (`__arrow_c_stream__`): the Arrow PyCapsule protocol. Any Arrow consumer —
//!   pyarrow, polars, duckdb, nanoarrow, lonboard — ingests this with no conversion and no
//!   hard pyarrow dependency on our side. This is the path that should carry real data.
//! - **Row tuples** (`fetchone`/`fetchmany`/`fetchall`): PEP 249. This is the only place that
//!   converts Arrow values into Python objects one at a time, so it is the only place that
//!   needs a type table.
//!
//! Batches are collected before the result reaches Python (with the GIL released), rather than
//! streamed lazily through the capsule. That trades peak memory for a guarantee: no engine
//! work happens inside an FFI callback, where the consumer holds the GIL and we could not
//! release it. Streaming results (`fetch_record_batch`) come later and will need their own
//! GIL-aware bridge.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, SchemaRef, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList, PyTuple};

use crate::errors::DataError;

/// Python constructors for temporal and decimal values, imported once per fetch call rather
/// than once per cell. Built through the `datetime`/`decimal` modules instead of pyo3's typed
/// APIs so the extension stays inside the abi3 limited API.
struct Converters<'py> {
    epoch_date: Bound<'py, PyAny>,
    epoch_naive: Bound<'py, PyAny>,
    timedelta: Bound<'py, PyAny>,
    time: Bound<'py, PyAny>,
    utc: Bound<'py, PyAny>,
    decimal: Bound<'py, PyAny>,
}

impl<'py> Converters<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        let date_cls = datetime.getattr("date")?;
        let datetime_cls = datetime.getattr("datetime")?;
        let timezone = datetime.getattr("timezone")?;
        Ok(Self {
            epoch_date: date_cls.call1((1970, 1, 1))?,
            epoch_naive: datetime_cls.call1((1970, 1, 1))?,
            timedelta: datetime.getattr("timedelta")?,
            time: datetime.getattr("time")?,
            utc: timezone.getattr("utc")?,
            decimal: py.import("decimal")?.getattr("Decimal")?,
        })
    }

    /// `datetime.timedelta(**{unit: value})` — the constructor takes i64-safe keyword units,
    /// which avoids hand-rolling day/second/microsecond math.
    fn delta(&self, py: Python<'py>, unit: &str, value: i64) -> PyResult<Bound<'py, PyAny>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item(unit, value)?;
        self.timedelta.call((), Some(&kwargs))
    }

    fn date(&self, py: Python<'py>, days: i64) -> PyResult<Bound<'py, PyAny>> {
        let delta = self.delta(py, "days", days)?;
        self.epoch_date.call_method1("__add__", (delta,))
    }

    fn timestamp(
        &self,
        py: Python<'py>,
        value: i64,
        unit: &TimeUnit,
        tz: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Nanoseconds are converted to microseconds because Python's datetime has no finer
        // resolution; sub-microsecond precision is lost here (as it is in pandas' object path).
        let delta = match unit {
            TimeUnit::Second => self.delta(py, "seconds", value)?,
            TimeUnit::Millisecond => self.delta(py, "milliseconds", value)?,
            TimeUnit::Microsecond => self.delta(py, "microseconds", value)?,
            TimeUnit::Nanosecond => self.delta(py, "microseconds", value / 1_000)?,
        };
        let naive = self.epoch_naive.call_method1("__add__", (delta,))?;
        match tz {
            // Arrow timestamps with a timezone are UTC instants, so the value is attached to
            // UTC and the caller converts. Reconstructing the *named* zone would need a tzdata
            // lookup, and guessing it would be worse than reporting the instant correctly.
            Some(_) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("tzinfo", &self.utc)?;
                naive.call_method("replace", (), Some(&kwargs))
            }
            None => Ok(naive),
        }
    }

    fn time_of_day(&self, micros: i64) -> PyResult<Bound<'py, PyAny>> {
        let (hours, rest) = (micros / 3_600_000_000, micros % 3_600_000_000);
        let (minutes, rest) = (rest / 60_000_000, rest % 60_000_000);
        let (seconds, micros) = (rest / 1_000_000, rest % 1_000_000);
        self.time.call1((hours, minutes, seconds, micros))
    }
}

/// A statement's result: the schema, the rows, and a cursor into them.
#[pyclass(module = "beacondb", name = "Result")]
pub struct ResultSet {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    /// Position of the next row to hand out, as (batch, row-within-batch).
    position: (usize, usize),
    /// Rows affected, for statements that report it (DML). `None` for result sets.
    affected_rows: Option<u64>,
}

impl ResultSet {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            position: (0, 0),
            affected_rows: None,
        }
    }

    pub fn with_affected_rows(mut self, rows: Option<u64>) -> Self {
        self.affected_rows = rows;
        self
    }

    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Advances the cursor by one row, returning its (batch, row) coordinates.
    fn next_position(&mut self) -> Option<(usize, usize)> {
        let (mut batch, mut row) = self.position;
        while batch < self.batches.len() {
            if row < self.batches[batch].num_rows() {
                self.position = (batch, row + 1);
                return Some((batch, row));
            }
            batch += 1;
            row = 0;
        }
        self.position = (batch, row);
        None
    }

    fn row_to_tuple<'py>(
        &self,
        py: Python<'py>,
        batch: usize,
        row: usize,
        converters: &Converters<'py>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let batch = &self.batches[batch];
        let mut values = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            values.push(value_to_py(py, column.as_ref(), row, converters)?);
        }
        PyTuple::new(py, values)
    }

    /// Builds the Arrow C Data Interface capsule over this result's batches.
    ///
    /// Inherent (not a `#[pymethod]`) so [`crate::relation::Relation`] can build a capsule from
    /// its own materialized result without going through a `Bound<ResultSet>`.
    pub fn arrow_c_stream<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let reader = RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        );
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        // The capsule name is fixed by the Arrow PyCapsule specification; consumers dispatch on
        // it. The capsule owns the stream and releases it if nobody consumes it.
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }

    /// This result's Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// This result's batches, for callers that format or inspect them directly.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

#[pymethods]
impl ResultSet {
    /// The Arrow PyCapsule stream interface — how every columnar consumer reads this result.
    ///
    /// `requested_schema` is accepted and ignored: the protocol allows a producer to return
    /// its own schema when it cannot cast, and casting here would hide a conversion the caller
    /// can do explicitly.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        self.arrow_c_stream(py)
    }

    /// PEP 249 `description`: a 7-tuple per column. Only name, type and null_ok are meaningful
    /// for a columnar engine; the size/precision slots stay `None`, as DuckDB's do.
    #[getter]
    pub fn description<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let entry = PyTuple::new(
                py,
                [
                    field.name().into_pyobject(py)?.into_any().unbind(),
                    field.data_type().to_string().into_pyobject(py)?.into_any().unbind(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    field.is_nullable().into_pyobject(py)?.to_owned().into_any().unbind(),
                ],
            )?;
            columns.push(entry);
        }
        PyList::new(py, columns)
    }

    /// PEP 249 `rowcount`: rows affected for DML, or the row count of a materialized result.
    #[getter]
    pub fn rowcount(&self) -> i64 {
        match self.affected_rows {
            Some(rows) => rows as i64,
            None => self.total_rows() as i64,
        }
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    #[getter]
    fn types(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect()
    }

    fn __len__(&self) -> usize {
        self.total_rows()
    }

    pub fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let converters = Converters::new(py)?;
        match self.next_position() {
            Some((batch, row)) => Ok(Some(self.row_to_tuple(py, batch, row, &converters)?)),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (size=1))]
    pub fn fetchmany<'py>(&mut self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyList>> {
        let converters = Converters::new(py)?;
        let mut rows = Vec::with_capacity(size);
        for _ in 0..size {
            match self.next_position() {
                Some((batch, row)) => rows.push(self.row_to_tuple(py, batch, row, &converters)?),
                None => break,
            }
        }
        PyList::new(py, rows)
    }

    pub fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let converters = Converters::new(py)?;
        let mut rows = Vec::with_capacity(self.total_rows());
        while let Some((batch, row)) = self.next_position() {
            rows.push(self.row_to_tuple(py, batch, row, &converters)?);
        }
        PyList::new(py, rows)
    }

    /// The result as a `pyarrow.Table`.
    fn arrow<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;
        // `pyarrow.table()` consumes `__arrow_c_stream__` directly (pyarrow >= 14), so no
        // conversion happens on our side.
        pyarrow.call_method1("table", (slf,))
    }

    /// The result as a `pandas.DataFrame`.
    fn df<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        import_or_hint(py, "pandas", "beacondb[pandas]")?;
        Self::arrow(slf)?.call_method0("to_pandas")
    }

    /// Alias of [`Self::df`], matching DuckDB's `fetchdf`.
    fn fetchdf<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::df(slf)
    }

    /// The result as a `polars.DataFrame`.
    fn pl<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let polars = import_or_hint(py, "polars", "beacondb[polars]")?;
        polars.call_method1("DataFrame", (slf,))
    }

    fn __repr__(&self) -> String {
        format!(
            "<beacondb.Result {} rows x {} columns>",
            self.total_rows(),
            self.schema.fields().len()
        )
    }
}

/// Imports an optional interop dependency, or explains how to install it.
///
/// These are optional on purpose: results cross into Python over the Arrow PyCapsule protocol,
/// so nothing here is needed until a caller asks for a specific library's type.
pub(crate) fn import_or_hint<'py>(
    py: Python<'py>,
    module: &str,
    extra: &str,
) -> PyResult<Bound<'py, PyModule>> {
    py.import(module).map_err(|_| {
        pyo3::exceptions::PyImportError::new_err(format!(
            "`{module}` is required for this method but is not installed. Install it with \
             `pip install {extra}`, or read the result with any Arrow consumer via the \
             `__arrow_c_stream__` protocol, which needs no extra dependency."
        ))
    })
}

/// Converts one Arrow value to a Python object.
///
/// Unsupported types raise `DataError` naming the type and pointing at the columnar path,
/// which handles every Arrow type without conversion — rather than silently stringifying.
fn value_to_py<'py>(
    py: Python<'py>,
    array: &dyn Array,
    row: usize,
    converters: &Converters<'py>,
) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }

    macro_rules! primitive {
        ($arrow_ty:ty) => {
            array
                .as_primitive::<$arrow_ty>()
                .value(row)
                .into_pyobject(py)?
                .into_any()
                .unbind()
        };
    }

    let value = match array.data_type() {
        DataType::Null => py.None(),
        DataType::Boolean => array
            .as_boolean()
            .value(row)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind(),

        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Float16 => array
            .as_primitive::<Float16Type>()
            .value(row)
            .to_f64()
            .into_pyobject(py)?
            .into_any()
            .unbind(),

        DataType::Utf8 => array.as_string::<i32>().value(row).into_pyobject(py)?.into_any().unbind(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).into_pyobject(py)?.into_any().unbind(),
        DataType::Utf8View => array.as_string_view().value(row).into_pyobject(py)?.into_any().unbind(),

        DataType::Binary => array.as_binary::<i32>().value(row).into_pyobject(py)?.into_any().unbind(),
        DataType::LargeBinary => array.as_binary::<i64>().value(row).into_pyobject(py)?.into_any().unbind(),
        DataType::BinaryView => array.as_binary_view().value(row).into_pyobject(py)?.into_any().unbind(),
        DataType::FixedSizeBinary(_) => array
            .as_fixed_size_binary()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),

        DataType::Date32 => converters
            .date(py, array.as_primitive::<Date32Type>().value(row) as i64)?
            .unbind(),
        // Date64 is milliseconds since the epoch, not days.
        DataType::Date64 => {
            let millis = array.as_primitive::<Date64Type>().value(row);
            converters.date(py, millis.div_euclid(86_400_000))?.unbind()
        }

        DataType::Timestamp(unit, tz) => {
            let value = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            converters
                .timestamp(py, value, unit, tz.as_ref().map(|tz| tz.as_ref()))?
                .unbind()
        }

        DataType::Time32(unit) => {
            let micros = match unit {
                TimeUnit::Second => array.as_primitive::<Time32SecondType>().value(row) as i64 * 1_000_000,
                _ => array.as_primitive::<Time32MillisecondType>().value(row) as i64 * 1_000,
            };
            converters.time_of_day(micros)?.unbind()
        }
        DataType::Time64(unit) => {
            let micros = match unit {
                TimeUnit::Nanosecond => array.as_primitive::<Time64NanosecondType>().value(row) / 1_000,
                _ => array.as_primitive::<Time64MicrosecondType>().value(row),
            };
            converters.time_of_day(micros)?.unbind()
        }

        DataType::Duration(unit) => {
            let (name, value) = match unit {
                TimeUnit::Second => ("seconds", array.as_primitive::<DurationSecondType>().value(row)),
                TimeUnit::Millisecond => (
                    "milliseconds",
                    array.as_primitive::<DurationMillisecondType>().value(row),
                ),
                TimeUnit::Microsecond => (
                    "microseconds",
                    array.as_primitive::<DurationMicrosecondType>().value(row),
                ),
                TimeUnit::Nanosecond => (
                    "microseconds",
                    array.as_primitive::<DurationNanosecondType>().value(row) / 1_000,
                ),
            };
            converters.delta(py, name, value)?.unbind()
        }

        // Decimals go through their exact decimal string: binary floats cannot represent them,
        // and `Decimal(str)` is the only lossless constructor.
        DataType::Decimal128(..) | DataType::Decimal256(..) => {
            let text = arrow::util::display::array_value_to_string(array, row)
                .map_err(|e| DataError::new_err(format!("decimal value: {e}")))?;
            converters.decimal.call1((text,))?.unbind()
        }

        other => {
            return Err(DataError::new_err(format!(
                "cannot convert Arrow type `{other}` to a Python row value; read this result \
                 columnar instead (`.arrow()`, `.df()`, `.pl()`), which needs no conversion"
            )));
        }
    };

    Ok(value)
}
