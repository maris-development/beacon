#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, PrimitiveArray, StringArray};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use beacon_binary_format::collection::CollectionReader;
use beacon_binary_format::collection_partition::{
    CollectionPartitionReadOptions, CollectionPartitionReader,
};
use beacon_binary_format::error::BBFError;
use beacon_binary_format::io_cache::ArrayIoCache;
use futures::StreamExt;
use nd_arrow_array::{NdArrowArray, batch::NdRecordBatch, dimensions::Dimensions};
use numpy::PyArray1;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyString, PyTuple};

use crate::utils::{StorageOptions, init_store, prepare_store_inputs, to_py_err};

const DEFAULT_CACHE_SIZE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 32;
const ENTRY_KEY_FIELD: &str = "__entry_key";

struct ReaderInner {
    runtime: Arc<tokio::runtime::Runtime>,
    reader: CollectionReader,
}

impl ReaderInner {
    fn new(
        base_dir: String,
        collection_path: String,
        cache_bytes: Option<usize>,
        storage: StorageOptions,
    ) -> PyResult<Self> {
        let runtime =
            Arc::new(tokio::runtime::Runtime::new().map_err(|err| {
                PyRuntimeError::new_err(format!("failed to start runtime: {err}"))
            })?);
        let store = init_store(base_dir, storage)?;
        let path = store.resolve_collection_path(&collection_path)?;
        let cache = ArrayIoCache::new(cache_bytes.unwrap_or(DEFAULT_CACHE_SIZE_BYTES));
        let reader = runtime
            .block_on(CollectionReader::new(store.store.clone(), path, cache))
            .map_err(to_py_err)?;
        Ok(Self { runtime, reader })
    }
}

impl ReaderInner {
    fn metadata(&self) -> &beacon_binary_format::collection::CollectionMetadata {
        self.reader.metadata()
    }
}

#[pyclass(name = "CollectionReader")]
#[derive(Clone)]
pub struct CollectionReaderHandle {
    inner: Arc<ReaderInner>,
}

#[pymethods]
impl CollectionReaderHandle {
    #[new]
    #[pyo3(signature = (base_dir, collection_path, cache_bytes=None, storage_options=None, filesystem=None))]
    pub fn new(
        base_dir: String,
        collection_path: String,
        cache_bytes: Option<usize>,
        storage_options: Option<Bound<'_, PyDict>>,
        filesystem: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let (normalized_base, storage) =
            prepare_store_inputs(base_dir, storage_options, filesystem)?;
        Ok(Self {
            inner: Arc::new(ReaderInner::new(
                normalized_base,
                collection_path,
                cache_bytes,
                storage,
            )?),
        })
    }

    pub fn metadata(&self, py: Python<'_>) -> PyResult<PyObject> {
        let meta = self.inner.metadata();
        let dict = PyDict::new(py);
        dict.set_item("collection_byte_size", meta.collection_byte_size)?;
        dict.set_item("collection_num_elements", meta.collection_num_elements)?;
        dict.set_item("partition_count", meta.partitions.len())?;
        dict.set_item("library_version", meta.library_version.clone())?;
        Ok(dict.into())
    }

    pub fn partition_names(&self) -> Vec<String> {
        self.inner.metadata().partitions.keys().cloned().collect()
    }

    pub fn open_partition(&self, partition_name: &str) -> PyResult<PartitionReaderHandle> {
        let partition_reader = self
            .inner
            .reader
            .partition_reader(partition_name)
            .ok_or_else(|| {
                PyValueError::new_err(format!("unknown partition '{partition_name}'"))
            })?;
        Ok(PartitionReaderHandle {
            collection: self.clone(),
            reader: partition_reader,
            partition_name: partition_name.to_string(),
        })
    }
}

#[pyclass(name = "PartitionReader")]
pub struct PartitionReaderHandle {
    collection: CollectionReaderHandle,
    reader: CollectionPartitionReader,
    partition_name: String,
}

#[pymethods]
impl PartitionReaderHandle {
    pub fn name(&self) -> &str {
        &self.partition_name
    }

    pub fn num_entries(&self) -> usize {
        self.reader.metadata.num_entries
    }

    #[pyo3(signature = (projection=None, max_concurrent_reads=None))]
    pub fn read_entries(
        &self,
        py: Python<'_>,
        projection: Option<Vec<String>>,
        max_concurrent_reads: Option<usize>,
    ) -> PyResult<Vec<PyObject>> {
        let projection = self.normalize_projection(projection);
        let options = CollectionPartitionReadOptions {
            max_concurrent_reads: max_concurrent_reads.unwrap_or(DEFAULT_MAX_CONCURRENCY),
        };
        let scheduler = self
            .collection
            .inner
            .runtime
            .block_on(self.reader.read_indexed(projection, options))
            .map_err(to_py_err)?;

        let total_entries = self.reader.metadata.num_entries;
        let batches = self
            .collection
            .inner
            .runtime
            .block_on(async {
                let mut stream = scheduler.shared_pollable_stream_ref().await;
                let mut ordered = vec![None; total_entries];
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok((index, batch)) => ordered[index] = Some(batch),
                        Err(err) => return Err(err),
                    }
                }
                Ok::<_, BBFError>(ordered)
            })
            .map_err(to_py_err)?;

        let mut entries = Vec::with_capacity(total_entries);
        for maybe_batch in batches {
            let batch = maybe_batch.ok_or_else(|| {
                PyRuntimeError::new_err("partition reader returned incomplete results")
            })?;
            entries.push(batch_to_python(py, &batch)?);
        }
        Ok(entries)
    }
}

impl PartitionReaderHandle {
    fn normalize_projection(&self, projection: Option<Vec<String>>) -> Option<Arc<[String]>> {
        projection.map(|mut names| {
            if !names.iter().any(|name| name == ENTRY_KEY_FIELD) {
                names.push(ENTRY_KEY_FIELD.to_string());
            }
            Arc::<[String]>::from(names.into_boxed_slice())
        })
    }
}

fn batch_to_python(py: Python<'_>, batch: &NdRecordBatch) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (field, array) in batch.schema().fields().iter().zip(batch.arrays().iter()) {
        let payload = array_to_python(py, array)?;
        dict.set_item(field.name(), payload)?;
    }
    Ok(dict.into())
}

fn array_to_python(py: Python<'_>, array: &NdArrowArray) -> PyResult<PyObject> {
    let dims = DimensionMetadata::from_nd(array);
    let arrow_array = array.as_arrow_array();
    match arrow_array.data_type() {
        DataType::Null => build_null_payload(py, &dims),
        DataType::Boolean => {
            let bool_array = arrow_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray");
            let conversion = boolean_array_to_numpy(py, bool_array)?;
            finalize_array(py, conversion, &dims, None)
        }
        DataType::Int8 => primitive_payload::<Int8Type>(py, arrow_array, &dims, None),
        DataType::Int16 => primitive_payload::<Int16Type>(py, arrow_array, &dims, None),
        DataType::Int32 => primitive_payload::<Int32Type>(py, arrow_array, &dims, None),
        DataType::Int64 => primitive_payload::<Int64Type>(py, arrow_array, &dims, None),
        DataType::UInt8 => primitive_payload::<UInt8Type>(py, arrow_array, &dims, None),
        DataType::UInt16 => primitive_payload::<UInt16Type>(py, arrow_array, &dims, None),
        DataType::UInt32 => primitive_payload::<UInt32Type>(py, arrow_array, &dims, None),
        DataType::UInt64 => primitive_payload::<UInt64Type>(py, arrow_array, &dims, None),
        DataType::Float32 => primitive_payload::<Float32Type>(py, arrow_array, &dims, None),
        DataType::Float64 => primitive_payload::<Float64Type>(py, arrow_array, &dims, None),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive_payload::<TimestampNanosecondType>(
                py,
                arrow_array,
                &dims,
                Some(ValueTransform::DatetimeNs),
            )
        }
        DataType::Timestamp(unit, _) => Err(PyValueError::new_err(format!(
            "unsupported timestamp unit: {unit:?}"
        ))),
        DataType::Utf8 => {
            let string_array = arrow_array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray");
            let conversion = string_array_to_numpy(py, string_array)?;
            finalize_array(py, conversion, &dims, None)
        }
        DataType::Binary => {
            let binary_array = arrow_array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("BinaryArray");
            let conversion = binary_array_to_numpy(py, binary_array)?;
            finalize_array(py, conversion, &dims, None)
        }
        other => Err(PyValueError::new_err(format!(
            "unsupported Arrow data type: {other:?}"
        ))),
    }
}

enum ValueTransform {
    DatetimeNs,
}

fn primitive_payload<T>(
    py: Python<'_>,
    array_ref: &ArrayRef,
    dims: &DimensionMetadata,
    transform: Option<ValueTransform>,
) -> PyResult<PyObject>
where
    T: ArrowPrimitiveType,
    T::Native: numpy::Element + Default + Copy,
{
    let primitive_array = array_ref
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("primitive array");
    let conversion = primitive_array_to_numpy(py, primitive_array)?;
    finalize_array(py, conversion, dims, transform)
}

fn finalize_array(
    py: Python<'_>,
    conversion: NumpyConversion,
    dims: &DimensionMetadata,
    transform: Option<ValueTransform>,
) -> PyResult<PyObject> {
    let reshaped_data = reshape_array_object(py, conversion.data, dims)?;
    let reshaped_mask = match conversion.mask {
        Some(mask) => Some(reshape_array_object(py, mask, dims)?),
        None => None,
    };

    let transformed = match transform {
        Some(ValueTransform::DatetimeNs) => {
            let view = reshaped_data
                .bind(py)
                .call_method1("view", ("datetime64[ns]",))?;
            view.into()
        }
        None => reshaped_data,
    };

    let masked = apply_mask(py, transformed, reshaped_mask)?;
    build_array_payload(py, dims, masked)
}

fn build_null_payload(py: Python<'_>, dims: &DimensionMetadata) -> PyResult<PyObject> {
    let total = dims.element_count();
    let zeros = vec![0f64; total];
    let mask = vec![true; total];
    let data = PyArray1::from_vec(py, zeros).into_any().unbind();
    let mask_obj = PyArray1::from_vec(py, mask).into_any().unbind();
    let reshaped_data = reshape_array_object(py, data, dims)?;
    let reshaped_mask = reshape_array_object(py, mask_obj, dims)?;
    let masked = apply_mask(py, reshaped_data, Some(reshaped_mask))?;
    build_array_payload(py, dims, masked)
}

struct NumpyConversion {
    data: PyObject,
    mask: Option<PyObject>,
}

fn primitive_array_to_numpy<T>(
    py: Python<'_>,
    array: &PrimitiveArray<T>,
) -> PyResult<NumpyConversion>
where
    T: ArrowPrimitiveType,
    T::Native: numpy::Element + Default + Copy,
{
    let len = array.len();
    let mut values = Vec::with_capacity(len);
    let mut mask = if array.null_count() > 0 {
        Some(vec![false; len])
    } else {
        None
    };
    for (idx, value) in array.iter().enumerate() {
        match value {
            Some(v) => values.push(v),
            None => {
                values.push(T::Native::default());
                if let Some(bits) = mask.as_mut() {
                    bits[idx] = true;
                }
            }
        }
    }
    let data = PyArray1::from_vec(py, values).into_any().unbind();
    let mask_obj = mask.map(|bits| PyArray1::from_vec(py, bits).into_any().unbind());
    Ok(NumpyConversion {
        data,
        mask: mask_obj,
    })
}

fn boolean_array_to_numpy(py: Python<'_>, array: &BooleanArray) -> PyResult<NumpyConversion> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);
    let mut mask = if array.null_count() > 0 {
        Some(vec![false; len])
    } else {
        None
    };
    for idx in 0..len {
        if array.is_null(idx) {
            values.push(false);
            if let Some(bits) = mask.as_mut() {
                bits[idx] = true;
            }
        } else {
            values.push(array.value(idx));
        }
    }
    let data = PyArray1::from_vec(py, values).into_any().unbind();
    let mask_obj = mask.map(|bits| PyArray1::from_vec(py, bits).into_any().unbind());
    Ok(NumpyConversion {
        data,
        mask: mask_obj,
    })
}

fn string_array_to_numpy(py: Python<'_>, array: &StringArray) -> PyResult<NumpyConversion> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);
    let mut mask = if array.null_count() > 0 {
        Some(vec![false; len])
    } else {
        None
    };
    for idx in 0..len {
        if array.is_null(idx) {
            values.push(String::new());
            if let Some(bits) = mask.as_mut() {
                bits[idx] = true;
            }
        } else {
            values.push(array.value(idx).to_string());
        }
    }
    let py_values: Vec<PyObject> = values
        .iter()
        .map(|value| PyString::new(py, value).into())
        .collect();
    let list = PyList::new(py, py_values)?;
    let numpy = py.import("numpy")?;
    let data = numpy.getattr("array")?.call1((list,))?;
    let mask_obj = mask.map(|bits| PyArray1::from_vec(py, bits).into_any().unbind());
    Ok(NumpyConversion {
        data: data.into(),
        mask: mask_obj,
    })
}

fn binary_array_to_numpy(py: Python<'_>, array: &BinaryArray) -> PyResult<NumpyConversion> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);
    let mut mask = if array.null_count() > 0 {
        Some(vec![false; len])
    } else {
        None
    };
    for idx in 0..len {
        if array.is_null(idx) {
            values.push(Vec::new());
            if let Some(bits) = mask.as_mut() {
                bits[idx] = true;
            }
        } else {
            values.push(array.value(idx).to_vec());
        }
    }
    let py_values: Vec<PyObject> = values
        .iter()
        .map(|value| PyBytes::new(py, value).into())
        .collect();
    let list = PyList::new(py, py_values)?;
    let numpy = py.import("numpy")?;
    let data = numpy.getattr("array")?.call1((list,))?;
    let mask_obj = mask.map(|bits| PyArray1::from_vec(py, bits).into_any().unbind());
    Ok(NumpyConversion {
        data: data.into(),
        mask: mask_obj,
    })
}

fn reshape_array_object(
    py: Python<'_>,
    array: PyObject,
    dims: &DimensionMetadata,
) -> PyResult<PyObject> {
    let shape = shape_tuple(py, &dims.shape)?;
    let reshaped = array.bind(py).call_method1("reshape", (shape,))?;
    Ok(reshaped.into())
}

fn shape_tuple<'py>(py: Python<'py>, dims: &[usize]) -> PyResult<Bound<'py, PyTuple>> {
    if dims.is_empty() {
        Ok(PyTuple::empty(py))
    } else {
        let converted = dims
            .iter()
            .map(|size| {
                isize::try_from(*size)
                    .map_err(|_| PyValueError::new_err("dimension size exceeds platform limits"))
            })
            .collect::<PyResult<Vec<isize>>>()?;
        PyTuple::new(py, converted)
    }
}

fn apply_mask(py: Python<'_>, data: PyObject, mask: Option<PyObject>) -> PyResult<PyObject> {
    if let Some(mask_obj) = mask {
        let numpy = py.import("numpy")?;
        let ma = numpy.getattr("ma")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("mask", mask_obj)?;
        let masked = ma.call_method("array", (data,), Some(&kwargs))?;
        Ok(masked.into())
    } else {
        Ok(data)
    }
}

fn build_array_payload(
    py: Python<'_>,
    dims: &DimensionMetadata,
    data: PyObject,
) -> PyResult<PyObject> {
    let payload = PyDict::new(py);
    payload.set_item("data", data)?;
    let dims_list = PyList::new(py, dims.names.iter().map(|name| name.as_str()))?;
    payload.set_item("dims", dims_list)?;
    let shape_tuple = PyTuple::new(py, dims.shape.iter().cloned())?;
    payload.set_item("shape", shape_tuple)?;
    Ok(payload.into())
}

struct DimensionMetadata {
    names: Vec<String>,
    shape: Vec<usize>,
}

impl DimensionMetadata {
    fn from_nd(array: &NdArrowArray) -> Self {
        match array.dimensions() {
            Dimensions::Scalar => Self {
                names: Vec::new(),
                shape: Vec::new(),
            },
            Dimensions::MultiDimensional(dims) => Self {
                names: dims.iter().map(|d| d.name.clone()).collect(),
                shape: dims.iter().map(|d| d.size).collect(),
            },
        }
    }

    fn element_count(&self) -> usize {
        if self.shape.is_empty() {
            1
        } else {
            self.shape.iter().product()
        }
    }
}
