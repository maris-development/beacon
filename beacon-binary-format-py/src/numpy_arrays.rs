use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
    UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use nd_arrow_array::NdArrowArray;
use nd_arrow_array::dimensions::{Dimension, Dimensions};
use numpy::{PyArrayDyn, PyArrayMethods, PyReadonlyArrayDyn};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PySequenceMethods;
use pyo3::types::{PyAny, PyBytes, PyDict, PyInt, PyList, PySequence, PyString, PyTuple};

macro_rules! try_numpy_array {
    ($value:expr, $ty:ty, $builder:ident, $field_name:expr, $dims:expr, $mask:expr) => {
        if let Ok(array) = $value.downcast::<PyArrayDyn<$ty>>() {
            return $builder($field_name, array.readonly(), $dims, $mask);
        }
    };
}

macro_rules! define_numeric_builder {
    ($fn_name:ident, $ty:ty, $builder:ty, $data_type:expr) => {
        fn $fn_name(
            field_name: &str,
            array: PyReadonlyArrayDyn<'_, $ty>,
            dimension_names: Option<&[String]>,
            mask: Option<&MaskValues>,
        ) -> PyResult<(FieldRef, NdArrowArray)> {
            let view = array.as_array();
            let mut builder = <$builder>::with_capacity(view.len());
            if let Some(mask_values) = mask {
                mask_values.ensure_len(view.len())?;
            }
            for (idx, value) in view.iter().enumerate() {
                if mask.map_or(false, |m| m.is_masked(idx)) {
                    builder.append_null();
                } else {
                    builder.append_value(*value);
                }
            }
            let dimensions = dimensions_from_shape(view.shape(), dimension_names)?;
            make_nd_array(
                field_name,
                $data_type,
                Arc::new(builder.finish()),
                dimensions,
            )
        }
    };
}

/// Flattened mask extracted from a `numpy.ma.MaskedArray`.
struct MaskValues {
    flags: Vec<bool>,
}

impl MaskValues {
    fn from_flags(flags: Vec<bool>) -> Option<Self> {
        if flags.iter().any(|flag| *flag) {
            Some(Self { flags })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.flags.len()
    }

    fn ensure_len(&self, expected: usize) -> PyResult<()> {
        if self.len() != expected {
            Err(PyValueError::new_err(format!(
                "masked array length mismatch (expected {expected}, found {})",
                self.len()
            )))
        } else {
            Ok(())
        }
    }

    fn is_masked(&self, index: usize) -> bool {
        self.flags[index]
    }
}

/// Convert a Python/Numpy value plus optional metadata into an [`NdArrowArray`].
///
/// `build_nd_array` is the single place where dtype inspection, dimension name
/// validation, and mask propagation occur before Arrow builders run.
pub(crate) fn build_nd_array(
    py: Python<'_>,
    field_name: &str,
    value: PyObject,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let (array_object, dimension_metadata) = extract_array_payload(py, value)?;
    let (array_object, mask_state) = separate_masked_array(py, array_object)?;
    let dimension_names = dimension_metadata.as_ref().map(|names| names.as_slice());
    let mask = mask_state.as_ref();
    let bound_value = array_object.bind(py);
    if let Some(dtype) = numpy_dtype(&bound_value)? {
        if let Some(kind) = numpy_kind(&dtype)? {
            match kind {
                'U' => {
                    return build_numpy_unicode_array(
                        py,
                        field_name,
                        &bound_value,
                        dimension_names,
                        mask,
                    );
                }
                'S' | 'a' => {
                    return build_numpy_binary_array(
                        py,
                        field_name,
                        &bound_value,
                        dimension_names,
                        mask,
                    );
                }
                'M' => {
                    return build_numpy_datetime64(
                        py,
                        field_name,
                        &bound_value,
                        &dtype,
                        dimension_names,
                        mask,
                    );
                }
                _ => {}
            }
        }
    }

    try_numpy_array!(
        &bound_value,
        bool,
        build_bool_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        i8,
        build_int8_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        i16,
        build_int16_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        i32,
        build_int32_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        i64,
        build_int64_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        u8,
        build_uint8_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        u16,
        build_uint16_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        u32,
        build_uint32_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        u64,
        build_uint64_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        f32,
        build_float32_numpy,
        field_name,
        dimension_names,
        mask
    );
    try_numpy_array!(
        &bound_value,
        f64,
        build_float64_numpy,
        field_name,
        dimension_names,
        mask
    );

    build_from_sequence(py, field_name, &bound_value, dimension_names)
}

fn build_from_sequence(
    py: Python<'_>,
    field_name: &str,
    value: &Bound<'_, PyAny>,
    dimension_names: Option<&[String]>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let sequence = value.downcast::<PySequence>()?;
    let len = sequence.len()? as usize;
    let mut items = Vec::with_capacity(len);
    for idx in 0..len {
        let item = sequence.get_item(idx)?;
        items.push(item.into_any().unbind());
    }

    let kind = infer_kind(py, &items)?;
    let (data_type, array): (DataType, ArrayRef) = match kind {
        ValueKind::Bool => build_bool_array(py, &items)?,
        ValueKind::Int => build_int_array(py, &items)?,
        ValueKind::Float => build_float_array(py, &items)?,
        ValueKind::Utf8 => build_string_array(py, &items, None)?,
        ValueKind::Binary => build_binary_array(py, &items, None)?,
    };

    let len = array.len();
    let shape = [len];
    let dimensions = dimensions_from_shape(&shape, dimension_names)?;
    make_nd_array(field_name, data_type, array, dimensions)
}

fn build_bool_numpy(
    field_name: &str,
    array: PyReadonlyArrayDyn<'_, bool>,
    dimension_names: Option<&[String]>,
    mask: Option<&MaskValues>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let view = array.as_array();
    let mut builder = BooleanBuilder::with_capacity(view.len());
    if let Some(mask_values) = mask {
        mask_values.ensure_len(view.len())?;
    }
    for (idx, value) in view.iter().enumerate() {
        if mask.map_or(false, |m| m.is_masked(idx)) {
            builder.append_null();
        } else {
            builder.append_value(*value);
        }
    }
    let dimensions = dimensions_from_shape(view.shape(), dimension_names)?;
    make_nd_array(
        field_name,
        DataType::Boolean,
        Arc::new(builder.finish()),
        dimensions,
    )
}

define_numeric_builder!(build_int8_numpy, i8, Int8Builder, DataType::Int8);
define_numeric_builder!(build_int16_numpy, i16, Int16Builder, DataType::Int16);
define_numeric_builder!(build_int32_numpy, i32, Int32Builder, DataType::Int32);
define_numeric_builder!(build_int64_numpy, i64, Int64Builder, DataType::Int64);
define_numeric_builder!(build_uint8_numpy, u8, UInt8Builder, DataType::UInt8);
define_numeric_builder!(build_uint16_numpy, u16, UInt16Builder, DataType::UInt16);
define_numeric_builder!(build_uint32_numpy, u32, UInt32Builder, DataType::UInt32);
define_numeric_builder!(build_uint64_numpy, u64, UInt64Builder, DataType::UInt64);
define_numeric_builder!(build_float32_numpy, f32, Float32Builder, DataType::Float32);
define_numeric_builder!(build_float64_numpy, f64, Float64Builder, DataType::Float64);

fn build_numpy_datetime64(
    py: Python<'_>,
    field_name: &str,
    array: &Bound<'_, PyAny>,
    dtype: &Bound<'_, PyAny>,
    dimension_names: Option<&[String]>,
    mask: Option<&MaskValues>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let numpy = py.import("numpy")?;
    let int64_type = numpy.getattr("int64")?;
    let viewed = array.call_method1("view", (int64_type,))?;
    let int_array = viewed.downcast::<PyArrayDyn<i64>>()?;
    build_datetime_numpy(
        field_name,
        int_array.readonly(),
        dtype,
        dimension_names,
        mask,
    )
}

fn build_datetime_numpy(
    field_name: &str,
    array: PyReadonlyArrayDyn<'_, i64>,
    dtype: &Bound<'_, PyAny>,
    dimension_names: Option<&[String]>,
    mask: Option<&MaskValues>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let multiplier = datetime_unit_multiplier(dtype)?;
    let view = array.as_array();
    let mut builder = TimestampNanosecondBuilder::with_capacity(view.len());
    if let Some(mask_values) = mask {
        mask_values.ensure_len(view.len())?;
    }
    for (idx, value) in view.iter().enumerate() {
        if mask.map_or(false, |m| m.is_masked(idx)) {
            builder.append_null();
        } else {
            builder.append_value(convert_datetime_value(*value, multiplier)?);
        }
    }
    let dimensions = dimensions_from_shape(view.shape(), dimension_names)?;
    make_nd_array(
        field_name,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Arc::new(builder.finish()),
        dimensions,
    )
}

fn build_numpy_unicode_array(
    py: Python<'_>,
    field_name: &str,
    array: &Bound<'_, PyAny>,
    dimension_names: Option<&[String]>,
    mask: Option<&MaskValues>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let shape = numpy_shape(array)?;
    let items = flatten_numpy_items(py, array)?;
    if let Some(mask_values) = mask {
        mask_values.ensure_len(items.len())?;
    }
    let (data_type, array_ref) = build_string_array(py, &items, mask)?;
    let dimensions = dimensions_from_shape(&shape, dimension_names)?;
    make_nd_array(field_name, data_type, array_ref, dimensions)
}

fn build_numpy_binary_array(
    py: Python<'_>,
    field_name: &str,
    array: &Bound<'_, PyAny>,
    dimension_names: Option<&[String]>,
    mask: Option<&MaskValues>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let shape = numpy_shape(array)?;
    let items = flatten_numpy_items(py, array)?;
    if let Some(mask_values) = mask {
        mask_values.ensure_len(items.len())?;
    }
    let (data_type, array_ref) = build_binary_array(py, &items, mask)?;
    let dimensions = dimensions_from_shape(&shape, dimension_names)?;
    make_nd_array(field_name, data_type, array_ref, dimensions)
}

fn flatten_numpy_items(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
    let flat = array.call_method0("ravel")?;
    let list = flat.call_method0("tolist")?;
    let sequence = list.downcast::<PySequence>()?;
    let len = sequence.len()? as usize;
    let mut items = Vec::with_capacity(len);
    for idx in 0..len {
        let item = sequence.get_item(idx)?;
        items.push(item.into_any().unbind());
    }
    Ok(items)
}

fn numpy_shape(array: &Bound<'_, PyAny>) -> PyResult<Vec<usize>> {
    let shape_obj = array.getattr("shape")?;
    let dims = shape_obj.downcast::<PySequence>()?;
    let ndim = dims.len()? as usize;
    if ndim == 0 {
        return Ok(vec![1]);
    }
    let mut shape = Vec::with_capacity(ndim);
    for idx in 0..ndim {
        let dim = dims.get_item(idx)?;
        shape.push(dim.extract::<usize>()?);
    }
    Ok(shape)
}

fn numpy_dtype<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    if value.hasattr("dtype")? {
        Ok(Some(value.getattr("dtype")?))
    } else {
        Ok(None)
    }
}

fn numpy_kind(dtype: &Bound<'_, PyAny>) -> PyResult<Option<char>> {
    let kind_obj = dtype.getattr("kind")?;
    let kind: String = kind_obj.extract()?;
    Ok(kind.chars().next())
}

fn datetime_unit_multiplier(dtype: &Bound<'_, PyAny>) -> PyResult<i64> {
    let name_obj = dtype.getattr("name")?;
    let descriptor: String = name_obj.extract()?;
    datetime_unit_multiplier_from_descriptor(&descriptor)
}

fn datetime_unit_multiplier_from_descriptor(descriptor: &str) -> PyResult<i64> {
    let unit = descriptor
        .split_once('[')
        .and_then(|(_, rest)| rest.strip_suffix(']'))
        .unwrap_or("ns");
    match unit {
        "s" => Ok(1_000_000_000i64),
        "ms" => Ok(1_000_000i64),
        "us" => Ok(1_000i64),
        "ns" => Ok(1i64),
        "m" => Ok(60i64 * 1_000_000_000i64),
        "h" => Ok(3_600i64 * 1_000_000_000i64),
        "D" => Ok(86_400i64 * 1_000_000_000i64),
        other => Err(PyValueError::new_err(format!(
            "unsupported numpy datetime64 unit: {other}"
        ))),
    }
}

fn convert_datetime_value(value: i64, multiplier: i64) -> PyResult<i64> {
    if multiplier == 1 {
        return Ok(value);
    }
    let scaled = (value as i128)
        .checked_mul(multiplier as i128)
        .ok_or_else(|| {
            PyValueError::new_err("datetime64 value overflow when converting to nanoseconds")
        })?;
    Ok(scaled as i64)
}

fn extract_array_payload(
    py: Python<'_>,
    value: PyObject,
) -> PyResult<(PyObject, Option<Vec<String>>)> {
    let bound_value = value.bind(py);
    if let Some(result) = try_extract_from_named_sequence(py, &bound_value)? {
        return Ok(result);
    }
    if let Some(result) = try_extract_from_mapping(py, &bound_value)? {
        return Ok(result);
    }
    Ok((value, None))
}

/// If `value` is a `numpy.ma.MaskedArray`, peel off the mask and return the
/// raw data object plus pre-flattened mask bits.
fn separate_masked_array(
    py: Python<'_>,
    value: PyObject,
) -> PyResult<(PyObject, Option<MaskValues>)> {
    let numpy = py.import("numpy")?;
    let ma = numpy.getattr("ma")?;
    let bound_value = value.bind(py);
    let is_masked: bool = ma
        .call_method1("isMaskedArray", (bound_value.clone(),))?
        .extract()?;
    if !is_masked {
        return Ok((value, None));
    }
    let mask_array_obj = ma.call_method1("getmaskarray", (bound_value.clone(),))?;
    let mask_array = mask_array_obj.downcast::<PyArrayDyn<bool>>()?;
    let mask_view = mask_array.readonly();
    let flags: Vec<bool> = mask_view.as_array().iter().copied().collect();
    let mask_values = MaskValues::from_flags(flags);
    let data_obj = bound_value.getattr("data")?;
    Ok((data_obj.into_any().unbind(), mask_values))
}

fn try_extract_from_named_sequence(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(PyObject, Option<Vec<String>>)>> {
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        if tuple.len() != 2 {
            return Ok(None);
        }
        let dims_obj = tuple.get_item(1)?;
        return match parse_dimension_names(&dims_obj)? {
            DimensionNameParse::Names(names) => {
                let data_obj = tuple.get_item(0)?;
                Ok(Some((data_obj.into_any().unbind(), Some(names))))
            }
            DimensionNameParse::NotDimensions => Ok(None),
        };
    }

    if let Ok(list) = value.downcast::<PyList>() {
        if list.len() != 2 {
            return Ok(None);
        }
        let dims_obj = list.get_item(1)?;
        return match parse_dimension_names(&dims_obj)? {
            DimensionNameParse::Names(names) => {
                let data_obj = list.get_item(0)?;
                Ok(Some((data_obj.into_any().unbind(), Some(names))))
            }
            DimensionNameParse::NotDimensions => Ok(None),
        };
    }

    Ok(None)
}

fn try_extract_from_mapping(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(PyObject, Option<Vec<String>>)>> {
    let Ok(mapping) = value.downcast::<PyDict>() else {
        return Ok(None);
    };
    let Some(array_obj) = find_mapping_value(mapping, &["data", "array", "values"]) else {
        return Ok(None);
    };
    let dims = if let Some(dims_obj) = find_mapping_value(mapping, &["dims", "dimensions"]) {
        match parse_dimension_names(&dims_obj)? {
            DimensionNameParse::Names(names) => Some(names),
            DimensionNameParse::NotDimensions => {
                return Err(PyValueError::new_err(
                    "dimension names must be sequences of strings when using 'dims' or 'dimensions'",
                ));
            }
        }
    } else {
        None
    };
    Ok(Some((array_obj.into_any().unbind(), dims)))
}

fn find_mapping_value<'py>(
    mapping: &Bound<'py, PyDict>,
    keys: &[&str],
) -> Option<Bound<'py, PyAny>> {
    for key in keys {
        if let Ok(Some(value)) = mapping.get_item(*key) {
            return Some(value);
        }
    }
    None
}

enum DimensionNameParse {
    Names(Vec<String>),
    NotDimensions,
}

fn parse_dimension_names(value: &Bound<'_, PyAny>) -> PyResult<DimensionNameParse> {
    if value.is_none() {
        return Ok(DimensionNameParse::NotDimensions);
    }

    if value.is_instance_of::<PyString>() {
        let name: String = value.extract()?;
        validate_dimension_name(&name)?;
        return Ok(DimensionNameParse::Names(vec![name]));
    }

    if let Ok(sequence) = value.downcast::<PySequence>() {
        let len = sequence.len()? as usize;
        if len == 0 {
            return Err(PyValueError::new_err(
                "dimension name sequences cannot be empty",
            ));
        }
        let mut names = Vec::with_capacity(len);
        for idx in 0..len {
            let item = sequence.get_item(idx)?;
            if !item.is_instance_of::<PyString>() {
                return Ok(DimensionNameParse::NotDimensions);
            }
            let name: String = item.extract()?;
            validate_dimension_name(&name)?;
            names.push(name);
        }
        return Ok(DimensionNameParse::Names(names));
    }

    Ok(DimensionNameParse::NotDimensions)
}

fn validate_dimension_name(name: &str) -> PyResult<()> {
    if name.trim().is_empty() {
        Err(PyValueError::new_err(
            "dimension names must contain at least one non-whitespace character",
        ))
    } else {
        Ok(())
    }
}

fn build_bool_array(py: Python<'_>, items: &[PyObject]) -> PyResult<(DataType, ArrayRef)> {
    let mut builder = BooleanBuilder::with_capacity(items.len());
    for obj in items {
        let any = obj.bind(py);
        if any.is_none() {
            builder.append_null();
        } else {
            let value: bool = any.extract()?;
            builder.append_value(value);
        }
    }
    Ok((DataType::Boolean, Arc::new(builder.finish())))
}

fn build_int_array(py: Python<'_>, items: &[PyObject]) -> PyResult<(DataType, ArrayRef)> {
    let mut builder = Int64Builder::with_capacity(items.len());
    for obj in items {
        let any = obj.bind(py);
        if any.is_none() {
            builder.append_null();
        } else {
            let value: i64 = any.extract()?;
            builder.append_value(value);
        }
    }
    Ok((DataType::Int64, Arc::new(builder.finish())))
}

fn build_float_array(py: Python<'_>, items: &[PyObject]) -> PyResult<(DataType, ArrayRef)> {
    let mut builder = Float64Builder::with_capacity(items.len());
    for obj in items {
        let any = obj.bind(py);
        if any.is_none() {
            builder.append_null();
        } else {
            let value: f64 = any.extract()?;
            builder.append_value(value);
        }
    }
    Ok((DataType::Float64, Arc::new(builder.finish())))
}

fn build_string_array(
    py: Python<'_>,
    items: &[PyObject],
    mask: Option<&MaskValues>,
) -> PyResult<(DataType, ArrayRef)> {
    let mut builder = StringBuilder::with_capacity(items.len(), items.len() * 4);
    for (idx, obj) in items.iter().enumerate() {
        if mask.map_or(false, |m| m.is_masked(idx)) {
            builder.append_null();
            continue;
        }
        let any = obj.bind(py);
        if any.is_none() {
            builder.append_null();
        } else {
            let value: String = any.extract()?;
            builder.append_value(value);
        }
    }
    Ok((DataType::Utf8, Arc::new(builder.finish())))
}

fn build_binary_array(
    py: Python<'_>,
    items: &[PyObject],
    mask: Option<&MaskValues>,
) -> PyResult<(DataType, ArrayRef)> {
    let mut builder = BinaryBuilder::with_capacity(items.len(), items.len() * 4);
    for (idx, obj) in items.iter().enumerate() {
        if mask.map_or(false, |m| m.is_masked(idx)) {
            builder.append_null();
            continue;
        }
        let any = obj.bind(py);
        if any.is_none() {
            builder.append_null();
        } else {
            let value = any.downcast::<PyBytes>()?;
            builder.append_value(value.as_bytes());
        }
    }
    Ok((DataType::Binary, Arc::new(builder.finish())))
}

#[derive(Debug, Clone, Copy)]
enum ValueKind {
    Bool,
    Int,
    Float,
    Utf8,
    Binary,
}

fn infer_kind(py: Python<'_>, items: &[PyObject]) -> PyResult<ValueKind> {
    if has_type(py, items, |any| {
        Ok(any.is_instance_of::<PyString>())
    })? {
        return Ok(ValueKind::Utf8);
    }
    if has_type(py, items, |any| Ok(any.is_instance_of::<PyBytes>()))? {
        return Ok(ValueKind::Binary);
    }
    if has_type(py, items, |any| {
        Ok(any.is_instance_of::<pyo3::types::PyFloat>())
    })? {
        return Ok(ValueKind::Float);
    }
    if has_type(py, items, |any| {
        Ok(any.is_instance_of::<pyo3::types::PyBool>())
    })? {
        return Ok(ValueKind::Bool);
    }
    if has_type(py, items, |any| {
        Ok(any.is_instance_of::<PyInt>())
    })? {
        return Ok(ValueKind::Int);
    }
    Err(PyValueError::new_err(
        "unable to determine array type from empty or null-only sequence",
    ))
}

fn has_type<F>(py: Python<'_>, items: &[PyObject], mut predicate: F) -> PyResult<bool>
where
    F: FnMut(&Bound<'_, PyAny>) -> PyResult<bool>,
{
    for obj in items {
        let any = obj.bind(py);
        if any.is_none() {
            continue;
        }
        if predicate(&any)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn make_nd_array(
    field_name: &str,
    data_type: DataType,
    array: ArrayRef,
    dimensions: Vec<Dimension>,
) -> PyResult<(FieldRef, NdArrowArray)> {
    let field: FieldRef = Arc::new(Field::new(field_name.to_string(), data_type.clone(), true));
    let nd_array = NdArrowArray::new(array, Dimensions::MultiDimensional(dimensions))
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok((field, nd_array))
}

/// Build Nd-array dimension metadata, validating optional user supplied names.
fn dimensions_from_shape(shape: &[usize], names: Option<&[String]>) -> PyResult<Vec<Dimension>> {
    if shape.is_empty() {
        let name = match names {
            Some(provided) => {
                if provided.len() != 1 {
                    return Err(PyValueError::new_err(format!(
                        "dimension name count ({}) must be 1 for scalar arrays",
                        provided.len()
                    )));
                }
                provided[0].clone()
            }
            None => "dim0".to_string(),
        };
        return Ok(vec![Dimension { name, size: 1 }]);
    }

    if let Some(provided) = names {
        if provided.len() != shape.len() {
            return Err(PyValueError::new_err(format!(
                "dimension name count ({}) does not match array rank ({})",
                provided.len(),
                shape.len()
            )));
        }
        return Ok(shape
            .iter()
            .enumerate()
            .map(|(idx, size)| Dimension {
                name: provided[idx].clone(),
                size: *size,
            })
            .collect());
    }

    Ok(shape
        .iter()
        .enumerate()
        .map(|(idx, size)| Dimension {
            name: format!("dim{idx}"),
            size: *size,
        })
        .collect())
}
