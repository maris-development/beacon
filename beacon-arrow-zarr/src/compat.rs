//! Conversion between zarrs arrays/attributes and `beacon-nd-array` types.

use std::sync::Arc;

use beacon_nd_array::{NdArray, NdArrayD};
use indexmap::IndexMap;

use crate::{
    attributes::AttributeValue,
    backend::{
        AttributeBackend, CfTimeBackend, ScaleOffsetBackend, ZarrArray, ZarrArrayBackend,
        parse_cf_time_units,
    },
    data_types::ZarrDtypeKind,
};

fn is_numeric(kind: ZarrDtypeKind) -> bool {
    matches!(
        kind,
        ZarrDtypeKind::Int8
            | ZarrDtypeKind::Int16
            | ZarrDtypeKind::Int32
            | ZarrDtypeKind::Int64
            | ZarrDtypeKind::UInt8
            | ZarrDtypeKind::UInt16
            | ZarrDtypeKind::UInt32
            | ZarrDtypeKind::UInt64
            | ZarrDtypeKind::Float32
            | ZarrDtypeKind::Float64
    )
}

/// Extract logical metadata from a zarr array: `(shape, dimension_names, chunk_shape)`.
fn array_metadata(array: &ZarrArray) -> (Vec<usize>, Vec<String>, Vec<usize>) {
    let shape: Vec<usize> = array.shape().iter().map(|&s| s as usize).collect();
    let ndim = shape.len();

    let dimensions: Vec<String> = match array.dimension_names() {
        Some(names) => names
            .iter()
            .enumerate()
            .map(|(i, name)| name.clone().unwrap_or_else(|| format!("dim_{i}")))
            .collect(),
        None => (0..ndim).map(|i| format!("dim_{i}")).collect(),
    };

    // Chunk 0's shape is the (regular) chunk shape; boundary chunks are handled
    // by the engine's chunk iterator. Fall back to the full shape if unavailable.
    let chunk_shape: Vec<usize> = array
        .chunk_grid()
        .subset(&vec![0u64; ndim])
        .ok()
        .flatten()
        .map(|s| s.shape().iter().map(|&x| x as usize).collect())
        .unwrap_or_else(|| shape.clone());

    (shape, dimensions, chunk_shape)
}

/// Convert a zarr array (plus its parsed attributes) into a lazy [`NdArrayD`].
///
/// CF conventions are applied at the backend level:
/// - `scale_factor` / `add_offset` → decoded `f64`.
/// - `units = "<unit> since <epoch>"` → nanosecond timestamps.
/// - `_FillValue` → propagated so the engine nulls matching elements.
pub fn array_to_nd_array(
    array: Arc<ZarrArray>,
    array_name: &str,
    attributes: &IndexMap<String, AttributeValue>,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    let kind = crate::data_types::classify(array.data_type());
    if kind == ZarrDtypeKind::Other {
        anyhow::bail!(
            "Zarr array '{}' has an unsupported data type: {:?}",
            array_name,
            array.data_type()
        );
    }

    let (shape, dimensions, chunk_shape) = array_metadata(&array);

    let raw_fill = attributes.get("_FillValue").and_then(|a| a.as_f64());
    let scale = attributes.get("scale_factor").and_then(|a| a.as_f64());
    let offset = attributes.get("add_offset").and_then(|a| a.as_f64());
    let units = attributes.get("units").and_then(|a| a.as_str());

    // CF scale_factor / add_offset packing → f64.
    if is_numeric(kind) && (scale.is_some() || offset.is_some()) {
        let backend = ScaleOffsetBackend::new(
            array,
            kind,
            shape,
            dimensions,
            chunk_shape,
            scale.unwrap_or(1.0),
            offset.unwrap_or(0.0),
            raw_fill,
        );
        return Ok(Arc::new(NdArray::new_with_backend(backend)?));
    }

    // CF time units → nanosecond timestamps.
    if is_numeric(kind)
        && let Some(units) = units
        && let Some((epoch, unit)) = parse_cf_time_units(units)
    {
        let backend = CfTimeBackend::new(
            array, kind, shape, dimensions, chunk_shape, epoch, unit, raw_fill,
        );
        return Ok(Arc::new(NdArray::new_with_backend(backend)?));
    }

    // Direct read in the array's native dtype.
    macro_rules! direct {
        ($ty:ty, $fill:expr) => {{
            let backend = ZarrArrayBackend::<$ty>::new(
                array,
                shape,
                dimensions,
                chunk_shape,
                $fill,
            );
            Ok(Arc::new(NdArray::new_with_backend(backend)?) as Arc<dyn NdArrayD>)
        }};
    }

    let bool_fill = attributes.get("_FillValue").and_then(|a| a.as_bool());
    let string_fill = attributes
        .get("_FillValue")
        .and_then(|a| a.as_str().map(|s| s.to_string()));

    match kind {
        ZarrDtypeKind::Bool => direct!(bool, bool_fill),
        ZarrDtypeKind::Int8 => direct!(i8, raw_fill.map(|f| f as i8)),
        ZarrDtypeKind::Int16 => direct!(i16, raw_fill.map(|f| f as i16)),
        ZarrDtypeKind::Int32 => direct!(i32, raw_fill.map(|f| f as i32)),
        ZarrDtypeKind::Int64 => direct!(i64, raw_fill.map(|f| f as i64)),
        ZarrDtypeKind::UInt8 => direct!(u8, raw_fill.map(|f| f as u8)),
        ZarrDtypeKind::UInt16 => direct!(u16, raw_fill.map(|f| f as u16)),
        ZarrDtypeKind::UInt32 => direct!(u32, raw_fill.map(|f| f as u32)),
        ZarrDtypeKind::UInt64 => direct!(u64, raw_fill.map(|f| f as u64)),
        ZarrDtypeKind::Float32 => direct!(f32, raw_fill.map(|f| f as f32)),
        ZarrDtypeKind::Float64 => direct!(f64, raw_fill),
        ZarrDtypeKind::String => direct!(String, string_fill),
        ZarrDtypeKind::Bytes => direct!(Vec<u8>, None::<Vec<u8>>),
        ZarrDtypeKind::Other => unreachable!("Other dtype rejected above"),
    }
}

/// Convert a scalar zarr attribute value into a rank-0 [`NdArrayD`].
pub fn attribute_to_nd_array(attr: &AttributeValue) -> anyhow::Result<Arc<dyn NdArrayD>> {
    match attr {
        AttributeValue::String(s) => Ok(Arc::new(NdArray::new_with_backend(
            AttributeBackend::new(s.clone()),
        )?)),
        AttributeValue::Float64(f) => Ok(Arc::new(NdArray::new_with_backend(
            AttributeBackend::new(*f),
        )?)),
        AttributeValue::Bool(b) => Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(
            *b,
        ))?)),
    }
}
