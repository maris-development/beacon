//! Compatibility helpers between NetCDF types and ND arrays.

use std::{collections::HashMap, sync::Arc};

use beacon_nd_array::{datatypes::NdArrayType, NdArray, NdArrayD};
use netcdf::AttributeValue;
use num_traits::AsPrimitive;

use crate::{
    backend::{AttributeBackend, VariableBackend},
    decoders::{
        cf_time::{parse_time_units, CFTimeVariableDecoder},
        scale_offset::ScaleOffsetVariableDecoder,
        strings::StringVariableDecoder,
        DefaultVariableDecoder,
    },
    NcChar,
};

/// Interpret a scalar NetCDF attribute (e.g. `scale_factor`, `add_offset`) as
/// an `f64`. Returns `None` for non-numeric or multi-valued attributes.
fn attribute_as_f64(attr: &AttributeValue) -> Option<f64> {
    match attr {
        AttributeValue::Float(v) => Some(*v as f64),
        AttributeValue::Double(v) => Some(*v),
        AttributeValue::Floats(v) if v.len() == 1 => Some(v[0] as f64),
        AttributeValue::Doubles(v) if v.len() == 1 => Some(v[0]),
        AttributeValue::Schar(v) => Some(*v as f64),
        AttributeValue::Uchar(v) => Some(*v as f64),
        AttributeValue::Short(v) => Some(*v as f64),
        AttributeValue::Ushort(v) => Some(*v as f64),
        AttributeValue::Int(v) => Some(*v as f64),
        AttributeValue::Uint(v) => Some(*v as f64),
        AttributeValue::Longlong(v) => Some(*v as f64),
        AttributeValue::Ulonglong(v) => Some(*v as f64),
        _ => None,
    }
}

fn numeric_variable_to_nd_array<T>(
    nc_file: Arc<netcdf::File>,
    variable_name: &str,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    fill_value: Option<T>,
    cf_time_epoch_unit: Option<(hifitime::Epoch, hifitime::Unit)>,
    scale_offset: Option<(f64, f64)>,
) -> anyhow::Result<Arc<dyn NdArrayD>>
where
    T: NdArrayType
        + netcdf::NcTypeDescriptor
        + Copy
        + num_traits::AsPrimitive<f64>
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let default_decoder: Arc<dyn crate::decoders::VariableDecoder<T>> = Arc::new(
        DefaultVariableDecoder::<T>::new(variable_name.to_string(), fill_value),
    );

    // CF `scale_factor` / `add_offset` packing → decoded `f64`. Takes precedence
    // over CF-time (a variable carries one or the other, never both), mirroring
    // the zarr reader.
    if let Some((scale, offset)) = scale_offset {
        let raw_fill = fill_value.map(|f| f.as_());
        let scale_offset_decoder = VariableBackend::new(
            Arc::new(ScaleOffsetVariableDecoder::new(
                variable_name.to_string(),
                default_decoder,
                scale,
                offset,
                raw_fill,
            )),
            nc_file,
            shape,
            dimensions,
        );

        let nd_array = NdArray::new_with_backend(scale_offset_decoder)?;
        return Ok(Arc::new(nd_array));
    }

    if let Some((epoch, unit)) = cf_time_epoch_unit {
        let time_backend = VariableBackend::new(
            Arc::new(CFTimeVariableDecoder::new(
                variable_name.to_string(),
                default_decoder,
                epoch,
                unit,
            )),
            nc_file,
            shape,
            dimensions,
        );

        let nd_array = NdArray::new_with_backend(time_backend)?;
        return Ok(Arc::new(nd_array));
    }

    let variable_backend = VariableBackend::new(default_decoder, nc_file, shape, dimensions);
    let nd_array = NdArray::new_with_backend(variable_backend)?;
    Ok(Arc::new(nd_array))
}

/// Convert a NetCDF attribute value into an ND scalar array.
///
/// Returns an error when the attribute type is not currently supported.
pub fn attribute_to_nd_array(
    _attr_name: &str,
    attr_value: AttributeValue,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    match attr_value {
        AttributeValue::Uchar(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Uchars(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Schar(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Schars(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Ushort(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Ushorts(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Short(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Shorts(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Uint(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Uints(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Int(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Ints(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Ulonglong(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Ulonglongs(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Longlong(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Longlongs(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Float(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Floats(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Double(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Doubles(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0]))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Str(value) => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(value))?;
            Ok(Arc::new(nd))
        }
        AttributeValue::Strs(values) if values.len() == 1 => {
            let nd = NdArray::new_with_backend(AttributeBackend::new(values[0].clone()))?;
            Ok(Arc::new(nd))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported attribute type for attribute '{}'",
            _attr_name
        )),
    }
}

/// Convert a NetCDF variable into an ND array.
///
/// The conversion applies special handling for:
/// - CF-time numeric variables (`units` attribute).
/// - NetCDF string variables.
/// - Char arrays with trailing string-length dimensions.
/// - `_FillValue` for supported numeric and string types.
pub fn variable_to_nd_array(
    nc_file: Arc<netcdf::File>,
    variable_name: &str,
    attributes: HashMap<String, AttributeValue>,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    let var = nc_file
        .variable(variable_name)
        .ok_or_else(|| anyhow::anyhow!("Variable '{}' not found in NetCDF file", variable_name))?;
    let shape = var
        .dimensions()
        .iter()
        .map(|dim| dim.len())
        .collect::<Vec<_>>();
    let dimensions = var
        .dimensions()
        .iter()
        .map(|dim| dim.name())
        .collect::<Vec<_>>();
    let var_type = var.vartype();

    // The optional CF `calendar` attribute selects how the reference date is
    // interpreted (defaults to Gregorian when absent).
    let calendar = match attributes.get("calendar") {
        Some(AttributeValue::Str(cal)) => Some(cal.clone()),
        Some(AttributeValue::Strs(cals)) if cals.len() == 1 => Some(cals[0].clone()),
        _ => None,
    };

    let cf_time_epoch_unit = match attributes.get("units") {
        Some(AttributeValue::Str(units_str)) => parse_time_units(units_str, calendar.as_deref()),
        Some(AttributeValue::Strs(units_strs)) if units_strs.len() == 1 => {
            parse_time_units(&units_strs[0], calendar.as_deref())
        }
        _ => None,
    };

    // CF `scale_factor` / `add_offset` packing. When either is present the
    // numeric variable is decoded to `f64` as `raw * scale + offset`; a missing
    // factor defaults to the identity (scale 1.0, offset 0.0).
    let scale = attributes.get("scale_factor").and_then(attribute_as_f64);
    let offset = attributes.get("add_offset").and_then(attribute_as_f64);
    let scale_offset =
        (scale.is_some() || offset.is_some()).then(|| (scale.unwrap_or(1.0), offset.unwrap_or(0.0)));

    match var_type {
        netcdf::types::NcVariableType::String => {
            let string_fill_attr =
                if let Some(AttributeValue::Str(fill_str)) = attributes.get("_FillValue") {
                    Some(fill_str.clone())
                } else {
                    None
                };
            let array_backend = VariableBackend::new(
                Arc::new(StringVariableDecoder::new(
                    variable_name.to_string(),
                    string_fill_attr,
                    None,
                )),
                nc_file.clone(),
                shape,
                dimensions,
            );

            let nd_array = NdArray::new_with_backend(array_backend)?;
            Ok(Arc::new(nd_array))
        }
        netcdf::types::NcVariableType::Int(int_type) => match int_type {
            netcdf::types::IntType::U8 => numeric_variable_to_nd_array::<u8>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Uchar(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::U16 => numeric_variable_to_nd_array::<u16>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Ushort(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::U32 => numeric_variable_to_nd_array::<u32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Uint(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::U64 => numeric_variable_to_nd_array::<u64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Ulonglong(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::I8 => numeric_variable_to_nd_array::<i8>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Schar(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::I16 => numeric_variable_to_nd_array::<i16>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Short(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::I32 => numeric_variable_to_nd_array::<i32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Int(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::IntType::I64 => numeric_variable_to_nd_array::<i64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Longlong(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
        },
        netcdf::types::NcVariableType::Float(float_type) => match float_type {
            netcdf::types::FloatType::F32 => numeric_variable_to_nd_array::<f32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Float(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
            netcdf::types::FloatType::F64 => numeric_variable_to_nd_array::<f64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Double(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
                scale_offset,
            ),
        },
        netcdf::types::NcVariableType::Char => {
            // For char variables, we can treat them as fixed-size strings where the last dimension is the string length
            let last_dim = var.dimensions().last();

            let is_fixed_string_array = if let Some(dim) = last_dim {
                let dim_name = dim.name().to_lowercase();
                dim_name.starts_with("string")
                    || dim_name.starts_with("strlen")
                    || dim_name.starts_with("strnlen")
            } else {
                false
            };

            if is_fixed_string_array {
                // Handle as fixed-size string array
                let string_fill_attr =
                    if let Some(AttributeValue::Str(fill_str)) = attributes.get("_FillValue") {
                        Some(fill_str.clone())
                    } else {
                        None
                    };

                // Safe: `is_fixed_string_array` is only true when `last_dim` is `Some`.
                let fixed_string_size = last_dim
                    .expect("is_fixed_string_array implies a trailing dimension exists")
                    .len();

                // The trailing dimension stores fixed string length and is consumed
                // by StringVariableDecoder; exclude it from the ND logical shape.
                let logical_shape = shape[..shape.len().saturating_sub(1)].to_vec();
                let logical_dimensions = dimensions[..dimensions.len().saturating_sub(1)].to_vec();

                let array_backend = VariableBackend::new(
                    Arc::new(StringVariableDecoder::new(
                        variable_name.to_string(),
                        string_fill_attr,
                        Some(fixed_string_size),
                    )),
                    nc_file.clone(),
                    logical_shape,
                    logical_dimensions,
                );

                let nd_array = NdArray::new_with_backend(array_backend)?;
                Ok(Arc::new(nd_array))
            } else {
                // Handle as array of single characters mapped to String via NcChar decoder.
                // NcChar implements NcTypeDescriptor but not NdArrayType, so we read
                // as NcChar and convert each element to a String.
                let character_fill_value =
                    if let Some(AttributeValue::Str(fill_str)) = attributes.get("_FillValue") {
                        if fill_str.len() == 1 {
                            Some(NcChar(fill_str.as_bytes()[0]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                let char_decoder: Arc<dyn crate::decoders::VariableDecoder<NcChar>> = Arc::new(
                    DefaultVariableDecoder::new(variable_name.to_string(), character_fill_value),
                );

                let char_to_string_decoder = CharToStringDecoder {
                    inner: char_decoder,
                    variable_name: variable_name.to_string(),
                };

                let array_backend = VariableBackend::new(
                    Arc::new(char_to_string_decoder),
                    nc_file.clone(),
                    shape,
                    dimensions,
                );

                let nd_array = NdArray::new_with_backend(array_backend)?;
                Ok(Arc::new(nd_array))
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported variable type '{:?}' for variable '{}'",
            var_type,
            variable_name
        )),
    }
}

/// Decoder adapter that reads `NcChar` values and converts each to a `String`.
#[derive(Debug)]
struct CharToStringDecoder {
    inner: Arc<dyn crate::decoders::VariableDecoder<NcChar>>,
    variable_name: String,
}

impl crate::decoders::VariableDecoder<String> for CharToStringDecoder {
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<ndarray::ArrayD<String>> {
        let char_array = self.inner.read(variable, extents)?;
        Ok(char_array.mapv(|c| {
            let NcChar(byte) = c;
            (byte as char).to_string()
        }))
    }

    fn fill_value(&self) -> Option<String> {
        self.inner
            .fill_value()
            .map(|NcChar(byte)| (byte as char).to_string())
    }

    fn variable_name(&self) -> &str {
        &self.variable_name
    }
}
