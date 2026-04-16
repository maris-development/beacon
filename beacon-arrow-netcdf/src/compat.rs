//! Compatibility helpers between NetCDF types and ND Arrow arrays.

use std::{collections::HashMap, sync::Arc};

use beacon_nd_arrow::{arrow::NdArrowArray, datatypes::NdArrayType, NdArray, NdArrayD};
use netcdf::AttributeValue;

use crate::{
    backend::{AttributeBackend, VariableBackend},
    decoders::{
        cf_time::{parse_time_units, CFTimeVariableDecoder},
        strings::StringVariableDecoder,
        DefaultVariableDecoder, VariableDecoder,
    },
    NcChar, OwnedNcString,
};

fn numeric_variable_to_nd_arrow_array<T>(
    nc_file: Arc<netcdf::File>,
    variable_name: &str,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    fill_value: Option<T>,
    cf_time_epoch_unit: Option<(hifitime::Epoch, hifitime::Unit)>,
) -> anyhow::Result<NdArrowArray>
where
    T: NdArrayType + netcdf::NcTypeDescriptor + Copy + num_traits::AsPrimitive<f64> + 'static,
{
    let default_decoder: Arc<dyn VariableDecoder<T>> =
        Arc::new(DefaultVariableDecoder::<T>::new(variable_name, fill_value));

    if let Some((epoch, unit)) = cf_time_epoch_unit {
        let field = Arc::new(arrow_schema::Field::new(
            variable_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ));
        let time_backend = VariableBackend::new(
            Arc::new(CFTimeVariableDecoder::new(default_decoder, epoch, unit)),
            nc_file,
            shape,
            dimensions,
        );

        return Ok(NdArrowArray::new(
            Arc::new(NdArray::new_with_backend(time_backend)?) as Arc<dyn NdArrayD>,
        ));
    }

    let variable_backend = VariableBackend::new(default_decoder, nc_file, shape, dimensions);
    Ok(NdArrowArray::new(
        Arc::new(NdArray::new_with_backend(variable_backend)?) as Arc<dyn NdArrayD>,
    ))
}

/// Convert a NetCDF attribute value into an ND Arrow scalar array.
///
/// Returns an error when the attribute type is not currently supported.
pub fn attribute_to_nd_arrow_array(
    attr_name: &str,
    attr_value: AttributeValue,
) -> anyhow::Result<NdArrowArray> {
    match attr_value {
        AttributeValue::Uchar(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Schar(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Ushort(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Short(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Uint(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Int(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Ulonglong(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Longlong(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Float(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Double(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        AttributeValue::Str(value) => {
            let array = AttributeBackend::new(attr_name, value);
            Ok(NdArrowArray::new(
                Arc::new(NdArray::new_with_backend(array)?) as Arc<dyn NdArrayD>,
            ))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported attribute type for attribute '{}'",
            attr_name
        )),
    }
}

/// Convert a NetCDF variable into an ND Arrow array.
///
/// The conversion applies special handling for:
/// - CF-time numeric variables (`units` attribute).
/// - NetCDF string variables.
/// - Char arrays with trailing string-length dimensions.
/// - `_FillValue` for supported numeric and string types.
pub fn variable_to_nd_arrow_array(
    nc_file: Arc<netcdf::File>,
    variable_name: &str,
    attributes: HashMap<String, AttributeValue>,
) -> anyhow::Result<NdArrowArray> {
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

    let cf_time_epoch_unit = match attributes.get("units") {
        Some(AttributeValue::Str(units_str)) => parse_time_units(units_str),
        _ => None,
    };

    match var_type {
        netcdf::types::NcVariableType::String => {
            let string_fill_attr =
                if let Some(AttributeValue::Str(fill_str)) = attributes.get("_FillValue") {
                    Some(OwnedNcString(fill_str.clone()))
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
            use beacon_nd_arrow::arrow::NdArrowArray;
            let nd_array = Arc::new(NdArray::new_with_backend(array_backend)?) as Arc<dyn NdArrayD>;
            Ok(NdArrowArray::new(nd_array))
        }
        netcdf::types::NcVariableType::Int(int_type) => match int_type {
            netcdf::types::IntType::U8 => numeric_variable_to_nd_arrow_array::<u8>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Uchar(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::U16 => numeric_variable_to_nd_arrow_array::<u16>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Ushort(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::U32 => numeric_variable_to_nd_arrow_array::<u32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Uint(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::U64 => numeric_variable_to_nd_arrow_array::<u64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Ulonglong(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::I8 => numeric_variable_to_nd_arrow_array::<i8>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Schar(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::I16 => numeric_variable_to_nd_arrow_array::<i16>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Short(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::I32 => numeric_variable_to_nd_arrow_array::<i32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Int(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::IntType::I64 => numeric_variable_to_nd_arrow_array::<i64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Longlong(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
        },
        netcdf::types::NcVariableType::Float(float_type) => match float_type {
            netcdf::types::FloatType::F32 => numeric_variable_to_nd_arrow_array::<f32>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Float(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
            ),
            netcdf::types::FloatType::F64 => numeric_variable_to_nd_arrow_array::<f64>(
                nc_file.clone(),
                variable_name,
                shape.clone(),
                dimensions.clone(),
                match attributes.get("_FillValue") {
                    Some(AttributeValue::Double(value)) => Some(*value),
                    _ => None,
                },
                cf_time_epoch_unit,
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
                        Some(OwnedNcString(fill_str.clone()))
                    } else {
                        None
                    };

                let fixed_string_size = last_dim.unwrap().len();

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

                Ok(NdArrowArray(Arc::new(NdArray::new_with_backend(
                    array_backend,
                )?)))
            } else {
                // Handle as array of single characters
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

                let array_backend = VariableBackend::new(
                    Arc::new(DefaultVariableDecoder::new(
                        variable_name.to_string(),
                        character_fill_value,
                    )),
                    nc_file.clone(),
                    shape,
                    dimensions,
                );

                Ok(NdArrowArray(Arc::new(NdArray::new_with_backend(
                    array_backend,
                )?)))
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported variable type '{:?}' for variable '{}'",
            var_type,
            variable_name
        )),
    }
}
