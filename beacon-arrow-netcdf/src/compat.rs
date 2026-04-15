//! Compatibility helpers between NetCDF types and ND Arrow arrays.

use std::{collections::HashMap, sync::Arc};

use beacon_nd_arrow::array::{backend::ArrayBackend, compat_typings::ArrowTypeConversion};
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
) -> anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>>
where
    T: ArrowTypeConversion
        + netcdf::NcTypeDescriptor
        + Copy
        + num_traits::AsPrimitive<f64>
        + 'static,
{
    let default_decoder: Arc<dyn VariableDecoder<T>> = Arc::new(DefaultVariableDecoder::<T>::new(
        Arc::new(arrow_schema::Field::new(
            variable_name,
            T::arrow_data_type(),
            false,
        )),
        fill_value,
    ));

    if let Some((epoch, unit)) = cf_time_epoch_unit {
        let field = Arc::new(arrow_schema::Field::new(
            variable_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ));
        let time_backend = VariableBackend::new(
            Arc::new(CFTimeVariableDecoder::new(
                field,
                default_decoder,
                epoch,
                unit,
            )),
            nc_file,
            shape,
            dimensions,
        );

        return Ok(time_backend.into_dyn_array()?);
    }

    let variable_backend = VariableBackend::new(default_decoder, nc_file, shape, dimensions);
    Ok(variable_backend.into_dyn_array()?)
}

/// Convert a NetCDF attribute value into an ND Arrow scalar array.
///
/// Returns an error when the attribute type is not currently supported.
pub fn attribute_to_nd_arrow_array(
    attr_name: &str,
    attr_value: AttributeValue,
) -> anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>> {
    match attr_value {
        AttributeValue::Uchar(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    u8::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Schar(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    i8::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Ushort(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    u16::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Short(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    i16::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Uint(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    u32::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Int(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    i32::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Ulonglong(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    u64::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Longlong(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    i64::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Float(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    f32::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Double(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    f64::arrow_data_type(),
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Str(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(
                    attr_name,
                    arrow_schema::DataType::Utf8,
                    false,
                )),
                value,
            );
            Ok(array.into_dyn_array()?)
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
) -> anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>> {
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
            let field = Arc::new(arrow_schema::Field::new(
                variable_name,
                arrow_schema::DataType::Utf8,
                string_fill_attr.is_some(),
            ));
            let array_backend = VariableBackend::new(
                Arc::new(StringVariableDecoder::new(field, string_fill_attr, None)),
                nc_file.clone(),
                shape,
                dimensions,
            );

            Ok(array_backend.into_dyn_array()?)
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
                let field = Arc::new(arrow_schema::Field::new(
                    variable_name,
                    arrow_schema::DataType::Utf8,
                    string_fill_attr.is_some(),
                ));

                // The trailing dimension stores fixed string length and is consumed
                // by StringVariableDecoder; exclude it from the ND logical shape.
                let logical_shape = shape[..shape.len().saturating_sub(1)].to_vec();
                let logical_dimensions = dimensions[..dimensions.len().saturating_sub(1)].to_vec();

                let array_backend = VariableBackend::new(
                    Arc::new(StringVariableDecoder::new(
                        field,
                        string_fill_attr,
                        Some(fixed_string_size),
                    )),
                    nc_file.clone(),
                    logical_shape,
                    logical_dimensions,
                );

                Ok(array_backend.into_dyn_array()?)
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

                let field = Arc::new(arrow_schema::Field::new(
                    variable_name,
                    arrow_schema::DataType::Utf8,
                    character_fill_value.is_some(),
                ));

                let array_backend = VariableBackend::new(
                    Arc::new(DefaultVariableDecoder::new(field, character_fill_value)),
                    nc_file.clone(),
                    shape,
                    dimensions,
                );

                Ok(array_backend.into_dyn_array()?)
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported variable type '{:?}' for variable '{}'",
            var_type,
            variable_name
        )),
    }
}
