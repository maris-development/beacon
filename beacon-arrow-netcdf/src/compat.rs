use std::{collections::HashMap, sync::Arc};

use beacon_nd_arrow::array::{backend::ArrayBackend, compat_typings::ArrowTypeConversion};
use netcdf::AttributeValue;

use crate::{
    backend::{AttributeBackend, VariableBackend},
    decoders::{strings::StringVariableDecoder, DefaultVariableDecoder},
    NcChar, OwnedNcString,
};

pub fn attribute_to_nd_arrow_array(
    attr_name: &str,
    attr_value: AttributeValue,
) -> anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>> {
    match attr_value {
        AttributeValue::Uchar(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, u8::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Schar(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, i8::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Ushort(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, u16::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Short(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, i16::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Uint(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, u32::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Int(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, i32::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Ulonglong(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, u64::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Longlong(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, i64::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Float(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, f32::data_type(), false)),
                value,
            );
            Ok(array.into_dyn_array()?)
        }
        AttributeValue::Double(value) => {
            let array = AttributeBackend::new(
                Arc::new(arrow_schema::Field::new(attr_name, f64::data_type(), false)),
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
            netcdf::types::IntType::U8 => todo!(),
            netcdf::types::IntType::U16 => todo!(),
            netcdf::types::IntType::U32 => todo!(),
            netcdf::types::IntType::U64 => todo!(),
            netcdf::types::IntType::I8 => todo!(),
            netcdf::types::IntType::I16 => todo!(),
            netcdf::types::IntType::I32 => todo!(),
            netcdf::types::IntType::I64 => todo!(),
        },
        netcdf::types::NcVariableType::Float(float_type) => match float_type {
            netcdf::types::FloatType::F32 => todo!(),
            netcdf::types::FloatType::F64 => todo!(),
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

                let array_backend = VariableBackend::new(
                    Arc::new(StringVariableDecoder::new(
                        field,
                        string_fill_attr,
                        Some(fixed_string_size),
                    )),
                    nc_file.clone(),
                    shape,
                    dimensions,
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
