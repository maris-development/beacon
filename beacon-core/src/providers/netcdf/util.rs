use std::sync::Arc;

use arrow::{array::*, datatypes::Field};
use netcdf::{types::NcVariableType, AttributeValue, File};

pub fn nc_type_to_arrow_type(
    nc_type: &NcVariableType,
) -> anyhow::Result<datafusion::arrow::datatypes::DataType> {
    match nc_type {
        NcVariableType::String => Ok(datafusion::arrow::datatypes::DataType::Utf8),
        NcVariableType::Int(int_type) => match int_type {
            netcdf::types::IntType::U8 => Ok(datafusion::arrow::datatypes::DataType::UInt8),
            netcdf::types::IntType::U16 => Ok(datafusion::arrow::datatypes::DataType::UInt16),
            netcdf::types::IntType::U32 => Ok(datafusion::arrow::datatypes::DataType::UInt32),
            netcdf::types::IntType::U64 => Ok(datafusion::arrow::datatypes::DataType::UInt64),
            netcdf::types::IntType::I8 => Ok(datafusion::arrow::datatypes::DataType::Int8),
            netcdf::types::IntType::I16 => Ok(datafusion::arrow::datatypes::DataType::Int16),
            netcdf::types::IntType::I32 => Ok(datafusion::arrow::datatypes::DataType::Int32),
            netcdf::types::IntType::I64 => Ok(datafusion::arrow::datatypes::DataType::Int64),
        },
        NcVariableType::Float(float_type) => match float_type {
            netcdf::types::FloatType::F32 => Ok(datafusion::arrow::datatypes::DataType::Float32),
            netcdf::types::FloatType::F64 => Ok(datafusion::arrow::datatypes::DataType::Float64),
        },
        NcVariableType::Char => Ok(datafusion::arrow::datatypes::DataType::Utf8),
        _ => Err(anyhow::anyhow!("Unsupported NetCDF type")),
    }
}

pub fn nc_attribute_to_arrow_type(
    nc_attribute: &netcdf::Attribute,
) -> anyhow::Result<datafusion::arrow::datatypes::DataType> {
    match nc_attribute.value()? {
        netcdf::AttributeValue::Uchar(_) => Ok(datafusion::arrow::datatypes::DataType::UInt8),
        netcdf::AttributeValue::Schar(_) => Ok(datafusion::arrow::datatypes::DataType::Int8),
        netcdf::AttributeValue::Ushort(_) => Ok(datafusion::arrow::datatypes::DataType::UInt16),
        netcdf::AttributeValue::Short(_) => Ok(datafusion::arrow::datatypes::DataType::Int16),
        netcdf::AttributeValue::Uint(_) => Ok(datafusion::arrow::datatypes::DataType::UInt32),
        netcdf::AttributeValue::Int(_) => Ok(datafusion::arrow::datatypes::DataType::Int32),
        netcdf::AttributeValue::Ulonglong(_) => Ok(datafusion::arrow::datatypes::DataType::UInt64),
        netcdf::AttributeValue::Longlong(_) => Ok(datafusion::arrow::datatypes::DataType::Int64),
        netcdf::AttributeValue::Float(_) => Ok(datafusion::arrow::datatypes::DataType::Float32),
        netcdf::AttributeValue::Double(_) => Ok(datafusion::arrow::datatypes::DataType::Float64),
        netcdf::AttributeValue::Str(_) => Ok(datafusion::arrow::datatypes::DataType::Utf8),
        dtype => Err(anyhow::anyhow!(
            "Unsupported NetCDF attribute type {:?}",
            dtype
        )),
    }
}

pub fn global_attributes_as_fields(nc_file: &File) -> Vec<Field> {
    let mut fields = vec![];

    for attr in nc_file.attributes() {
        let name = attr.name();
        if let Ok(dtype) = nc_attribute_to_arrow_type(&attr) {
            fields.push(Field::new(format!(".{}", name), dtype, true));
        }
    }

    fields
}

pub fn variables_as_fields(nc_file: &File) -> Vec<Field> {
    let mut fields = vec![];

    for var in nc_file.variables() {
        let name = var.name();
        let data_type = nc_type_to_arrow_type(&var.vartype()).unwrap();
        fields.push(Field::new(name, data_type, true));

        for attr in var.attributes() {
            let name = attr.name();
            if let Ok(dtype) = nc_attribute_to_arrow_type(&attr) {
                fields.push(Field::new(format!("{}.{}", var.name(), name), dtype, true));
            }
        }
    }

    fields
}

pub fn attribute_to_scalar(attribute_value: &AttributeValue) -> anyhow::Result<Scalar<ArrayRef>> {
    match attribute_value {
        AttributeValue::Uchar(value) => Ok(Scalar::new(Arc::new(UInt8Array::from(vec![*value])))),
        AttributeValue::Schar(value) => Ok(Scalar::new(Arc::new(Int8Array::from(vec![*value])))),
        AttributeValue::Ushort(value) => Ok(Scalar::new(Arc::new(UInt16Array::from(vec![*value])))),
        AttributeValue::Short(value) => Ok(Scalar::new(Arc::new(Int16Array::from(vec![*value])))),
        AttributeValue::Uint(value) => Ok(Scalar::new(Arc::new(UInt32Array::from(vec![*value])))),
        AttributeValue::Int(value) => Ok(Scalar::new(Arc::new(Int32Array::from(vec![*value])))),
        AttributeValue::Ulonglong(value) => {
            Ok(Scalar::new(Arc::new(UInt64Array::from(vec![*value]))))
        }
        AttributeValue::Longlong(value) => {
            Ok(Scalar::new(Arc::new(Int64Array::from(vec![*value]))))
        }
        AttributeValue::Float(value) => Ok(Scalar::new(Arc::new(Float32Array::from(vec![*value])))),
        AttributeValue::Double(value) => {
            Ok(Scalar::new(Arc::new(Float64Array::from(vec![*value]))))
        }
        AttributeValue::Str(value) => Ok(Scalar::new(Arc::new(StringArray::from(vec![
            value.to_string()
        ])))),
        _type => {
            return Err(anyhow::anyhow!(
                "Unsupported NetCDF attribute value type: {:?}",
                _type
            ))
        }
    }
}
