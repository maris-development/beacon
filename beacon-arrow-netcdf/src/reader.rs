use ndarray::{ArrayBase, ArrayD};
use netcdf::{
    types::{FloatType, IntType},
    Attribute, Extents, NcTypeDescriptor, Variable,
};

use crate::{
    nc_array::{Dimension, NetCDFNdArray, NetCDFNdArrayBase, NetCDFNdArrayInner},
    NcChar,
};

macro_rules! create_netcdf_ndarray {
    ($var:ident, $t:ty, $inner_variant:ident) => {{
        let array = $var.get::<$t, _>(Extents::All)?;
        let dims = $var
            .dimensions()
            .iter()
            .map(|d| Dimension {
                name: d.name().to_string(),
                size: d.len(),
            })
            .collect::<Vec<_>>();
        let fill_value = $var.fill_value::<$t>()?;
        let base = NetCDFNdArrayBase::<$t> {
            inner: array,
            fill_value,
        };
        let inner_array = NetCDFNdArrayInner::$inner_variant(base);

        Ok(NetCDFNdArray {
            dims,
            array: inner_array,
        })
    }};
}

pub fn read_variable(variable: &Variable) -> anyhow::Result<NetCDFNdArray> {
    match variable.vartype() {
        netcdf::types::NcVariableType::Int(IntType::I8) => {
            create_netcdf_ndarray!(variable, i8, I8)
        }
        netcdf::types::NcVariableType::Int(IntType::I16) => {
            create_netcdf_ndarray!(variable, i16, I16)
        }
        netcdf::types::NcVariableType::Int(IntType::I32) => {
            create_netcdf_ndarray!(variable, i32, I32)
        }
        netcdf::types::NcVariableType::Int(IntType::I64) => {
            create_netcdf_ndarray!(variable, i64, I64)
        }
        netcdf::types::NcVariableType::Int(IntType::U8) => {
            create_netcdf_ndarray!(variable, u8, U8)
        }
        netcdf::types::NcVariableType::Int(IntType::U16) => {
            create_netcdf_ndarray!(variable, u16, U16)
        }
        netcdf::types::NcVariableType::Int(IntType::U32) => {
            create_netcdf_ndarray!(variable, u32, U32)
        }
        netcdf::types::NcVariableType::Int(IntType::U64) => {
            create_netcdf_ndarray!(variable, u64, U64)
        }
        netcdf::types::NcVariableType::Float(FloatType::F32) => {
            create_netcdf_ndarray!(variable, f32, F32)
        }
        netcdf::types::NcVariableType::Float(FloatType::F64) => {
            create_netcdf_ndarray!(variable, f64, F64)
        }
        netcdf::types::NcVariableType::Char => {
            // // NcChar is both a value itself or possibly a fixed size string
            // let array = variable.get::<NcChar, _>(Extents::All)?;

            // //Get the last dimension of the variable
            // if let Some(dim) = variable.dimensions().last() {
            //     //Check if the dimensions starts with STRING** or strlen**
            //     if dim.name().starts_with("STRING") || dim.name().starts_with("strlen") {
            //         return Ok(NetCDFNdArrayInner::FixedStringSize(array));
            //     }
            // }

            // Ok(NetCDFNdArrayInner::Char(array))
            todo!()
        }
        nctype => anyhow::bail!("Unsupported variable type: {:?}", nctype),
    }
}

pub fn variable_attribute(
    variable: &Variable,
    attribute_name: &str,
) -> anyhow::Result<Option<NetCDFNdArray>> {
    let attr = variable.attribute(attribute_name);
    match attr {
        Some(attr) => attribute_to_nd_array(&attr).map(Some),
        None => Ok(None),
    }
}

pub fn attribute_to_nd_array(attribute: &Attribute) -> anyhow::Result<NetCDFNdArray> {
    match attribute.value()? {
        netcdf::AttributeValue::Schar(value) => {
            let base = NetCDFNdArrayBase::<i8> {
                inner: ArrayD::from_shape_vec(vec![], vec![value])?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::I8(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Uchar(array) => {
            todo!()
        }
        netcdf::AttributeValue::Short(array) => {
            todo!()
        }
        netcdf::AttributeValue::Ushort(array) => {
            todo!()
        }
        netcdf::AttributeValue::Int(array) => {
            todo!()
        }
        netcdf::AttributeValue::Uint(array) => {
            todo!()
        }
        netcdf::AttributeValue::Longlong(array) => {
            todo!()
        }
        netcdf::AttributeValue::Ulonglong(array) => {
            todo!()
        }
        netcdf::AttributeValue::Float(array) => {
            todo!()
        }
        netcdf::AttributeValue::Double(array) => {
            todo!()
        }

        netcdf::AttributeValue::Str(array) => {
            todo!()
        }
        attr_value => anyhow::bail!("Unsupported attribute value: {:?}", attr_value),
    }
}
