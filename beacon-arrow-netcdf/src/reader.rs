use netcdf::{
    types::{FloatType, IntType},
    Extents, Variable,
};

use crate::{nc_array::NetCDFNdArrayInner, NcChar};

pub fn read_variable(variable: &Variable) -> anyhow::Result<NetCDFNdArrayInner> {
    match variable.vartype() {
        netcdf::types::NcVariableType::Int(IntType::I8) => {
            let array = variable.get::<i8, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::I8(array))
        }
        netcdf::types::NcVariableType::Int(IntType::I16) => {
            let array = variable.get::<i16, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::I16(array))
        }
        netcdf::types::NcVariableType::Int(IntType::I32) => {
            let array = variable.get::<i32, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::I32(array))
        }
        netcdf::types::NcVariableType::Int(IntType::I64) => {
            let array = variable.get::<i64, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::I64(array))
        }
        netcdf::types::NcVariableType::Int(IntType::U8) => {
            let array = variable.get::<u8, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::U8(array))
        }
        netcdf::types::NcVariableType::Int(IntType::U16) => {
            let array = variable.get::<u16, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::U16(array))
        }
        netcdf::types::NcVariableType::Int(IntType::U32) => {
            let array = variable.get::<u32, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::U32(array))
        }
        netcdf::types::NcVariableType::Int(IntType::U64) => {
            let array = variable.get::<u64, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::U64(array))
        }
        netcdf::types::NcVariableType::Float(FloatType::F32) => {
            let array = variable.get::<f32, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::F32(array))
        }
        netcdf::types::NcVariableType::Float(FloatType::F64) => {
            let array = variable.get::<f64, _>(Extents::All)?;
            Ok(NetCDFNdArrayInner::F64(array))
        }
        netcdf::types::NcVariableType::Char => {
            // NcChar is both a value itself or possibly a fixed size string
            let array = variable.get::<NcChar, _>(Extents::All)?;

            //Get the last dimension of the variable
            if let Some(dim) = variable.dimensions().last() {
                //Check if the dimensions starts with STRING** or strlen**
                if dim.name().starts_with("STRING") || dim.name().starts_with("strlen") {
                    return Ok(NetCDFNdArrayInner::FixedStringSize(array));
                }
            }

            Ok(NetCDFNdArrayInner::Char(array))
        }
        nctype => anyhow::bail!("Unsupported variable type: {:?}", nctype),
    }
}
