use std::{path::Path, sync::Arc};

use arrow::array::RecordBatch;
use nd_arrow_array::NdArrowArray;
use ndarray::{ArrayBase, ArrayD};
use netcdf::{
    types::{FloatType, IntType},
    Attribute, Extents, NcTypeDescriptor, Variable,
};

use crate::{
    cf_time::{decode_cf_time_variable, is_cf_time_variable},
    error::ArrowNetCDFError,
    nc_array::{Dimension, NetCDFNdArray, NetCDFNdArrayBase, NetCDFNdArrayInner},
    NcChar, NcResult,
};

pub struct NetCDFArrowReader {
    file_schema: arrow::datatypes::SchemaRef,
    file: netcdf::File,
}

impl NetCDFArrowReader {
    pub fn new<P: AsRef<Path>>(path: P) -> NcResult<Self> {
        let file = netcdf::open(path)?;
        let file_schema = Arc::new(arrow_schema(&file)?);
        Ok(Self { file_schema, file })
    }

    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.file_schema.clone()
    }

    pub fn read_as_batch<P: AsRef<[usize]>>(&self, projection: Option<P>) -> NcResult<RecordBatch> {
        let projected_schema = if let Some(projection) = projection {
            Arc::new(
                self.file_schema
                    .project(projection.as_ref())
                    .map_err(|e| ArrowNetCDFError::ArrowSchemaProjectionError(e))?,
            )
        } else {
            self.file_schema.clone()
        };

        let mut columns = indexmap::IndexMap::new();
        for field in projected_schema.fields() {
            let name = field.name();
            if name.contains('.') {
                let parts = name.split('.').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(ArrowNetCDFError::InvalidFieldName(name.to_string()));
                }
                if parts[0].is_empty() {
                    //Global attribute
                    let attr_name = parts[1];
                    let attr_value = global_attribute(&self.file, attr_name)?
                        .expect("Attribute not found but was in schema.");
                    columns.insert(field.clone(), attr_value.into_nd_arrow_array());
                } else {
                    //Variable attribute
                    let variable = self
                        .file
                        .variable(parts[0])
                        .expect("Variable not found but was in schema.");
                    columns.insert(
                        field.clone(),
                        variable_attribute(&variable, parts[1])?
                            .expect("Attribute not found but was in schema.")
                            .into_nd_arrow_array(),
                    );
                }
            } else {
                let variable = self
                    .file
                    .variable(name)
                    .expect("Variable not found but was in schema.");
                let array = read_variable(&variable)
                    .map_err(|e| ArrowNetCDFError::VariableReadError(Box::new(e)))?;
                columns.insert(field.clone(), array.into_nd_arrow_array());
            }
        }
        let arrays = columns.iter().map(|(_, v)| v).collect::<Vec<_>>();
        let broadcast_shape = NdArrowArray::find_broadcast_shape(arrays)
            .map_err(|e| ArrowNetCDFError::UnableToBroadcast(e.to_string()))?;

        let mut arrays = vec![];
        for (_, column) in columns {
            let broadcast_array = column.broadcast(&broadcast_shape);
            arrays.push(broadcast_array.into_arrow_array());
        }

        let record_batch = RecordBatch::try_new(projected_schema, arrays)
            .map_err(|e| ArrowNetCDFError::ArrowRecordBatchError(e))?;

        Ok(record_batch)
    }
}

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

pub fn read_variable(variable: &Variable) -> NcResult<NetCDFNdArray> {
    if is_cf_time_variable(variable) {
        if let Some(array) = decode_cf_time_variable(variable)
            .map_err(|e| ArrowNetCDFError::TimeVariableReadError(Box::new(e)))?
        {
            return Ok(array);
        }
    }

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
            // NcChar is both a value itself or possibly a fixed size string
            let array = variable.get::<NcChar, _>(Extents::All)?;

            //Get the last dimension of the variable
            if let Some(dim) = variable.dimensions().last() {
                //Check if the dimensions starts with STRING** or strlen**
                if dim.name().starts_with("STRING")
                    || dim.name().starts_with("strlen")
                    || dim.name().starts_with("strnlen")
                {
                    let dims = variable
                        .dimensions()
                        .iter()
                        .take(variable.dimensions().len() - 1)
                        .map(|d| Dimension {
                            name: d.name().to_string(),
                            size: d.len(),
                        })
                        .collect::<Vec<_>>();

                    let fill_value = variable.fill_value::<NcChar>()?;
                    let base = NetCDFNdArrayBase::<NcChar> {
                        inner: array,
                        fill_value,
                    };
                    let inner_array = NetCDFNdArrayInner::FixedStringSize(base);

                    return Ok(NetCDFNdArray {
                        dims,
                        array: inner_array,
                    });
                }
            }
            return create_netcdf_ndarray!(variable, NcChar, Char);
        }
        nctype => {
            return Err(ArrowNetCDFError::UnsupportedNetCDFDataType(nctype));
        }
    }
}

pub fn global_attribute(
    nc_file: &netcdf::File,
    attribute_name: &str,
) -> NcResult<Option<NetCDFNdArray>> {
    let attr = nc_file.attribute(attribute_name);
    match attr {
        Some(attr) => attribute_to_nd_array(&attr).map(Some),
        None => Ok(None),
    }
}

pub fn arrow_schema(nc_file: &netcdf::File) -> NcResult<arrow::datatypes::Schema> {
    let mut fields = vec![];
    for variable in nc_file.variables() {
        let field = variable_as_arrow_field(&variable)?;
        fields.push(field);

        let attr_fields = variable_attributes_as_arrow_fields(&variable);
        fields.extend(attr_fields);
    }

    let global_attr_fields = global_attributes_as_arrow_fields(&nc_file);
    fields.extend(global_attr_fields);

    Ok(arrow::datatypes::Schema::new(fields))
}

fn variable_as_arrow_field(variable: &Variable) -> NcResult<arrow::datatypes::Field> {
    let name = variable.name();
    let arrow_type = variable_to_arrow_type(variable)?;
    Ok(arrow::datatypes::Field::new(name, arrow_type, true))
}

fn variable_attributes_as_arrow_fields(variable: &Variable) -> Vec<arrow::datatypes::Field> {
    let mut fields = Vec::new();
    let variable_name = variable.name();
    for attr in variable.attributes() {
        let name = attr.name();
        if let Ok(arrow_type) = attribute_to_arrow_type(&attr) {
            let field =
                arrow::datatypes::Field::new(format!("{variable_name}.{name}"), arrow_type, true);
            fields.push(field);
        }
    }
    fields
}

fn variable_to_arrow_type(variable: &Variable) -> NcResult<arrow::datatypes::DataType> {
    if is_cf_time_variable(variable) {
        return Ok(arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Second,
            None,
        ));
    }

    match variable.vartype() {
        netcdf::types::NcVariableType::Int(IntType::I8) => Ok(arrow::datatypes::DataType::Int8),
        netcdf::types::NcVariableType::Int(IntType::I16) => Ok(arrow::datatypes::DataType::Int16),
        netcdf::types::NcVariableType::Int(IntType::I32) => Ok(arrow::datatypes::DataType::Int32),
        netcdf::types::NcVariableType::Int(IntType::I64) => Ok(arrow::datatypes::DataType::Int64),
        netcdf::types::NcVariableType::Int(IntType::U8) => Ok(arrow::datatypes::DataType::UInt8),
        netcdf::types::NcVariableType::Int(IntType::U16) => Ok(arrow::datatypes::DataType::UInt16),
        netcdf::types::NcVariableType::Int(IntType::U32) => Ok(arrow::datatypes::DataType::UInt32),
        netcdf::types::NcVariableType::Int(IntType::U64) => Ok(arrow::datatypes::DataType::UInt64),
        netcdf::types::NcVariableType::Float(FloatType::F32) => {
            Ok(arrow::datatypes::DataType::Float32)
        }
        netcdf::types::NcVariableType::Float(FloatType::F64) => {
            Ok(arrow::datatypes::DataType::Float64)
        }
        netcdf::types::NcVariableType::Char => Ok(arrow::datatypes::DataType::Utf8),
        nctype => Err(ArrowNetCDFError::UnsupportedNetCDFDataType(nctype)),
    }
}

fn global_attributes_as_arrow_fields(nc_file: &netcdf::File) -> Vec<arrow::datatypes::Field> {
    let mut fields = Vec::new();
    for attr in nc_file.attributes() {
        let name = attr.name();
        if let Ok(arrow_type) = attribute_to_arrow_type(&attr) {
            let field = arrow::datatypes::Field::new(format!(".{name}"), arrow_type, true);
            fields.push(field);
        }
    }
    fields
}

fn attribute_to_arrow_type(attribute: &Attribute) -> NcResult<arrow::datatypes::DataType> {
    match attribute.value()? {
        netcdf::AttributeValue::Schar(_) => Ok(arrow::datatypes::DataType::Int8),
        netcdf::AttributeValue::Uchar(_) => Ok(arrow::datatypes::DataType::UInt8),
        netcdf::AttributeValue::Short(_) => Ok(arrow::datatypes::DataType::Int16),
        netcdf::AttributeValue::Ushort(_) => Ok(arrow::datatypes::DataType::UInt16),
        netcdf::AttributeValue::Int(_) => Ok(arrow::datatypes::DataType::Int32),
        netcdf::AttributeValue::Uint(_) => Ok(arrow::datatypes::DataType::UInt32),
        netcdf::AttributeValue::Longlong(_) => Ok(arrow::datatypes::DataType::Int64),
        netcdf::AttributeValue::Ulonglong(_) => Ok(arrow::datatypes::DataType::UInt64),
        netcdf::AttributeValue::Float(_) => Ok(arrow::datatypes::DataType::Float32),
        netcdf::AttributeValue::Double(_) => Ok(arrow::datatypes::DataType::Float64),
        netcdf::AttributeValue::Str(_) => Ok(arrow::datatypes::DataType::Utf8),
        attr_value => Err(ArrowNetCDFError::UnsupportedAttributeValueType(attr_value)),
    }
}

pub fn variable_attribute(
    variable: &Variable,
    attribute_name: &str,
) -> NcResult<Option<NetCDFNdArray>> {
    let attr = variable.attribute(attribute_name);
    match attr {
        Some(attr) => attribute_to_nd_array(&attr).map(Some),
        None => Ok(None),
    }
}

pub fn attribute_to_nd_array(attribute: &Attribute) -> NcResult<NetCDFNdArray> {
    match attribute.value()? {
        netcdf::AttributeValue::Schar(value) => {
            let base = NetCDFNdArrayBase::<i8> {
                inner: ArrayD::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::I8(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Uchar(value) => {
            let base = NetCDFNdArrayBase::<u8> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::U8(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Short(value) => {
            let base = NetCDFNdArrayBase::<i16> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::I16(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Ushort(value) => {
            let base = NetCDFNdArrayBase::<u16> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::U16(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Int(value) => {
            let base = NetCDFNdArrayBase::<i32> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::I32(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Uint(value) => {
            let base = NetCDFNdArrayBase::<u32> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::U32(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Longlong(value) => {
            let base = NetCDFNdArrayBase::<i64> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::I64(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Ulonglong(value) => {
            let base = NetCDFNdArrayBase::<u64> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::U64(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Float(value) => {
            let base = NetCDFNdArrayBase::<f32> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::F32(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        netcdf::AttributeValue::Double(value) => {
            let base = NetCDFNdArrayBase::<f64> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::F64(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }

        netcdf::AttributeValue::Str(value) => {
            let base = NetCDFNdArrayBase::<String> {
                inner: ArrayBase::from_shape_vec(vec![], vec![value])
                    .map_err(|e| ArrowNetCDFError::AttributeShapeError(e))?,
                fill_value: None,
            };
            let inner_array = NetCDFNdArrayInner::String(base);

            Ok(NetCDFNdArray {
                dims: vec![],
                array: inner_array,
            })
        }
        attr_value => Err(ArrowNetCDFError::UnsupportedAttributeValueType(attr_value)),
    }
}
