use std::{path::Path, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema};
use nd_arrow_array::NdArrowArray;
use ndarray::{ArrayBase, ArrayD};
use netcdf::{
    types::{FloatType, IntType},
    Attribute, Extents, Variable,
};

use crate::{
    cf_time::{decode_cf_time_variable, is_cf_time_variable},
    chunked_stream::{Chunking, Stream},
    error::ArrowNetCDFError,
    nc_array::{Dimension, NetCDFNdArray, NetCDFNdArrayBase, NetCDFNdArrayInner},
    NcChar, NcResult, NcString,
};

pub struct NetCDFArrowReader {
    file_schema: arrow::datatypes::SchemaRef,
    file: Arc<netcdf::File>,
}

impl NetCDFArrowReader {
    pub fn new<P: AsRef<Path>>(path: P) -> NcResult<Self> {
        let file = netcdf::open(path)?;
        let file_schema = Arc::new(arrow_schema(&file)?);
        Ok(Self {
            file_schema,
            file: Arc::new(file),
        })
    }

    pub fn new_with_aligned_dimensions<P: AsRef<Path>>(
        path: P,
        dimensions: Vec<String>,
    ) -> NcResult<Self> {
        let file = netcdf::open(path.as_ref())?;
        let file_schema = arrow_schema(&file)?;
        let file_dimensions = file
            .dimensions()
            .map(|d| d.name().to_string())
            .collect::<Vec<_>>();

        // Check if all specified dimensions are present in the file
        if !dimensions.iter().all(|d| file_dimensions.contains(d)) {
            return Err(ArrowNetCDFError::Reader(format!(
                "Not all specified dimensions: {:?} are present in the NetCDF dimensions: {:?} for file: {:?}.",
                dimensions,
                file_dimensions,
                path.as_ref()
            )));
        }

        // Check all the variables, and keep only the variables which have one of the specified dimensions or all of them
        // Scalars (0D variables) are always kept
        let mut removable_variables = vec![];
        for variable in file.variables() {
            let variable_dimensions = variable.dimensions();

            if variable_dimensions.is_empty() {
                continue; // Scalar variable, keep it
            }
            if variable.dimensions().len() == 1 {
                let dimension_name = variable_dimensions[0].name();
                if !dimensions.contains(&dimension_name) {
                    removable_variables.push(variable.name().to_string());
                }
            } else if !variable
                .dimensions()
                .iter()
                .all(|d| dimensions.contains(&d.name().to_string()))
            {
                removable_variables.push(variable.name().to_string());
            }
        }

        // Create a new schema excluding the removable variables
        let fields: Vec<arrow::datatypes::Field> = file_schema
            .fields()
            .iter()
            .filter(|field| {
                let field_name = field.name();
                // Remove the field if in the removable variables list
                !removable_variables.contains(field_name)
                    // Also remove variable attributes if the parent variable is removed
                    || (field_name.contains('.') && {
                        let parts: Vec<&str> = field_name.split('.').collect();
                        parts.len() == 2 && !removable_variables.contains(&parts[0].to_string())
                    })
            })
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();

        Ok(Self {
            file_schema: Arc::new(Schema::new(fields)),
            file: Arc::new(file),
        })
    }

    pub fn dimensions(&self) -> Vec<Dimension> {
        self.file
            .dimensions()
            .map(|d| Dimension {
                name: d.name().to_string(),
                size: d.len(),
            })
            .collect()
    }

    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.file_schema.clone()
    }

    pub fn read_as_batch<P: AsRef<[usize]>>(&self, projection: Option<P>) -> NcResult<RecordBatch> {
        let projected_schema = if let Some(projection) = projection {
            Arc::new(
                self.file_schema
                    .project(projection.as_ref())
                    .map_err(ArrowNetCDFError::ArrowSchemaProjectionError)?,
            )
        } else {
            self.file_schema.clone()
        };
        let mut stream = Stream::new(self.file.clone(), None, projected_schema)?;
        let batch = stream.next().transpose().unwrap().ok_or_else(|| {
            ArrowNetCDFError::Stream("No data available in the NetCDF file.".to_string())
        })?;
        batch.to_arrow_record_batch().map_err(|e| {
            ArrowNetCDFError::Stream(format!("Failed to flatten to RecordBatch: {}", e))
        })
    }

    pub fn read_as_stream<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
        chunking: Option<Chunking>,
    ) -> NcResult<Stream> {
        let projected_schema = if let Some(projection) = projection {
            Arc::new(
                self.file_schema
                    .project(projection.as_ref())
                    .map_err(ArrowNetCDFError::ArrowSchemaProjectionError)?,
            )
        } else {
            self.file_schema.clone()
        };
        Stream::new(self.file.clone(), chunking, projected_schema)
    }

    pub fn read_column_as_stream<P: AsRef<str>>(
        &self,
        column_name: P,
        chunking: Option<Chunking>,
    ) -> NcResult<Stream> {
        let column_name = column_name.as_ref();
        let column_index = self
            .file_schema
            .index_of(column_name)
            .map_err(|_| ArrowNetCDFError::InvalidFieldName(column_name.to_string()))?;

        self.read_as_stream(Some(&[column_index]), chunking)
    }

    pub fn read_column(&self, column_name: &str) -> NcResult<NdArrowArray> {
        self.file_schema.field_with_name(column_name).map_err(|_| {
            ArrowNetCDFError::ArrowSchemaError(Box::new(ArrowNetCDFError::InvalidFieldName(
                column_name.to_string(),
            )))
        })?;

        if column_name.contains('.') {
            let parts = column_name.split('.').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(ArrowNetCDFError::InvalidFieldName(column_name.to_string()));
            }
            if parts[0].is_empty() {
                //Global attribute
                let attr_name = parts[1];
                let attr_value = global_attribute(&self.file, attr_name)?
                    .expect("Attribute not found but was in schema.");
                Ok(attr_value.into_nd_arrow_array().unwrap())
            } else {
                //Variable attribute
                let variable = self
                    .file
                    .variable(parts[0])
                    .expect("Variable not found but was in schema.");
                Ok(variable_attribute(&variable, parts[1])?
                    .expect("Attribute not found but was in schema.")
                    .into_nd_arrow_array()
                    .unwrap())
            }
        } else {
            let variable = self
                .file
                .variable(column_name)
                .expect("Variable not found but was in schema.");
            let array = read_variable(&variable, None)
                .map_err(|e| ArrowNetCDFError::VariableReadError(Box::new(e)))?;
            Ok(array.into_nd_arrow_array().unwrap())
        }
    }
}

macro_rules! create_netcdf_ndarray {
    ($var:ident, $t:ty, $inner_variant:ident, $as_ref:ident, $dims:expr, $extents:expr) => {{
        let array = $var.get::<$t, _>($extents)?;
        let dims = if let Some(dims) = $dims {
            dims
        } else {
            $var.dimensions()
                .iter()
                .map(|d| Dimension {
                    name: d.name().to_string(),
                    size: d.len(),
                })
                .collect::<Vec<_>>()
        };
        let mut fill_value = $var.fill_value::<$t>()?;

        {
            fill_value = read_fill_value_attribute(&$var)?.and_then(|fv| fv.$as_ref())
        }

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

pub fn read_variable(
    variable: &Variable,
    mut extents_start_count: Option<(Vec<usize>, Vec<usize>)>,
) -> NcResult<NetCDFNdArray> {
    if is_cf_time_variable(variable) {
        if let Some(array) = decode_cf_time_variable(variable)
            .map_err(|e| ArrowNetCDFError::TimeVariableReadError(Box::new(e)))?
        {
            return Ok(array);
        }
    }

    let mut extents = extents_start_count
        .clone()
        .map(|e| e.try_into())
        .unwrap_or(Ok(Extents::All))
        .unwrap();
    let mut dims = vec![];
    for (i, dim) in variable.dimensions().iter().enumerate() {
        match extents_start_count.as_ref() {
            Some((_, count)) => {
                dims.push(Dimension::new(dim.name(), count[i]));
            }
            None => {
                dims.push(Dimension::new(dim.name(), dim.len()));
            }
        }
    }

    match variable.vartype() {
        netcdf::types::NcVariableType::Int(IntType::I8) => {
            create_netcdf_ndarray!(variable, i8, I8, as_i8, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::I16) => {
            create_netcdf_ndarray!(variable, i16, I16, as_i16, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::I32) => {
            create_netcdf_ndarray!(variable, i32, I32, as_i32, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::I64) => {
            create_netcdf_ndarray!(variable, i64, I64, as_i64, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::U8) => {
            create_netcdf_ndarray!(variable, u8, U8, as_u8, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::U16) => {
            create_netcdf_ndarray!(variable, u16, U16, as_u16, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::U32) => {
            create_netcdf_ndarray!(variable, u32, U32, as_u32, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Int(IntType::U64) => {
            create_netcdf_ndarray!(variable, u64, U64, as_u64, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Float(FloatType::F32) => {
            create_netcdf_ndarray!(variable, f32, F32, as_f32, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Float(FloatType::F64) => {
            create_netcdf_ndarray!(variable, f64, F64, as_f64, Some(dims), extents)
        }
        netcdf::types::NcVariableType::Char => {
            // NcChar is both a value itself or possibly a fixed size string
            //Get the last dimension of the variable
            if let Some(dim) = variable.dimensions().last() {
                //Check if the dimensions starts with string** or strlen**
                let dim_name = dim.name().to_lowercase();
                if dim_name.starts_with("string")
                    || dim_name.starts_with("strlen")
                    || dim_name.starts_with("strnlen")
                {
                    // Append the dimension and extents
                    dims.push(Dimension {
                        name: dim.name().to_string(),
                        size: dim.len(),
                    });

                    extents_start_count.as_mut().map(|ex| {
                        ex.0.push(0);
                        ex.1.push(dim.len());
                    });

                    extents = extents_start_count
                        .clone()
                        .map(|e| e.try_into())
                        .unwrap_or(Ok(Extents::All))
                        .unwrap();

                    let array = variable.get::<NcChar, _>(&extents)?;

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
            create_netcdf_ndarray!(variable, NcChar, Char, as_nc_char, Some(dims), extents)
        }
        netcdf::types::NcVariableType::String => {
            let strings = variable.get_strings(&extents)?;

            let base = NetCDFNdArrayBase::<String> {
                inner: ArrayD::from_shape_vec(
                    dims.iter().map(|d| d.size).collect::<Vec<_>>(),
                    strings,
                )
                .unwrap(),
                fill_value: None,
            };

            let inner_array = NetCDFNdArrayInner::String(base);
            Ok(NetCDFNdArray {
                dims,
                array: inner_array,
            })
        }
        nctype => Err(ArrowNetCDFError::UnsupportedNetCDFDataType(nctype)),
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
        match variable_as_arrow_field(&variable) {
            Ok(field) => {
                fields.push(field);
                let attr_fields = variable_attributes_as_arrow_fields(&variable);
                fields.extend(attr_fields);
            }
            Err(err) => {
                tracing::debug!(
                    "Skipping unsupported variable '{}': {}",
                    variable.name(),
                    err
                );
            }
        }
    }

    let global_attr_fields = global_attributes_as_arrow_fields(nc_file);
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
            arrow::datatypes::TimeUnit::Millisecond,
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
        netcdf::types::NcVariableType::String => Ok(arrow::datatypes::DataType::Utf8),
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

enum FillValueAttr {
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
}

impl FillValueAttr {
    fn as_nc_char(&self) -> Option<NcChar> {
        match self {
            FillValueAttr::U8(value) => Some(NcChar(*value)),
            FillValueAttr::String(value) if value.len() == 1 => Some(NcChar(value.as_bytes()[0])),
            _ => None,
        }
    }

    fn as_i8(&self) -> Option<i8> {
        match self {
            FillValueAttr::I8(value) => Some(*value),
            _ => None,
        }
    }
    fn as_u8(&self) -> Option<u8> {
        match self {
            FillValueAttr::U8(value) => Some(*value),
            _ => None,
        }
    }
    fn as_i16(&self) -> Option<i16> {
        match self {
            FillValueAttr::I16(value) => Some(*value),
            _ => None,
        }
    }
    fn as_u16(&self) -> Option<u16> {
        match self {
            FillValueAttr::U16(value) => Some(*value),
            _ => None,
        }
    }
    fn as_i32(&self) -> Option<i32> {
        match self {
            FillValueAttr::I32(value) => Some(*value),
            _ => None,
        }
    }
    fn as_u32(&self) -> Option<u32> {
        match self {
            FillValueAttr::U32(value) => Some(*value),
            _ => None,
        }
    }
    fn as_i64(&self) -> Option<i64> {
        match self {
            FillValueAttr::I64(value) => Some(*value),
            _ => None,
        }
    }
    fn as_u64(&self) -> Option<u64> {
        match self {
            FillValueAttr::U64(value) => Some(*value),
            _ => None,
        }
    }
    fn as_f32(&self) -> Option<f32> {
        match self {
            FillValueAttr::F32(value) => Some(*value),
            _ => None,
        }
    }
    fn as_f64(&self) -> Option<f64> {
        match self {
            FillValueAttr::F64(value) => Some(*value),
            _ => None,
        }
    }
    fn as_string(&self) -> Option<&str> {
        match self {
            FillValueAttr::String(value) => Some(value.as_str()),
            _ => None,
        }
    }
}

fn read_fill_value_attribute(variable: &Variable) -> NcResult<Option<FillValueAttr>> {
    let attr = variable.attribute("_FillValue");
    match attr {
        Some(attr) => {
            let value = attr.value()?;
            match value {
                netcdf::AttributeValue::Uchar(u8) => Ok(Some(FillValueAttr::U8(u8))),
                netcdf::AttributeValue::Schar(i8) => Ok(Some(FillValueAttr::I8(i8))),
                netcdf::AttributeValue::Ushort(u16) => Ok(Some(FillValueAttr::U16(u16))),
                netcdf::AttributeValue::Short(i16) => Ok(Some(FillValueAttr::I16(i16))),
                netcdf::AttributeValue::Uint(u32) => Ok(Some(FillValueAttr::U32(u32))),
                netcdf::AttributeValue::Int(i32) => Ok(Some(FillValueAttr::I32(i32))),
                netcdf::AttributeValue::Ulonglong(u64) => Ok(Some(FillValueAttr::U64(u64))),
                netcdf::AttributeValue::Longlong(i64) => Ok(Some(FillValueAttr::I64(i64))),
                netcdf::AttributeValue::Float(f32) => Ok(Some(FillValueAttr::F32(f32))),
                netcdf::AttributeValue::Double(f64) => Ok(Some(FillValueAttr::F64(f64))),
                netcdf::AttributeValue::Str(string) => Ok(Some(FillValueAttr::String(string))),
                _type => Err(ArrowNetCDFError::UnsupportedAttributeValueType(_type)),
            }
        }
        None => Ok(None),
    }
}

pub fn attribute_to_nd_array(attribute: &Attribute) -> NcResult<NetCDFNdArray> {
    match attribute.value()? {
        netcdf::AttributeValue::Schar(value) => {
            let base = NetCDFNdArrayBase::<i8> {
                inner: ArrayD::from_shape_vec(vec![], vec![value])
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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
                    .map_err(ArrowNetCDFError::AttributeShapeError)?,
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

#[cfg(test)]
mod tests {
    use netcdf::Options;

    use super::*;

    #[test]
    fn test_name() {
        let path = Path::new("https://s3.eu-west-3.amazonaws.com/argo-gdac-sandbox/pub/dac/aoml/13857/13857_prof.nc#mode=bytes");
        println!("{:?}", path);
        let ds = netcdf::open(path).unwrap();

        println!("{:?}", ds.dimensions().collect::<Vec<_>>());
    }
}
