use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringBuilder, TimestampMillisecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, FieldRef};
use beacon_nd_arrow::array::backend::ArrayBackend;
use ndarray::{ArrayBase, ArrayViewD, Axis, Dim, IxDynImpl, OwnedRepr};
use netcdf::{types::NcVariableType, Extents};

use crate::{
    error::ArrowNetCDFError,
    reader::{global_attribute, read_variable, variable_attribute},
    NcChar, NcResult,
};

#[derive(Debug, Clone)]
pub struct NetCDFArrayBackend {
    nc_file: Arc<netcdf::File>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    arrow_field: FieldRef,
}

#[async_trait::async_trait]
impl ArrayBackend for NetCDFArrayBackend {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        let nd_array = if self.arrow_field.name().contains('.') {
            let parts = self.arrow_field.name().split('.').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid field name format for attribute: {}",
                    self.arrow_field.name()
                ));
            }
            if parts[0].is_empty() {
                //Global attribute
                let attr_name = parts[1];
                let attr_value = global_attribute(&self.nc_file, attr_name)?.ok_or(
                    anyhow::anyhow!("Attribute not found but was in schema: {}", attr_name),
                )?;
                attr_value
            } else {
                //Variable attribute
                let variable = self.nc_file.variable(parts[0]).ok_or(anyhow::anyhow!(
                    "Variable not found but was in schema: {}",
                    parts[0]
                ))?;
                variable_attribute(&variable, parts[1])?.ok_or(anyhow::anyhow!(
                    "Attribute not found but was in schema: {}.{}",
                    parts[0],
                    parts[1]
                ))?
            }
        } else {
            let variable =
                self.nc_file
                    .variable(self.arrow_field.name())
                    .ok_or(anyhow::anyhow!(
                        "Variable not found but was in schema: {}",
                        self.arrow_field.name()
                    ))?;
            let array = read_variable(&variable, None)
                .map_err(|e| ArrowNetCDFError::VariableReadError(Box::new(e)))?;
            array
        };

        let arrow_array = nd_array.build_arrow();
        // Slice the array according to the requested start and length
        let sliced_array = arrow_array.slice(start, length);
        Ok(sliced_array)
    }
}

pub struct NetCDFNdArrayBase<T> {
    pub inner: NetCDFNdArrayInnerBase<T>,
    pub fill_value: Option<T>,
}

pub type NetCDFNdArrayInnerBase<T> = ArrayBase<OwnedRepr<T>, Dim<IxDynImpl>>;

pub struct Dimension {
    pub name: String,
    pub size: usize,
}

impl Dimension {
    pub fn new(name: String, size: usize) -> Self {
        Self { name, size }
    }
}

pub struct NetCDFNdArray {
    pub dims: Vec<Dimension>,
    pub array: NetCDFNdArrayInner,
}

macro_rules! create_array_builder {
    ($array_type:ident, $array_base: expr) => {{
        let mut builder = $array_type::builder($array_base.inner.len());
        for value in $array_base.inner.iter() {
            if let Some(fill_value) = $array_base.fill_value {
                if *value == fill_value {
                    builder.append_null();
                } else {
                    builder.append_value(*value);
                }
            } else {
                builder.append_value(*value);
            }
        }
        Arc::new(builder.finish())
    }};
}

impl NetCDFNdArray {
    pub fn new(dims: Vec<Dimension>, array: NetCDFNdArrayInner) -> Self {
        Self { dims, array }
    }

    pub fn build_arrow(&self) -> ArrayRef {
        match &self.array {
            NetCDFNdArrayInner::U8(array_base) => {
                create_array_builder!(UInt8Array, array_base)
            }
            NetCDFNdArrayInner::U16(array_base) => {
                create_array_builder!(UInt16Array, array_base)
            }
            NetCDFNdArrayInner::U32(array_base) => {
                create_array_builder!(UInt32Array, array_base)
            }
            NetCDFNdArrayInner::U64(array_base) => {
                create_array_builder!(UInt64Array, array_base)
            }
            NetCDFNdArrayInner::I8(array_base) => {
                create_array_builder!(Int8Array, array_base)
            }
            NetCDFNdArrayInner::I16(array_base) => {
                create_array_builder!(Int16Array, array_base)
            }
            NetCDFNdArrayInner::I32(array_base) => {
                create_array_builder!(Int32Array, array_base)
            }
            NetCDFNdArrayInner::I64(array_base) => {
                create_array_builder!(Int64Array, array_base)
            }
            NetCDFNdArrayInner::F32(array_base) => {
                create_array_builder!(Float32Array, array_base)
            }
            NetCDFNdArrayInner::F64(array_base) => {
                create_array_builder!(Float64Array, array_base)
            }
            NetCDFNdArrayInner::TimestampSecond(array_base) => {
                create_array_builder!(TimestampSecondArray, array_base)
            }
            NetCDFNdArrayInner::TimestampMillisecond(array_base) => {
                create_array_builder!(TimestampMillisecondArray, array_base)
            }
            NetCDFNdArrayInner::Char(array_base) => {
                let mut builder = StringBuilder::new();
                char_to_arrow(array_base.inner.view(), &mut builder, array_base.fill_value);
                Arc::new(builder.finish())
            }
            NetCDFNdArrayInner::FixedStringSize(array_base) => {
                let mut builder = StringBuilder::new();
                fixed_sized_string_ndarray_to_arrow(
                    array_base.inner.view(),
                    &mut builder,
                    array_base.fill_value,
                );
                Arc::new(builder.finish())
            }
            NetCDFNdArrayInner::String(array_base) => {
                let mut builder = StringBuilder::new();
                for s in array_base.inner.iter() {
                    if s.is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(s);
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}

fn char_to_arrow<'a>(
    array: ArrayViewD<'a, NcChar>,
    builder: &mut StringBuilder,
    fill_value: Option<NcChar>,
) {
    array.iter().for_each(|c| {
        if *c == fill_value.unwrap_or(NcChar(0)) {
            builder.append_null();
        } else {
            let buffer = &[c.0];
            let value = String::from_utf8_lossy(buffer);
            if let Some(fill_value) = fill_value {
                // Remove the u8 from the value
                builder.append_value(value.trim_end_matches(fill_value.0 as char));
            } else {
                builder.append_value(value);
            }
        }
    });
}

fn fixed_sized_string_ndarray_to_arrow<'a>(
    array: ArrayViewD<'a, NcChar>,
    builder: &mut StringBuilder,
    fill_value: Option<NcChar>,
) {
    let ndim = array.ndim();

    if ndim == 1 {
        // Base case: Convert (string_length) into &str
        let nc_bytes = array.as_slice().expect("Array should be contiguous");
        //Transmute to &[u8] slice
        let bytes = unsafe { std::mem::transmute::<&[NcChar], &[u8]>(nc_bytes) };
        let s = String::from_utf8_lossy(bytes);
        let string = s.trim_end_matches('\0');
        if string.is_empty() {
            builder.append_null();
        } else {
            // Remove trailing white space from string
            let string = string.trim_end();
            if let Some(fill_value) = fill_value {
                builder.append_value(string.trim_end_matches(fill_value.0 as char));
            } else {
                builder.append_value(string); // Zero-copy string slice
            }
        }
        return;
    }

    if ndim == 2 {
        // Base case: Convert (rows, string_length) into Vec<&str>
        for row in array.rows() {
            // Base case: Convert (string_length) into &str
            let nc_bytes = row.as_slice().expect("Array should be contiguous");
            //Transmute to &[u8] slice
            let bytes = unsafe { std::mem::transmute::<&[NcChar], &[u8]>(nc_bytes) };
            let s = String::from_utf8_lossy(bytes);
            let string = s.trim_end_matches('\0');
            if string.is_empty() {
                builder.append_null();
            } else {
                // Remove trailing white space from string
                let string = string.trim_end();
                if let Some(fill_value) = fill_value {
                    builder.append_value(string.trim_end_matches(fill_value.0 as char));
                } else {
                    builder.append_value(string); // Zero-copy string slice
                }
            }
        }
        return;
    }

    // Recursive case: Process higher dimensions
    for sub_array in array.axis_iter(Axis(0)) {
        fixed_sized_string_ndarray_to_arrow(sub_array, builder, fill_value);
    }
}

pub enum NetCDFNdArrayInner {
    U8(NetCDFNdArrayBase<u8>),
    U16(NetCDFNdArrayBase<u16>),
    U32(NetCDFNdArrayBase<u32>),
    U64(NetCDFNdArrayBase<u64>),
    I8(NetCDFNdArrayBase<i8>),
    I16(NetCDFNdArrayBase<i16>),
    I32(NetCDFNdArrayBase<i32>),
    I64(NetCDFNdArrayBase<i64>),
    F32(NetCDFNdArrayBase<f32>),
    F64(NetCDFNdArrayBase<f64>),
    TimestampSecond(NetCDFNdArrayBase<i64>),
    TimestampMillisecond(NetCDFNdArrayBase<i64>),
    Char(NetCDFNdArrayBase<NcChar>),
    FixedStringSize(NetCDFNdArrayBase<NcChar>),
    String(NetCDFNdArrayBase<String>),
}
