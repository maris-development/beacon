use std::{cell::RefCell, collections::HashMap, rc::Rc};

use arrow::{
    array::{ArrayRef, Int16Array, RecordBatch},
    datatypes::{DataType, Field, SchemaRef},
};
use netcdf::{types::NcVariableType, FileMut};

use crate::NcChar;

use super::Encoder;

pub struct DefaultEncoder {
    nc_file: Rc<RefCell<FileMut>>,
    schema: SchemaRef,
    offsets: HashMap<String, usize>,
}

impl DefaultEncoder {
    const OBS_DIM_NAME: &'static str = "obs";
}

macro_rules! downcast_and_put_values {
    ($array:expr, $variable:expr, $extents:expr, $arrow_type:ty, $native_type:ty) => {{
        let array = $array
            .as_any()
            .downcast_ref::<$arrow_type>()
            .expect(concat!("Failed to downcast to ", stringify!($arrow_type)));

        let temp_buffer = array
            .iter()
            .map(|x| x.unwrap_or(<$native_type>::MIN))
            .collect::<Vec<_>>();

        $variable.put_values::<$native_type, _>(&temp_buffer, $extents)?;
    }};
}

impl DefaultEncoder {
    fn define_variable(file: &mut FileMut, field: &Field) -> anyhow::Result<()> {
        match field.data_type() {
            DataType::FixedSizeBinary(size) => {
                let strlen_dim_name = format!("STRING{}", size);
                //Create STRING dimension if it doesnt exist
                if file.dimension(&strlen_dim_name).is_none() {
                    file.add_dimension(&strlen_dim_name, *size as usize)?;
                }

                let mut variable = file.add_variable::<NcChar>(
                    field.name(),
                    &[Self::OBS_DIM_NAME, &strlen_dim_name],
                )?;

                variable.set_fill_value(NcChar(b'\0'))?;
            }
            DataType::Int8 => {
                let mut variable = file.add_variable::<i8>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(i8::MAX)?;
            }
            DataType::Int16 => {
                let mut variable = file.add_variable::<i16>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(i16::MAX)?;
            }
            DataType::Int32 => {
                let mut variable = file.add_variable::<i32>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(i32::MAX)?;
            }
            DataType::Int64 => {
                let mut variable = file.add_variable::<i64>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(i64::MAX)?;
            }
            DataType::UInt8 => {
                let mut variable = file.add_variable::<u8>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(u8::MAX)?;
            }
            DataType::UInt16 => {
                let mut variable = file.add_variable::<u16>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(u16::MAX)?;
            }
            DataType::UInt32 => {
                let mut variable = file.add_variable::<u32>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(u32::MAX)?;
            }
            DataType::UInt64 => {
                let mut variable = file.add_variable::<u64>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(u64::MAX)?;
            }
            DataType::Timestamp(time_unit, _) => {
                let mut variable = file.add_variable::<i64>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.put_attribute("calendar", "gregorian")?;
                variable.put_attribute("standard_name", "time")?;
                match time_unit {
                    arrow::datatypes::TimeUnit::Second => {
                        variable.put_attribute("units", "seconds since 1970-01-01T00:00:00Z")?;
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        variable
                            .put_attribute("units", "milliseconds since 1970-01-01T00:00:00Z")?;
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        variable
                            .put_attribute("units", "microseconds since 1970-01-01T00:00:00Z")?;
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        variable
                            .put_attribute("units", "nanoseconds since 1970-01-01T00:00:00Z")?;
                    }
                }
                variable.set_fill_value(i64::MAX)?;
            }
            DataType::Utf8 => {
                anyhow::bail!("Variable sized string not supported yet");
            }
            _ => anyhow::bail!(
                "Unsupported data type: {:?} for defining netcdf variable: {}",
                field.data_type(),
                field.name()
            ),
        }

        Ok(())
    }

    fn write_array_chunk(
        &mut self,
        var_name: &str,
        array: ArrayRef,
        offset: usize,
    ) -> anyhow::Result<()> {
        let mut extents = vec![offset..offset + array.len()];

        let mut nc_file = self.nc_file.borrow_mut();
        let mut variable = nc_file.variable_mut(var_name).expect("Variable not found");

        match array.data_type() {
            DataType::FixedSizeBinary(size) => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    .expect("Failed to downcast to FixedSizeBinaryArray");
                extents.push(0..*size as usize);

                let byte_slice = array.value_data();
                //We can transmute the byte slice to FixedSizeString as they have the same memory layout (1 ubyte) & repr(transparent)
                let fixed_size_string =
                    unsafe { std::mem::transmute::<&[u8], &[NcChar]>(byte_slice) };

                variable.put_values::<NcChar, _>(fixed_size_string, extents)?;
            }
            DataType::Int8 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Int8Array, i8);
            }
            DataType::Int16 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Int16Array, i16);
            }
            DataType::Int32 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Int32Array, i32);
            }
            DataType::Int64 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Int64Array, i64);
            }
            DataType::UInt8 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::UInt8Array, u8);
            }
            DataType::UInt16 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::UInt16Array, u16);
            }
            DataType::UInt32 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::UInt32Array, u32);
            }
            DataType::UInt64 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::UInt64Array, u64);
            }
            DataType::Float32 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Float32Array, f32);
            }
            DataType::Float64 => {
                downcast_and_put_values!(array, variable, extents, arrow::array::Float64Array, f64);
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampSecondArray,
                        i64
                    );
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampMillisecondArray,
                        i64
                    );
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampMicrosecondArray,
                        i64
                    );
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampNanosecondArray,
                        i64
                    );
                }
            },

            dtype => anyhow::bail!(
                "Unsupported data type: {:?} for writing netcdf variable chunk: {}",
                dtype,
                var_name
            ),
        }

        Ok(())
    }
}

impl Encoder for DefaultEncoder {
    fn create(nc_file: Rc<RefCell<FileMut>>, schema: SchemaRef) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        //Add unlimited dimension to append data as it comes
        nc_file
            .borrow_mut()
            .add_unlimited_dimension(Self::OBS_DIM_NAME)?;

        for field in schema.fields() {
            Self::define_variable(&mut nc_file.borrow_mut(), field)?;
        }

        let offsets = schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), 0))
            .collect();

        Ok(Self {
            nc_file,
            schema,
            offsets,
        })
    }

    fn write_column(&mut self, name: &str, array: ArrayRef) -> anyhow::Result<()> {
        let offset = *self.offsets.get(name).expect("Column not found in schema");

        self.write_array_chunk(name, array.clone(), offset)?;

        self.offsets.get_mut(name).map(|x| *x += array.len());

        Ok(())
    }
}
