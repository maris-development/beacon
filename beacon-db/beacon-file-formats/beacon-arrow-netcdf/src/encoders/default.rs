//! Default Arrow-to-NetCDF encoder.

use std::collections::HashMap;

use arrow::{
    array::{self, Array, ArrayRef},
    datatypes::{DataType, Field, SchemaRef},
};
use oxcdf::{AttributeValue, DType, FileMut};

use super::{discard, Encoder, EncoderError, EncoderOptions};

/// The text of `s` before its first NUL byte. A NetCDF string holds no NUL.
pub(crate) fn nul_free(s: &str) -> &str {
    match s.find('\0') {
        Some(nul) => {
            tracing::warn!(
                nul_position = nul,
                "NetCDF string contains an interior NUL byte; truncating at the first NUL"
            );
            &s[..nul]
        }
        None => s,
    }
}

/// Default encoder mapping common Arrow scalar arrays into NetCDF variables.
pub struct DefaultEncoder {
    nc_file: FileMut,
    _schema: SchemaRef,
    offsets: HashMap<String, usize>,
}

impl DefaultEncoder {
    /// Name of the record-axis dimension in produced NetCDF files.
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
            .map(|x| x.unwrap_or(<$native_type>::MAX))
            .collect::<Vec<_>>();

        $variable.put_values::<$native_type, _>(&temp_buffer, $extents)?;
    }};
}

impl DefaultEncoder {
    /// Define the record dimension and one variable for each field of `schema`.
    fn define_file(
        file: &mut FileMut,
        schema: &SchemaRef,
        options: &EncoderOptions,
    ) -> anyhow::Result<()> {
        //Add unlimited dimension to append data as it comes
        file.add_unlimited_dimension(Self::OBS_DIM_NAME)?;

        for field in schema.fields() {
            Self::define_variable(file, field)?;
            Self::set_storage(file, field, options)?;
        }
        Ok(())
    }

    fn define_variable(file: &mut FileMut, field: &Field) -> anyhow::Result<()> {
        match field.data_type() {
            DataType::FixedSizeBinary(size) => {
                let strlen_dim_name = format!("STRING{}", size);
                //Create STRING dimension if it doesnt exist
                if file.dimension_len(&strlen_dim_name).is_none() {
                    file.add_dimension(&strlen_dim_name, *size as usize)?;
                }

                let mut variable = file.add_variable_with_type(
                    field.name(),
                    &[Self::OBS_DIM_NAME, &strlen_dim_name],
                    &DType::Char,
                )?;

                variable.set_fill_value(0u8)?;
            }
            DataType::Utf8 => {
                file.add_string_variable(field.name(), &[Self::OBS_DIM_NAME])?;
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
            DataType::Float32 => {
                let mut variable = file.add_variable::<f32>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(f32::MAX)?;
            }
            DataType::Float64 => {
                let mut variable = file.add_variable::<f64>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(f64::MAX)?;
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
            // DataType::Utf8 => {
            //     anyhow::bail!("Variable sized string not supported yet");
            // }
            _ => anyhow::bail!(
                "Unsupported data type: {:?} for defining netcdf variable: {}",
                field.data_type(),
                field.name()
            ),
        }

        Ok(())
    }

    /// Set the chunk shape and the compression of the variable of `field`.
    fn set_storage(
        file: &mut FileMut,
        field: &Field,
        options: &EncoderOptions,
    ) -> anyhow::Result<()> {
        let mut variable = file
            .variable_mut(field.name())
            .ok_or_else(|| anyhow::anyhow!("NetCDF variable not found: {}", field.name()))?;
        if let Some(rows) = options.chunk_rows {
            let mut chunk = vec![rows];
            if let DataType::FixedSizeBinary(size) = field.data_type() {
                chunk.push(*size as usize);
            }
            variable.set_chunking(&chunk)?;
        }
        // netCDF-C 4.9 rejects a filter on a variable-length type, such as a string.
        let variable_length = field.data_type() == &DataType::Utf8;
        if let Some(compression) = options.compression.filter(|_| !variable_length) {
            variable.set_compression(compression.level.get().into(), compression.shuffle)?;
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

        let nc_file = &mut self.nc_file;
        let mut variable = nc_file
            .variable_mut(var_name)
            .ok_or_else(|| anyhow::anyhow!("NetCDF variable not found: {var_name}"))?;

        match array.data_type() {
            DataType::FixedSizeBinary(size) => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!("failed to downcast column {var_name} to FixedSizeBinaryArray")
                    })?;
                extents.push(0..*size as usize);

                // The bytes are row-major in (rows, fixed-size width) order.
                variable
                    .put_values::<u8, _>(array.value_data(), extents)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to write FixedSizeBinary column {var_name} to NetCDF: {e}"
                        )
                    })?;
            }
            DataType::Utf8 => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .expect("Failed to downcast to StringArray");

                let strings = string_array
                    .iter()
                    .map(|x| nul_free(x.unwrap_or("")))
                    .collect::<Vec<_>>();

                variable.put_strings(&strings, extents)?;
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
    fn create(
        mut nc_file: FileMut,
        schema: SchemaRef,
        options: &EncoderOptions,
    ) -> Result<Self, EncoderError>
    where
        Self: Sized,
    {
        if let Err(e) = Self::define_file(&mut nc_file, &schema, options) {
            discard(nc_file);
            return Err(e.into());
        }

        let offsets = schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), 0))
            .collect();

        Ok(Self {
            nc_file,
            _schema: schema,
            offsets,
        })
    }

    fn write_column(&mut self, name: &str, array: ArrayRef) -> Result<(), EncoderError> {
        let offset = *self.offsets.get(name).expect("Column not found in schema");

        self.write_array_chunk(name, array.clone(), offset)?;

        if let Some(x) = self.offsets.get_mut(name) {
            *x += array.len();
        }

        Ok(())
    }

    fn put_attribute(
        &mut self,
        variable: Option<&str>,
        name: &str,
        value: AttributeValue,
    ) -> Result<(), EncoderError> {
        let value = match value {
            AttributeValue::Str(text) => AttributeValue::Str(nul_free(&text).to_string()),
            AttributeValue::Strs(texts) => AttributeValue::Strs(
                texts.iter().map(|text| nul_free(text).to_string()).collect(),
            ),
            value => value,
        };
        match variable {
            Some(variable) => {
                let mut nc_variable = self
                    .nc_file
                    .variable_mut(variable)
                    .ok_or_else(|| format!("NetCDF variable not found: {variable}"))?;
                nc_variable.put_attribute(name, value)?;
            }
            None => {
                self.nc_file.add_attribute(name, value)?;
            }
        }
        Ok(())
    }

    fn into_file(self) -> FileMut {
        self.nc_file
    }

    fn encoder_name() -> &'static str {
        "DefaultEncoder"
    }
}
