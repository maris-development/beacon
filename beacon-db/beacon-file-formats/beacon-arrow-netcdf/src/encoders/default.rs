//! Default Arrow-to-NetCDF encoder.

use std::collections::HashMap;

use arrow::{
    array::{self, Array, ArrayRef},
    datatypes::{DataType, Field, SchemaRef, TimeUnit},
};
use ndarray::ArrayView;
use netcdf::{AttributeValue, FileMut, NcTypeDescriptor};

use crate::{NcChar, NcString};

use super::{Encoder, EncoderError, EncoderOptions};

/// Default encoder mapping common Arrow scalar arrays into NetCDF variables.
pub struct DefaultEncoder {
    nc_file: FileMut,
    _schema: SchemaRef,
    offsets: HashMap<String, usize>,
    /// The settings of the variables: fill values and time units.
    options: EncoderOptions,
}

impl DefaultEncoder {
    /// Name of the record-axis dimension in produced NetCDF files.
    const OBS_DIM_NAME: &'static str = "obs";
}

/// A numeric NetCDF type and its fill value.
trait FillValue: NcTypeDescriptor + Copy {
    /// The fill value of a variable with no source fill value.
    const DEFAULT: Self;
    /// `value` as this type, or `None` when it has another type.
    fn from_attribute(value: &AttributeValue) -> Option<Self>;
}

macro_rules! impl_fill_value {
    ($native_type:ty, $default:expr, $variant:ident) => {
        impl FillValue for $native_type {
            const DEFAULT: Self = $default;

            fn from_attribute(value: &AttributeValue) -> Option<Self> {
                match value {
                    AttributeValue::$variant(value) => Some(*value),
                    _ => None,
                }
            }
        }
    };
}

impl_fill_value!(i8, i8::MAX, Schar);
impl_fill_value!(i16, i16::MAX, Short);
impl_fill_value!(i32, i32::MAX, Int);
impl_fill_value!(i64, i64::MAX, Longlong);
impl_fill_value!(u8, u8::MAX, Uchar);
impl_fill_value!(u16, u16::MAX, Ushort);
impl_fill_value!(u32, u32::MAX, Uint);
impl_fill_value!(u64, u64::MAX, Ulonglong);
// xarray writes NaN, and a tool that does not mask still shows a float null as NaN.
impl_fill_value!(f32, f32::NAN, Float);
impl_fill_value!(f64, f64::NAN, Double);

/// The fill value of the variable `name`.
fn fill_value<T: FillValue>(fill_values: &HashMap<String, AttributeValue>, name: &str) -> T {
    fill_values
        .get(name)
        .and_then(T::from_attribute)
        .unwrap_or(T::DEFAULT)
}

macro_rules! downcast_and_put_values {
    ($array:expr, $variable:expr, $extents:expr, $arrow_type:ty, $native_type:ty, $fill:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<$arrow_type>()
            .expect(concat!("Failed to downcast to ", stringify!($arrow_type)));

        let fill: $native_type = $fill;
        let temp_buffer = array.iter().map(|x| x.unwrap_or(fill)).collect::<Vec<_>>();

        $variable.put_values::<$native_type, _>(&temp_buffer, $extents)?;
    }};
}

impl DefaultEncoder {
    /// Define a numeric variable on `obs` with its fill value.
    fn define_numeric<T: FillValue>(
        file: &mut FileMut,
        name: &str,
        fill_values: &HashMap<String, AttributeValue>,
    ) -> anyhow::Result<()> {
        let mut variable = file.add_variable::<T>(name, &[Self::OBS_DIM_NAME])?;
        variable.set_fill_value(fill_value::<T>(fill_values, name))?;
        Ok(())
    }

    /// Whether the timestamp variable `name` holds floats in `time_units`.
    fn is_float_time(options: &EncoderOptions, name: &str) -> bool {
        options.time_units.is_some() && options.float_time_columns.contains(name)
    }

    fn define_variable(
        file: &mut FileMut,
        field: &Field,
        options: &EncoderOptions,
    ) -> anyhow::Result<()> {
        let name = field.name();
        let fill_values = &options.fill_values;
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
            DataType::Utf8 => {
                let mut variable =
                    file.add_variable::<NcString>(field.name(), &[Self::OBS_DIM_NAME])?;
                variable.set_fill_value(NcString::new(""))?;
            }
            DataType::Int8 => Self::define_numeric::<i8>(file, name, fill_values)?,
            DataType::Int16 => Self::define_numeric::<i16>(file, name, fill_values)?,
            DataType::Int32 => Self::define_numeric::<i32>(file, name, fill_values)?,
            DataType::Int64 => Self::define_numeric::<i64>(file, name, fill_values)?,
            DataType::UInt8 => Self::define_numeric::<u8>(file, name, fill_values)?,
            DataType::UInt16 => Self::define_numeric::<u16>(file, name, fill_values)?,
            DataType::UInt32 => Self::define_numeric::<u32>(file, name, fill_values)?,
            DataType::UInt64 => Self::define_numeric::<u64>(file, name, fill_values)?,
            DataType::Float32 => Self::define_numeric::<f32>(file, name, fill_values)?,
            DataType::Float64 => Self::define_numeric::<f64>(file, name, fill_values)?,
            DataType::Timestamp(time_unit, _) => {
                let mut variable = if Self::is_float_time(options, name) {
                    let mut variable = file.add_variable::<f64>(name, &[Self::OBS_DIM_NAME])?;
                    variable.set_fill_value(f64::NAN)?;
                    variable
                } else {
                    let mut variable = file.add_variable::<i64>(name, &[Self::OBS_DIM_NAME])?;
                    variable.set_fill_value(i64::MAX)?;
                    variable
                };
                variable.put_attribute("calendar", "standard")?;
                variable.put_attribute("standard_name", "time")?;
                let units = match (&options.time_units, time_unit) {
                    (Some(units), _) => units.as_str(),
                    (None, TimeUnit::Second) => "seconds since 1970-01-01T00:00:00Z",
                    (None, TimeUnit::Millisecond) => "milliseconds since 1970-01-01T00:00:00Z",
                    (None, TimeUnit::Microsecond) => "microseconds since 1970-01-01T00:00:00Z",
                    (None, TimeUnit::Nanosecond) => "nanoseconds since 1970-01-01T00:00:00Z",
                };
                variable.put_attribute("units", units)?;
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

        let fills = &self.options.fill_values;
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

                let byte_slice = array.value_data();

                // Shape is (rows, fixed-size width); previously this was a hardcoded
                // (1_000_000, 5), which panicked for any other array dimensions.
                let view = ArrayView::from_shape((array.len(), *size as usize), byte_slice)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to view FixedSizeBinary column {var_name} as ({}, {}): {e}",
                            array.len(),
                            size
                        )
                    })?;

                variable.put(view, extents).map_err(|e| {
                    anyhow::anyhow!("failed to write FixedSizeBinary column {var_name} to NetCDF: {e}")
                })?;
            }
            DataType::Utf8 => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .expect("Failed to downcast to StringArray");

                let cstrings = string_array
                    .iter()
                    .map(|x| NcString::new(x.unwrap_or("")))
                    .collect::<Vec<_>>();

                variable.put_values::<NcString, _>(&cstrings, extents)?;
            }
            DataType::Int8 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Int8Array,
                    i8,
                    fill_value(fills, var_name)
                );
            }
            DataType::Int16 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Int16Array,
                    i16,
                    fill_value(fills, var_name)
                );
            }
            DataType::Int32 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Int32Array,
                    i32,
                    fill_value(fills, var_name)
                );
            }
            DataType::Int64 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Int64Array,
                    i64,
                    fill_value(fills, var_name)
                );
            }
            DataType::UInt8 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::UInt8Array,
                    u8,
                    fill_value(fills, var_name)
                );
            }
            DataType::UInt16 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::UInt16Array,
                    u16,
                    fill_value(fills, var_name)
                );
            }
            DataType::UInt32 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::UInt32Array,
                    u32,
                    fill_value(fills, var_name)
                );
            }
            DataType::UInt64 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::UInt64Array,
                    u64,
                    fill_value(fills, var_name)
                );
            }
            DataType::Float32 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Float32Array,
                    f32,
                    fill_value(fills, var_name)
                );
            }
            DataType::Float64 => {
                downcast_and_put_values!(
                    array,
                    variable,
                    extents,
                    arrow::array::Float64Array,
                    f64,
                    fill_value(fills, var_name)
                );
            }
            DataType::Timestamp(_, _) if self.options.time_units.is_some() => {
                let units = self.options.time_units.as_ref().expect("checked above");
                if Self::is_float_time(&self.options, var_name) {
                    variable.put_values(&units.encode_float(&array, f64::NAN), extents)?;
                } else {
                    variable.put_values(&units.encode_integer(&array, i64::MAX), extents)?;
                }
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampSecondArray,
                        i64,
                        i64::MAX
                    );
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampMillisecondArray,
                        i64,
                        i64::MAX
                    );
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampMicrosecondArray,
                        i64,
                        i64::MAX
                    );
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_put_values!(
                        array,
                        variable,
                        extents,
                        arrow::array::TimestampNanosecondArray,
                        i64,
                        i64::MAX
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
        // A length of 0 means unlimited in netCDF, so an empty output stays unlimited.
        match options.record_rows {
            Some(rows) if rows > 0 => nc_file.add_dimension(Self::OBS_DIM_NAME, rows)?,
            _ => nc_file.add_unlimited_dimension(Self::OBS_DIM_NAME)?,
        };

        for field in schema.fields() {
            Self::define_variable(&mut nc_file, field, options)?;
            Self::set_storage(&mut nc_file, field, options)?;
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
            options: options.clone(),
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
        value: netcdf::AttributeValue,
    ) -> Result<(), EncoderError> {
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

    fn encoder_name() -> &'static str {
        "DefaultEncoder"
    }
}
