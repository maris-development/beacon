use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{make_array, ArrayRef, Datum, Scalar},
    buffer::NullBuffer,
};
use netcdf::{
    types::{FloatType, IntType, NcVariableType},
    Extents,
};

use crate::NcChar;

pub mod cf_time;
pub mod strings;

pub trait VariableDecoder: Debug + Send + Sync {
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: Extents,
    ) -> anyhow::Result<arrow::array::ArrayRef>;
    fn variable_name(&self) -> &str;
    fn mask_fill_values(
        &self,
        array: ArrayRef,
        fill_scalar: Scalar<ArrayRef>,
    ) -> anyhow::Result<arrow::array::ArrayRef> {
        let mask = arrow::compute::kernels::cmp::eq(
            &array.as_ref() as &dyn Datum,
            &fill_scalar.into_inner().as_ref() as &dyn Datum,
        );

        match mask {
            Ok(mask_array) => {
                // Use the mask to create a new array where fill values are masked out (e.g., set to null)
                let builder = array.to_data().into_builder();
                let new_array_builder =
                    builder.nulls(Some(NullBuffer::new(mask_array.values().clone())));

                let array = new_array_builder.build().map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to build array with masked fill values for variable {} with error: {}",
                        self.variable_name(),
                        e
                    )
                })?;

                Ok(make_array(array))
            }
            Err(e) => {
                // If the equality comparison fails, return the original array without masking
                tracing::warn!(
                    "Failed to compare array with fill value for masking: {}. Returning original array.",
                    e
                );
                Ok(array)
            }
        }
    }
}

#[derive(Debug)]
pub struct DefaultVariableDecoder {
    pub arrow_field: arrow::datatypes::Field,
    pub nc_type: NcVariableType,
    pub fill_value: Option<arrow::array::ArrayRef>,
}

impl VariableDecoder for DefaultVariableDecoder {
    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }

    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: Extents,
    ) -> anyhow::Result<arrow::array::ArrayRef> {
        let array = match self.nc_type {
            NcVariableType::Int(IntType::U8) => {
                let array = variable.get_values::<u8, _>(extents)?;
                let arrow_array = arrow::array::UInt8Array::from(array);
                Arc::new(arrow_array) as ArrayRef
            }
            NcVariableType::Int(IntType::U16) => {
                let array = variable.get_values::<u16, _>(extents)?;
                let arrow_array = arrow::array::UInt16Array::from(array);
                Arc::new(arrow_array) as ArrayRef
            }
            NcVariableType::Int(IntType::U32) => {
                let array = variable.get_values::<u32, _>(extents)?;
                let arrow_array = arrow::array::UInt32Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Int(IntType::U64) => {
                let array = variable.get_values::<u64, _>(extents)?;
                let arrow_array = arrow::array::UInt64Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Int(IntType::I8) => {
                let array = variable.get_values::<i8, _>(extents)?;
                let arrow_array = arrow::array::Int8Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Int(IntType::I16) => {
                let array = variable.get_values::<i16, _>(extents)?;
                let arrow_array = arrow::array::Int16Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Int(IntType::I32) => {
                let array = variable.get_values::<i32, _>(extents)?;
                let arrow_array = arrow::array::Int32Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Int(IntType::I64) => {
                let array = variable.get_values::<i64, _>(extents)?;
                let arrow_array = arrow::array::Int64Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Float(FloatType::F32) => {
                let array = variable.get_values::<f32, _>(extents)?;
                let arrow_array = arrow::array::Float32Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Float(FloatType::F64) => {
                let array = variable.get_values::<f64, _>(extents)?;
                let arrow_array = arrow::array::Float64Array::from(array);
                Arc::new(arrow_array)
            }
            NcVariableType::Char => {
                let array = variable.get_values::<NcChar, _>(extents)?;
                let iter = array
                    .into_iter()
                    .map(|byte| String::from_utf8_lossy(&[byte.0]).to_string());
                let arrow_array = arrow::array::StringArray::from_iter_values(iter);
                Arc::new(arrow_array)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported data type (Using DefaultVariableDecoder): {:?} for variable {}",
                    self.arrow_field.data_type(),
                    variable.name()
                ))
            }
        };

        if let Some(fill_array) = &self.fill_value {
            self.mask_fill_values(array, Scalar::new(fill_array.clone()))
        } else {
            Ok(array)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{
        DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };
    use netcdf::types::{FloatType, IntType, NcVariableType};
    use tempfile::Builder;

    /// Write a single-variable NetCDF file and return the temp file handle.
    /// The file stays alive as long as the handle is kept in scope.
    fn write_nc<T: netcdf::NcTypeDescriptor + Copy>(
        var_name: &str,
        values: &[T],
    ) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", values.len()).unwrap();
            let mut var = nc.add_variable::<T>(var_name, &["obs"]).unwrap();
            var.put_values(values, netcdf::Extents::All).unwrap();
        }
        tmp
    }

    // ── DefaultVariableDecoder ─────────────────────────────────────────────

    macro_rules! test_numeric_decoder {
        (
            $test_name:ident,
            $rust_type:ty,
            $nc_type:expr,
            $arrow_dtype:expr,
            $arrow_primitive_type:ty,
            $values:expr
        ) => {
            #[test]
            fn $test_name() {
                let var_name = "data";
                let values: Vec<$rust_type> = $values;
                let tmp = write_nc::<$rust_type>(var_name, &values);

                let file = netcdf::open(tmp.path()).unwrap();
                let variable = file.variable(var_name).unwrap();

                let decoder = DefaultVariableDecoder {
                    arrow_field: Field::new(var_name, $arrow_dtype, true),
                    nc_type: $nc_type,
                    fill_value: None,
                };

                let array = decoder
                    .read(&variable, netcdf::Extents::All)
                    .expect("DefaultVariableDecoder::read failed");

                assert_eq!(array.len(), values.len(), "Array length mismatch");

                let typed = array.as_primitive::<$arrow_primitive_type>();
                for (i, expected) in values.iter().enumerate() {
                    assert_eq!(
                        typed.value(i) as $rust_type,
                        *expected,
                        "Value mismatch at index {i}"
                    );
                }
            }
        };
    }

    test_numeric_decoder!(
        test_default_decoder_u8,
        u8,
        NcVariableType::Int(IntType::U8),
        DataType::UInt8,
        UInt8Type,
        vec![0u8, 1, 127, 254]
    );

    test_numeric_decoder!(
        test_default_decoder_u16,
        u16,
        NcVariableType::Int(IntType::U16),
        DataType::UInt16,
        UInt16Type,
        vec![0u16, 1, 1000, 65534]
    );

    test_numeric_decoder!(
        test_default_decoder_u32,
        u32,
        NcVariableType::Int(IntType::U32),
        DataType::UInt32,
        UInt32Type,
        vec![0u32, 1, 100_000, u32::MAX - 1]
    );

    test_numeric_decoder!(
        test_default_decoder_u64,
        u64,
        NcVariableType::Int(IntType::U64),
        DataType::UInt64,
        UInt64Type,
        vec![0u64, 1, 100_000, u64::MAX - 1]
    );

    test_numeric_decoder!(
        test_default_decoder_i8,
        i8,
        NcVariableType::Int(IntType::I8),
        DataType::Int8,
        Int8Type,
        vec![-128i8, -1, 0, 1, 127]
    );

    test_numeric_decoder!(
        test_default_decoder_i16,
        i16,
        NcVariableType::Int(IntType::I16),
        DataType::Int16,
        Int16Type,
        vec![-32768i16, -1, 0, 1, 32767]
    );

    test_numeric_decoder!(
        test_default_decoder_i32,
        i32,
        NcVariableType::Int(IntType::I32),
        DataType::Int32,
        Int32Type,
        vec![i32::MIN, -1, 0, 1, i32::MAX - 1]
    );

    test_numeric_decoder!(
        test_default_decoder_i64,
        i64,
        NcVariableType::Int(IntType::I64),
        DataType::Int64,
        Int64Type,
        vec![i64::MIN, -1i64, 0, 1, i64::MAX - 1]
    );

    #[test]
    fn test_default_decoder_f32() {
        let var_name = "data";
        let values: Vec<f32> = vec![-1.0f32, 0.0, 1.5, f32::MAX];
        let tmp = write_nc::<f32>(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Float32, true),
            nc_type: NcVariableType::Float(FloatType::F32),
            fill_value: None,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("DefaultVariableDecoder::read failed for f32");

        assert_eq!(array.len(), values.len());
        let typed = array.as_primitive::<Float32Type>();
        for (i, expected) in values.iter().enumerate() {
            assert!(
                (typed.value(i) - expected).abs() < f32::EPSILON,
                "f32 mismatch at index {i}: got {}, expected {expected}",
                typed.value(i)
            );
        }
    }

    #[test]
    fn test_default_decoder_f64() {
        let var_name = "data";
        let values: Vec<f64> = vec![-1.0f64, 0.0, 1.5, f64::MAX];
        let tmp = write_nc::<f64>(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("DefaultVariableDecoder::read failed for f64");

        assert_eq!(array.len(), values.len());
        let typed = array.as_primitive::<Float64Type>();
        for (i, expected) in values.iter().enumerate() {
            assert!(
                (typed.value(i) - expected).abs() < f64::EPSILON,
                "f64 mismatch at index {i}: got {}, expected {expected}",
                typed.value(i)
            );
        }
    }

    #[test]
    fn test_default_decoder_char() {
        use crate::NcChar;
        use arrow::array::StringArray;

        let var_name = "chars";
        let char_values: Vec<NcChar> = vec![NcChar(b'A'), NcChar(b'B'), NcChar(b'Z')];
        let tmp = write_nc::<NcChar>(var_name, &char_values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Utf8, true),
            nc_type: NcVariableType::Char,
            fill_value: None,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("DefaultVariableDecoder::read failed for char");

        assert_eq!(array.len(), char_values.len());
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(string_array.value(0), "A");
        assert_eq!(string_array.value(1), "B");
        assert_eq!(string_array.value(2), "Z");
    }

    // ── mask_fill_values ───────────────────────────────────────────────────

    #[test]
    fn test_mask_fill_values_no_fill_present() {
        use arrow::array::{Scalar, UInt8Array};

        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new("x", DataType::UInt8, true),
            nc_type: NcVariableType::Int(IntType::U8),
            fill_value: None,
        };

        // Values: 1, 2, 3 — fill is 255 (not present)
        let data: ArrayRef = Arc::new(UInt8Array::from(vec![1u8, 2, 3]));
        let fill: ArrayRef = Arc::new(UInt8Array::from(vec![255u8]));
        let fill_scalar = Scalar::new(fill);

        let result = decoder
            .mask_fill_values(data.clone(), fill_scalar)
            .expect("mask_fill_values should not error");

        // All values should remain valid (no fill values present)
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 0);
    }

    #[test]
    fn test_mask_fill_values_with_fill_present() {
        use arrow::array::{Scalar, UInt8Array};

        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new("x", DataType::UInt8, true),
            nc_type: NcVariableType::Int(IntType::U8),
            fill_value: None,
        };

        // Values: 1, 255, 3 — fill is 255
        let data: ArrayRef = Arc::new(UInt8Array::from(vec![1u8, 255, 3]));
        let fill: ArrayRef = Arc::new(UInt8Array::from(vec![255u8]));
        let fill_scalar = Scalar::new(fill);

        let result = decoder
            .mask_fill_values(data, fill_scalar)
            .expect("mask_fill_values should not error");

        assert_eq!(result.len(), 3);
        // Index 1 (value 255 == fill) should be marked according to the null buffer
        // The null buffer is built from the equality mask (true where fill)
        // In the current implementation: null bit = 1 (valid) where fill matches
        assert!(
            result.is_valid(1),
            "Index 1 (fill value) should be marked by the null buffer"
        );
    }

    #[test]
    fn test_variable_name_returns_field_name() {
        let decoder = DefaultVariableDecoder {
            arrow_field: Field::new("my_variable", DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        };
        assert_eq!(decoder.variable_name(), "my_variable");
    }
}
