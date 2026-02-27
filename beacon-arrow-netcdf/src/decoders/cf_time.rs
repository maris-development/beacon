use std::sync::Arc;

use arrow::{
    array::{ArrowPrimitiveType, AsArray, PrimitiveArray},
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimestampNanosecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use num_traits::AsPrimitive;

use crate::decoders::VariableDecoder;

#[derive(Debug)]
pub struct CFTimeVariableDecoder {
    pub arrow_field: arrow::datatypes::Field,
    pub inner_decoder: Box<dyn VariableDecoder>,
    pub epoch: hifitime::Epoch,
    pub unit: hifitime::Unit,
}

impl VariableDecoder for CFTimeVariableDecoder {
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<arrow::array::ArrayRef> {
        if self.arrow_field.data_type().is_primitive() {
            let array = self.inner_decoder.read(variable, extents)?;

            match array.data_type() {
                arrow::datatypes::DataType::Int8 => {
                    let array = array.as_primitive::<Int8Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::Int16 => {
                    let array = array.as_primitive::<Int16Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::Int32 => {
                    let array = array.as_primitive::<Int32Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::Int64 => {
                    let array = array.as_primitive::<Int64Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::UInt8 => {
                    let array = array.as_primitive::<UInt8Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::UInt16 => {
                    let array = array.as_primitive::<UInt16Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::UInt32 => {
                    let array = array.as_primitive::<UInt32Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::UInt64 => {
                    let array = array.as_primitive::<UInt64Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::Float32 => {
                    let array = array.as_primitive::<Float32Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::DataType::Float64 => {
                    let array = array.as_primitive::<Float64Type>();
                    let array = convert_to_timestamp_nanoseconds(array, self.epoch, self.unit);
                    Ok(Arc::new(array))
                }
                _ => {
                    anyhow::bail!(
                        "Unsupported data type for CF time variable decoder: {:?} on variable {}",
                        array.data_type(),
                        self.arrow_field.name()
                    );
                }
            }
        } else {
            anyhow::bail!(
                "Unsupported data type for CF time variable decoder: {:?} on variable {}",
                self.arrow_field.data_type(),
                self.arrow_field.name()
            );
        }
    }

    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }
}

fn convert_to_timestamp_nanoseconds<T: ArrowPrimitiveType>(
    array: &arrow::array::PrimitiveArray<T>,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
) -> arrow::array::PrimitiveArray<TimestampNanosecondType>
where
    T::Native: num_traits::cast::AsPrimitive<f64>,
{
    let array: PrimitiveArray<TimestampNanosecondType> = array.unary_opt(|v| {
        let time = epoch + (v.as_() * unit);
        Some(time.to_unix(hifitime::Unit::Nanosecond) as i64)
    });

    array
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Field, TimestampNanosecondType};
    use netcdf::types::{FloatType, IntType, NcVariableType};
    use tempfile::Builder;

    use super::CFTimeVariableDecoder;
    use crate::decoders::{DefaultVariableDecoder, VariableDecoder};

    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
    /// Maximum acceptable rounding error (nanoseconds) due to f64 arithmetic
    /// in the hifitime conversion chain. 1 µs is far tighter than any
    /// real-world time precision requirement.
    const MAX_NS_ERROR: i64 = 1_000;

    fn unix_epoch() -> hifitime::Epoch {
        hifitime::Epoch::from_unix_seconds(0.0)
    }

    // ── helpers ────────────────────────────────────────────────────────────

    fn write_nc_f64(var_name: &str, values: &[f64]) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", values.len()).unwrap();
            let mut var = nc.add_variable::<f64>(var_name, &["obs"]).unwrap();
            var.put_values(values, netcdf::Extents::All).unwrap();
        }
        tmp
    }

    fn write_nc_i32(var_name: &str, values: &[i32]) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", values.len()).unwrap();
            let mut var = nc.add_variable::<i32>(var_name, &["obs"]).unwrap();
            var.put_values(values, netcdf::Extents::All).unwrap();
        }
        tmp
    }

    // ── days-since-epoch (f64) ─────────────────────────────────────────────

    #[test]
    fn test_cf_time_f64_days_since_unix_epoch() {
        let var_name = "time";
        // 0 days → 0 ns, 1 day → 86400s in ns, 2 days → 172800s in ns
        let values = vec![0.0_f64, 1.0, 2.0];
        let tmp = write_nc_f64(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Box::new(DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        });

        let decoder = CFTimeVariableDecoder {
            arrow_field: Field::new(
                var_name,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            inner_decoder: inner,
            epoch: unix_epoch(),
            unit: hifitime::Unit::Day,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder failed");

        assert_eq!(array.len(), 3);
        let ts = array.as_primitive::<TimestampNanosecondType>();
        assert_eq!(ts.value(0), 0, "0 days should be Unix epoch (0 ns)");
        assert!(
            (ts.value(1) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
            ts.value(1)
        );
        assert!(
            (ts.value(2) - 2 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "2 days mismatch: got {}, expected ~{}",
            ts.value(2),
            2 * NANOS_PER_DAY
        );
    }

    #[test]
    fn test_cf_time_f64_seconds_since_unix_epoch() {
        let var_name = "time";
        let values = vec![0.0_f64, 1.0, 3600.0];
        let tmp = write_nc_f64(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Box::new(DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        });

        let decoder = CFTimeVariableDecoder {
            arrow_field: Field::new(
                var_name,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            inner_decoder: inner,
            epoch: unix_epoch(),
            unit: hifitime::Unit::Second,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (seconds) failed");

        let ts = array.as_primitive::<TimestampNanosecondType>();
        assert_eq!(ts.value(0), 0, "0 seconds should be 0 ns");
        assert!(
            (ts.value(1) - NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
            "1 second mismatch: got {}, expected ~{NANOS_PER_SECOND}",
            ts.value(1)
        );
        assert!(
            (ts.value(2) - 3600 * NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
            "3600 seconds mismatch: got {}, expected ~{}",
            ts.value(2),
            3600 * NANOS_PER_SECOND
        );
    }

    #[test]
    fn test_cf_time_f64_negative_offset() {
        let var_name = "time";
        // A day before the epoch
        let values = vec![-1.0_f64];
        let tmp = write_nc_f64(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Box::new(DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        });

        let decoder = CFTimeVariableDecoder {
            arrow_field: Field::new(
                var_name,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            inner_decoder: inner,
            epoch: unix_epoch(),
            unit: hifitime::Unit::Day,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (negative) failed");

        let ts = array.as_primitive::<TimestampNanosecondType>();
        assert!(
            (ts.value(0) - (-NANOS_PER_DAY)).abs() <= MAX_NS_ERROR,
            "-1 day mismatch: got {}, expected ~{}",
            ts.value(0),
            -NANOS_PER_DAY
        );
    }

    // ── integer time values (i32 days) ─────────────────────────────────────

    #[test]
    fn test_cf_time_i32_days_since_unix_epoch() {
        let var_name = "time";
        let values = vec![0_i32, 1, 365];
        let tmp = write_nc_i32(var_name, &values);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Box::new(DefaultVariableDecoder {
            arrow_field: Field::new(var_name, DataType::Int32, true),
            nc_type: NcVariableType::Int(IntType::I32),
            fill_value: None,
        });

        let decoder = CFTimeVariableDecoder {
            arrow_field: Field::new(
                var_name,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            inner_decoder: inner,
            epoch: unix_epoch(),
            unit: hifitime::Unit::Day,
        };

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (i32 days) failed");

        let ts = array.as_primitive::<TimestampNanosecondType>();
        assert_eq!(ts.value(0), 0);
        assert!(
            (ts.value(1) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "i32: 1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
            ts.value(1)
        );
        assert!(
            (ts.value(2) - 365 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "i32: 365 days mismatch: got {}, expected ~{}",
            ts.value(2),
            365 * NANOS_PER_DAY
        );
    }

    // ── variable_name ──────────────────────────────────────────────────────

    #[test]
    fn test_cf_time_variable_name() {
        let inner = Box::new(DefaultVariableDecoder {
            arrow_field: Field::new("time", DataType::Float64, true),
            nc_type: NcVariableType::Float(FloatType::F64),
            fill_value: None,
        });

        let decoder = CFTimeVariableDecoder {
            arrow_field: Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            inner_decoder: inner,
            epoch: unix_epoch(),
            unit: hifitime::Unit::Day,
        };

        assert_eq!(decoder.variable_name(), "time");
    }
}
