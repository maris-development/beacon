use std::{str::FromStr, sync::Arc};

use beacon_nd_arrow::array::compat_typings::{ArrowTypeConversion, TimestampNanosecond};
use hifitime::Epoch;
use netcdf::NcTypeDescriptor;
use num_traits::AsPrimitive;
use regex::Regex;

use crate::{decoders::VariableDecoder, NcTimestampNanosecond};

#[derive(Debug)]
pub struct CFTimeVariableDecoder<T>
where
    T: ArrowTypeConversion + NcTypeDescriptor + AsPrimitive<f64>,
{
    pub arrow_field: arrow::datatypes::FieldRef,
    pub inner_decoder: Arc<dyn VariableDecoder<T>>,
    pub epoch: hifitime::Epoch,
    pub unit: hifitime::Unit,
}

impl<T> CFTimeVariableDecoder<T>
where
    T: ArrowTypeConversion + NcTypeDescriptor + AsPrimitive<f64>,
{
    pub fn new(
        arrow_field: arrow::datatypes::FieldRef,
        inner_decoder: Arc<dyn VariableDecoder<T>>,
        epoch: hifitime::Epoch,
        unit: hifitime::Unit,
    ) -> Self {
        Self {
            arrow_field,
            inner_decoder,
            epoch,
            unit,
        }
    }
}

impl<T> VariableDecoder<NcTimestampNanosecond> for CFTimeVariableDecoder<T>
where
    T: ArrowTypeConversion + NcTypeDescriptor + AsPrimitive<f64>,
{
    fn arrow_field(&self) -> &arrow::datatypes::Field {
        &self.arrow_field
    }

    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<ndarray::ArrayD<NcTimestampNanosecond>> {
        let array = self.inner_decoder.read(variable, extents)?;
        let ts_array = convert_to_timestamp_nanoseconds(array.view(), self.epoch, self.unit);
        Ok(ts_array)
    }

    fn variable_name(&self) -> &str {
        self.arrow_field.name()
    }
}

fn convert_to_timestamp_nanoseconds<T>(
    array: ndarray::ArrayViewD<T>,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
) -> ndarray::ArrayD<NcTimestampNanosecond>
where
    T: num_traits::cast::AsPrimitive<f64>,
{
    let array: ndarray::ArrayD<NcTimestampNanosecond> = array.mapv(|v| {
        let time = epoch + (v.as_() * unit);
        NcTimestampNanosecond(TimestampNanosecond(
            time.to_unix(hifitime::Unit::Nanosecond).as_(),
        ))
    });

    array
}

pub(crate) fn parse_time_units(units_str: &str) -> Option<(hifitime::Epoch, hifitime::Unit)> {
    let unit = extract_units(units_str)?;
    let epoch = extract_epoch(units_str)?;
    Some((epoch, unit))
}

pub(crate) fn extract_units(input: &str) -> Option<hifitime::Unit> {
    let re = Regex::new(r"^(?P<units>\w+) since").unwrap();
    re.captures(input)
        .and_then(|caps| match caps["units"].to_string().as_str() {
            "seconds" => Some(hifitime::Unit::Second),
            "milliseconds" => Some(hifitime::Unit::Millisecond),
            "microseconds" => Some(hifitime::Unit::Microsecond),
            "nanoseconds" => Some(hifitime::Unit::Nanosecond),
            "days" => Some(hifitime::Unit::Day),
            "weeks" => Some(hifitime::Unit::Week),
            _ => None,
        })
}

/// Extracts the epoch date from a string like "days since -4713-11-24"
pub(crate) fn extract_epoch(input: &str) -> Option<Epoch> {
    let re = Regex::new(r"since (?P<epoch>-?\d{1,4}-\d{1,2}-\d{1,2})").unwrap();
    let result = re.captures(input).and_then(|caps| {
        let epoch_str = caps["epoch"].to_string();
        let mut epoch = Epoch::from_str(&epoch_str).ok();

        if epoch.is_none() && epoch_str == "-4713-01-01" {
            epoch = Some(Epoch::from_jde_utc(0.0));
        }

        epoch
    });

    result
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use arrow::array::AsArray;
//     use arrow::datatypes::{DataType, Field, TimestampNanosecondType};
//     use netcdf::types::{FloatType, IntType, NcVariableType};
//     use tempfile::Builder;

//     use super::CFTimeVariableDecoder;
//     use crate::decoders::{DefaultVariableDecoder, VariableDecoder};

//     const NANOS_PER_SECOND: i64 = 1_000_000_000;
//     const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
//     /// Maximum acceptable rounding error (nanoseconds) due to f64 arithmetic
//     /// in the hifitime conversion chain. 1 µs is far tighter than any
//     /// real-world time precision requirement.
//     const MAX_NS_ERROR: i64 = 1_000;

//     fn unix_epoch() -> hifitime::Epoch {
//         hifitime::Epoch::from_unix_seconds(0.0)
//     }

//     // ── helpers ────────────────────────────────────────────────────────────

//     fn write_nc_f64(var_name: &str, values: &[f64]) -> tempfile::NamedTempFile {
//         let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
//         {
//             let mut nc = netcdf::create(tmp.path()).unwrap();
//             nc.add_dimension("obs", values.len()).unwrap();
//             let mut var = nc.add_variable::<f64>(var_name, &["obs"]).unwrap();
//             var.put_values(values, netcdf::Extents::All).unwrap();
//         }
//         tmp
//     }

//     fn write_nc_i32(var_name: &str, values: &[i32]) -> tempfile::NamedTempFile {
//         let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
//         {
//             let mut nc = netcdf::create(tmp.path()).unwrap();
//             nc.add_dimension("obs", values.len()).unwrap();
//             let mut var = nc.add_variable::<i32>(var_name, &["obs"]).unwrap();
//             var.put_values(values, netcdf::Extents::All).unwrap();
//         }
//         tmp
//     }

//     // ── days-since-epoch (f64) ─────────────────────────────────────────────

//     #[test]
//     fn test_cf_time_f64_days_since_unix_epoch() {
//         let var_name = "time";
//         // 0 days → 0 ns, 1 day → 86400s in ns, 2 days → 172800s in ns
//         let values = vec![0.0_f64, 1.0, 2.0];
//         let tmp = write_nc_f64(var_name, &values);

//         let file = netcdf::open(tmp.path()).unwrap();
//         let variable = file.variable(var_name).unwrap();

//         let inner = Arc::new(DefaultVariableDecoder {
//             arrow_field: Arc::new(Field::new(var_name, DataType::Float64, true)),
//             nc_type: NcVariableType::Float(FloatType::F64),
//             fill_value: None,
//         });

//         let decoder = CFTimeVariableDecoder {
//             arrow_field: Arc::new(Field::new(
//                 var_name,
//                 DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
//                 true,
//             )),
//             inner_decoder: inner,
//             epoch: unix_epoch(),
//             unit: hifitime::Unit::Day,
//         };

//         let array = decoder
//             .read(&variable, netcdf::Extents::All)
//             .expect("CF time decoder failed");

//         assert_eq!(array.len(), 3);
//         let ts = array.as_primitive::<TimestampNanosecondType>();
//         assert_eq!(ts.value(0), 0, "0 days should be Unix epoch (0 ns)");
//         assert!(
//             (ts.value(1) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
//             "1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
//             ts.value(1)
//         );
//         assert!(
//             (ts.value(2) - 2 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
//             "2 days mismatch: got {}, expected ~{}",
//             ts.value(2),
//             2 * NANOS_PER_DAY
//         );
//     }

//     #[test]
//     fn test_cf_time_f64_seconds_since_unix_epoch() {
//         let var_name = "time";
//         let values = vec![0.0_f64, 1.0, 3600.0];
//         let tmp = write_nc_f64(var_name, &values);

//         let file = netcdf::open(tmp.path()).unwrap();
//         let variable = file.variable(var_name).unwrap();

//         let inner = Arc::new(DefaultVariableDecoder {
//             arrow_field: Arc::new(Field::new(var_name, DataType::Float64, true)),
//             nc_type: NcVariableType::Float(FloatType::F64),
//             fill_value: None,
//         });

//         let decoder = CFTimeVariableDecoder {
//             arrow_field: Arc::new(Field::new(
//                 var_name,
//                 DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
//                 true,
//             )),
//             inner_decoder: inner,
//             epoch: unix_epoch(),
//             unit: hifitime::Unit::Second,
//         };

//         let array = decoder
//             .read(&variable, netcdf::Extents::All)
//             .expect("CF time decoder (seconds) failed");

//         let ts = array.as_primitive::<TimestampNanosecondType>();
//         assert_eq!(ts.value(0), 0, "0 seconds should be 0 ns");
//         assert!(
//             (ts.value(1) - NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
//             "1 second mismatch: got {}, expected ~{NANOS_PER_SECOND}",
//             ts.value(1)
//         );
//         assert!(
//             (ts.value(2) - 3600 * NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
//             "3600 seconds mismatch: got {}, expected ~{}",
//             ts.value(2),
//             3600 * NANOS_PER_SECOND
//         );
//     }

//     #[test]
//     fn test_cf_time_f64_negative_offset() {
//         let var_name = "time";
//         // A day before the epoch
//         let values = vec![-1.0_f64];
//         let tmp = write_nc_f64(var_name, &values);

//         let file = netcdf::open(tmp.path()).unwrap();
//         let variable = file.variable(var_name).unwrap();

//         let inner = Arc::new(DefaultVariableDecoder {
//             arrow_field: Arc::new(Field::new(var_name, DataType::Float64, true)),
//             nc_type: NcVariableType::Float(FloatType::F64),
//             fill_value: None,
//         });

//         let decoder = CFTimeVariableDecoder {
//             arrow_field: Arc::new(Field::new(
//                 var_name,
//                 DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
//                 true,
//             )),
//             inner_decoder: inner,
//             epoch: unix_epoch(),
//             unit: hifitime::Unit::Day,
//         };

//         let array = decoder
//             .read(&variable, netcdf::Extents::All)
//             .expect("CF time decoder (negative) failed");

//         let ts = array.as_primitive::<TimestampNanosecondType>();
//         assert!(
//             (ts.value(0) - (-NANOS_PER_DAY)).abs() <= MAX_NS_ERROR,
//             "-1 day mismatch: got {}, expected ~{}",
//             ts.value(0),
//             -NANOS_PER_DAY
//         );
//     }

//     // ── integer time values (i32 days) ─────────────────────────────────────

//     #[test]
//     fn test_cf_time_i32_days_since_unix_epoch() {
//         let var_name = "time";
//         let values = vec![0_i32, 1, 365];
//         let tmp = write_nc_i32(var_name, &values);

//         let file = netcdf::open(tmp.path()).unwrap();
//         let variable = file.variable(var_name).unwrap();

//         let inner = Arc::new(DefaultVariableDecoder {
//             arrow_field: Arc::new(Field::new(var_name, DataType::Int32, true)),
//             nc_type: NcVariableType::Int(IntType::I32),
//             fill_value: None,
//         });

//         let decoder = CFTimeVariableDecoder {
//             arrow_field: Arc::new(Field::new(
//                 var_name,
//                 DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
//                 true,
//             )),
//             inner_decoder: inner,
//             epoch: unix_epoch(),
//             unit: hifitime::Unit::Day,
//         };

//         let array = decoder
//             .read(&variable, netcdf::Extents::All)
//             .expect("CF time decoder (i32 days) failed");

//         let ts = array.as_primitive::<TimestampNanosecondType>();
//         assert_eq!(ts.value(0), 0);
//         assert!(
//             (ts.value(1) - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
//             "i32: 1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
//             ts.value(1)
//         );
//         assert!(
//             (ts.value(2) - 365 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
//             "i32: 365 days mismatch: got {}, expected ~{}",
//             ts.value(2),
//             365 * NANOS_PER_DAY
//         );
//     }

//     // ── variable_name ──────────────────────────────────────────────────────

//     #[test]
//     fn test_cf_time_variable_name() {
//         let inner = Arc::new(DefaultVariableDecoder {
//             arrow_field: Arc::new(Field::new("time", DataType::Float64, true)),
//             nc_type: NcVariableType::Float(FloatType::F64),
//             fill_value: None,
//         });

//         let decoder = CFTimeVariableDecoder {
//             arrow_field: Arc::new(Field::new(
//                 "time",
//                 DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
//                 true,
//             )),
//             inner_decoder: inner,
//             epoch: unix_epoch(),
//             unit: hifitime::Unit::Day,
//         };

//         assert_eq!(decoder.variable_name(), "time");
//     }
// }
