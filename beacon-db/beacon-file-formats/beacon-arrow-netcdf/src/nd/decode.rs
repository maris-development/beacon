//! The CF decode of a NetCDF variable: fill, valid range, packing and time.

use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Float64Type};
use beacon_common::cf_time::{parse_cf_time, CfScale};
use nd_arrow_array::compute::cf::{decode_cf, is_cf_key, ADD_OFFSET, SCALE_FACTOR};
use nd_arrow_array::compute::elementwise::cast;
use nd_arrow_array::compute::time::{decode_cf_time, timestamp_type, CfTimeUnits, CALENDAR, UNITS};
use nd_arrow_array::{Attributes, NdArrowArray};

/// The decode of one variable, chosen once from its metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Decode {
    /// No CF attribute: the stored values.
    None,
    /// Fill, missing value, valid range, packing: `decode_cf`.
    Cf,
    /// A CF time variable: `decode_cf_time`.
    Time,
    /// A Julian-calendar time variable: the `beacon_common::cf_time` path, in microseconds.
    JulianTime {
        epoch: hifitime::Epoch,
        unit: hifitime::Unit,
    },
}

impl Decode {
    /// Spec 8.4: the choice from the stored type and the attributes. No I/O.
    pub(crate) fn choose(stored: &DataType, attributes: &Attributes) -> Decode {
        // The read handles the fill of a string variable.
        if !(stored.is_integer() || stored.is_floating()) {
            return Decode::None;
        }
        if let Some(time) = Self::choose_time(attributes) {
            return time;
        }
        if attributes.iter().any(|(key, _)| is_cf_key(key)) {
            Decode::Cf
        } else {
            Decode::None
        }
    }

    /// `Time` or `JulianTime` for a CF time variable with a supported calendar.
    fn choose_time(attributes: &Attributes) -> Option<Decode> {
        let units = attributes.get_str(UNITS)?;
        CfTimeUnits::parse(units).ok()?;
        let calendar = attributes.get_str(CALENDAR);
        match calendar.map(str::to_ascii_lowercase).as_deref() {
            None | Some("standard" | "gregorian" | "proleptic_gregorian") => Some(Decode::Time),
            Some("julian") => match parse_cf_time(units, calendar) {
                Ok((epoch, unit)) => Some(Decode::JulianTime { epoch, unit }),
                Err(error) => {
                    tracing::debug!(units, %error, "no time decode for a Julian time variable");
                    None
                }
            },
            Some(other) => {
                tracing::debug!(units, calendar = other, "no time decode for a CF calendar");
                None
            }
        }
    }

    /// The type that `apply` returns for `stored`.
    pub(crate) fn data_type(&self, stored: &DataType, attributes: &Attributes) -> DataType {
        match self {
            Decode::None => stored.clone(),
            Decode::Time | Decode::JulianTime { .. } => timestamp_type(),
            Decode::Cf => {
                let packing: Vec<DataType> = [SCALE_FACTOR, ADD_OFFSET]
                    .into_iter()
                    .filter_map(|key| attributes.get(key))
                    .map(|value| item_type(value.data_type()))
                    .collect();
                if packing.is_empty() {
                    return stored.clone();
                }
                packing
                    .into_iter()
                    .find(|t| matches!(t, DataType::Float32 | DataType::Float64))
                    .unwrap_or(DataType::Float64)
            }
        }
    }

    /// Decode one read. The input has the stored type and the variable attributes.
    pub(crate) fn apply(&self, raw: NdArrowArray) -> beacon_nd_dataset::Result<NdArrowArray> {
        match self {
            Decode::None => Ok(raw),
            Decode::Cf => Ok(decode_cf(&raw)?.0),
            Decode::Time => {
                let units_text = raw.attributes().get_str(UNITS).unwrap_or_default();
                let units = CfTimeUnits::parse(units_text)?;
                let (numbers, kept) = time_numbers(&raw, |v| fits_micros(units, v))?;
                let mut time_attributes = Attributes::new();
                for key in [UNITS, CALENDAR] {
                    if let Some(value) = raw.attributes().get(key) {
                        time_attributes.try_insert_array(key, value.clone())?;
                    }
                }
                let numbers = NdArrowArray::try_new(Arc::new(numbers), raw.dims().clone())?
                    .with_attributes(time_attributes);
                let (stamps, _) = decode_cf_time(&numbers)?;
                Ok(stamps.with_attributes(kept))
            }
            Decode::JulianTime { epoch, unit } => {
                let scale = CfScale::new(*epoch, *unit);
                let epoch_nanos = epoch.to_unix_duration().total_nanoseconds() as f64;
                let unit_nanos = (1.0 * *unit).total_nanoseconds() as f64;
                let (numbers, kept) =
                    time_numbers(&raw, |v| (epoch_nanos + v * unit_nanos).abs() < NANOS_LIMIT)?;
                let stamps: TimestampMicrosecondArray = numbers
                    .iter()
                    .map(|v| v.and_then(|v| scale.nanos(v)).map(|nanos| nanos / 1000))
                    .collect();
                Ok(NdArrowArray::try_new(Arc::new(stamps), raw.dims().clone())?
                    .with_attributes(kept))
            }
        }
    }
}

/// A bound below `i64::MAX` nanoseconds, so `CfScale::nanos` never saturates.
const NANOS_LIMIT: f64 = 9.2e18;

/// The value type of an attribute: the item type of a list, else its own type.
fn item_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            field.data_type().clone()
        }
        other => other.clone(),
    }
}

/// True when `reference + round(v * step)` is an `i64` of microseconds.
fn fits_micros(units: CfTimeUnits, v: f64) -> bool {
    let offset = (v * units.step_micros as f64).round();
    // `i64::MAX as f64` is 2^63, one past the largest `i64`.
    offset.is_finite()
        && offset >= i64::MIN as f64
        && offset < i64::MAX as f64
        && units.reference_micros.checked_add(offset as i64).is_some()
}

/// The `decode_cf` values as `Float64`, null where `fits` is false, and the attributes
/// that the decode keeps, without `units` and `calendar`.
fn time_numbers(
    raw: &NdArrowArray,
    fits: impl Fn(f64) -> bool,
) -> beacon_nd_dataset::Result<(Float64Array, Attributes)> {
    let (decoded, _) = decode_cf(raw)?;
    let numbers = cast(&decoded, &DataType::Float64)?;
    let masked: Float64Array = numbers
        .values()
        .as_primitive::<Float64Type>()
        .iter()
        .map(|v| v.filter(|v| fits(*v)))
        .collect();
    let mut kept = Attributes::new();
    for (key, value) in decoded.attributes().iter() {
        if key != UNITS && key != CALENDAR {
            kept.try_insert_array(key, value.clone())?;
        }
    }
    Ok((masked, kept))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        StringArray,
    };
    use arrow::datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    };
    use chrono::NaiveDate;
    use nd_arrow_array::compute::time::timestamp_type;
    use nd_arrow_array::Dimensions;

    use super::*;

    fn array(values: ArrayRef, attributes: Attributes) -> NdArrowArray {
        let len = values.len();
        NdArrowArray::try_new(values, Dimensions::try_new([("x", len)]).unwrap())
            .unwrap()
            .with_attributes(attributes)
    }

    /// Choose the decode and apply it, as the variable does.
    fn decode(raw: &NdArrowArray) -> (Decode, NdArrowArray) {
        let decode = Decode::choose(raw.data_type(), raw.attributes());
        let out = decode.apply(raw.clone()).unwrap();
        (decode, out)
    }

    fn micros(year: i32, month: u32, day: u32, hour: u32) -> i64 {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    fn stamps(out: &NdArrowArray) -> Vec<Option<i64>> {
        assert_eq!(out.data_type(), &timestamp_type());
        out.values()
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect()
    }

    fn no_cf_key() -> NdArrowArray {
        array(
            Arc::new(Int32Array::from(vec![1, 2])),
            Attributes::new().with("long_name", "count"),
        )
    }

    fn fill_and_missing() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![1, -9, -8, 4])),
            Attributes::new()
                .with("_FillValue", -9i16)
                .with("missing_value", vec![-8i16]),
        )
    }

    fn fill_of_another_type() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![1, -9])),
            Attributes::new().with("_FillValue", -9.0f64),
        )
    }

    fn valid_range() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![-1, 5, 11])),
            Attributes::new().with("valid_range", vec![0i16, 10]),
        )
    }

    fn float32_scale() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![10])),
            Attributes::new().with("scale_factor", 0.5f32),
        )
    }

    fn integer_offset() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![10])),
            Attributes::new().with("add_offset", 1i32),
        )
    }

    fn time() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![1.0])),
            Attributes::new().with("units", "days since 1970-01-01"),
        )
    }

    fn packed_time() -> NdArrowArray {
        array(
            Arc::new(Int16Array::from(vec![10])),
            Attributes::new()
                .with("scale_factor", 0.5f32)
                .with("units", "days since 2000-01-01"),
        )
    }

    fn time_of_day() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![0.0])),
            Attributes::new().with("units", "hours since 2000-01-01 12:00:00"),
        )
    }

    fn not_finite_times() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                1.0,
            ])),
            Attributes::new().with("units", "days since 1970-01-01"),
        )
    }

    fn julian_time() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![0.0])),
            Attributes::new()
                .with("units", "days since 2000-01-01")
                .with("calendar", "julian"),
        )
    }

    fn noleap_time() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![1.0])),
            Attributes::new()
                .with("units", "days since 2000-01-01")
                .with("calendar", "noleap"),
        )
    }

    fn nanosecond_time() -> NdArrowArray {
        array(
            Arc::new(Int64Array::from(vec![5i64])),
            Attributes::new().with("units", "nanoseconds since 1970-01-01"),
        )
    }

    fn huge_time() -> NdArrowArray {
        array(
            Arc::new(Float64Array::from(vec![1e300, -1e18, 1.0])),
            Attributes::new().with("units", "days since 1970-01-01"),
        )
    }

    fn string_variable() -> NdArrowArray {
        array(
            Arc::new(StringArray::from(vec!["a", "-"])),
            Attributes::new()
                .with("_FillValue", "-")
                .with("scale_factor", 0.5f32)
                .with("units", "days since 1970-01-01"),
        )
    }

    #[test]
    fn a_variable_with_no_cf_key_is_not_decoded() {
        let raw = no_cf_key();
        let (decode, out) = decode(&raw);
        assert_eq!(decode, Decode::None);
        assert_eq!(out.data_type(), &DataType::Int32);
        let values: Vec<Option<i32>> = out.values().as_primitive::<Int32Type>().iter().collect();
        assert_eq!(values, vec![Some(1), Some(2)]);
    }

    #[test]
    fn fill_and_missing_values_become_null() {
        let (decode, out) = decode(&fill_and_missing());
        assert_eq!(decode, Decode::Cf);
        let values: Vec<Option<i16>> = out.values().as_primitive::<Int16Type>().iter().collect();
        assert_eq!(values, vec![Some(1), None, None, Some(4)]);
    }

    #[test]
    fn a_fill_of_another_type_is_applied() {
        let (_, out) = decode(&fill_of_another_type());
        let values: Vec<Option<i16>> = out.values().as_primitive::<Int16Type>().iter().collect();
        assert_eq!(values, vec![Some(1), None]);
    }

    #[test]
    fn values_outside_the_valid_range_become_null() {
        let (_, out) = decode(&valid_range());
        let values: Vec<Option<i16>> = out.values().as_primitive::<Int16Type>().iter().collect();
        assert_eq!(values, vec![None, Some(5), None]);
    }

    #[test]
    fn a_float32_scale_gives_float32() {
        let raw = float32_scale();
        let (decode, out) = decode(&raw);
        assert_eq!(
            decode.data_type(raw.data_type(), raw.attributes()),
            DataType::Float32
        );
        let values: Vec<Option<f32>> = out.values().as_primitive::<Float32Type>().iter().collect();
        assert_eq!(values, vec![Some(5.0)]);
    }

    #[test]
    fn an_integer_offset_gives_float64() {
        let (_, out) = decode(&integer_offset());
        let values: Vec<Option<f64>> = out.values().as_primitive::<Float64Type>().iter().collect();
        assert_eq!(values, vec![Some(11.0)]);
    }

    #[test]
    fn a_time_variable_gives_microseconds() {
        let (decode, out) = decode(&time());
        assert_eq!(decode, Decode::Time);
        assert_eq!(stamps(&out), vec![Some(86_400_000_000)]);
    }

    #[test]
    fn a_packed_time_variable_gives_a_timestamp() {
        let (decode, out) = decode(&packed_time());
        assert_eq!(decode, Decode::Time);
        assert_eq!(stamps(&out), vec![Some(micros(2000, 1, 6, 0))]);
    }

    #[test]
    fn a_reference_time_of_day_is_applied() {
        let (_, out) = decode(&time_of_day());
        assert_eq!(stamps(&out), vec![Some(micros(2000, 1, 1, 12))]);
    }

    #[test]
    fn nan_and_infinite_times_become_null() {
        let (_, out) = decode(&not_finite_times());
        assert_eq!(stamps(&out), vec![None, None, None, Some(86_400_000_000)]);
    }

    #[test]
    fn times_outside_the_microsecond_range_become_null() {
        let (_, out) = decode(&huge_time());
        assert_eq!(stamps(&out), vec![None, None, Some(86_400_000_000)]);
    }

    #[test]
    fn a_julian_time_decodes() {
        let (epoch, unit) =
            beacon_common::cf_time::parse_cf_time("days since 2000-01-01", Some("julian")).unwrap();
        let expected = beacon_common::cf_time::CfScale::new(epoch, unit)
            .nanos(0.0)
            .unwrap()
            / 1000;
        // Julian 2000-01-01 is Gregorian 2000-01-14.
        assert_eq!(expected, micros(2000, 1, 14, 0));

        let (decode, out) = decode(&julian_time());
        assert!(matches!(decode, Decode::JulianTime { .. }));
        assert_eq!(stamps(&out), vec![Some(expected)]);
    }

    #[test]
    fn a_noleap_time_stays_a_number() {
        let (decode, out) = decode(&noleap_time());
        assert_eq!(decode, Decode::None);
        assert_eq!(out.data_type(), &DataType::Float64);
    }

    #[test]
    fn nanosecond_units_stay_numbers() {
        let (decode, out) = decode(&nanosecond_time());
        assert_eq!(decode, Decode::None);
        let values: Vec<Option<i64>> = out.values().as_primitive::<Int64Type>().iter().collect();
        assert_eq!(values, vec![Some(5)]);
    }

    #[test]
    fn an_empty_scale_factor_is_an_nd_error() {
        let raw = array(
            Arc::new(Int16Array::from(vec![10])),
            Attributes::new().with("scale_factor", Vec::<f32>::new()),
        );
        let decode = Decode::choose(raw.data_type(), raw.attributes());
        let error = decode.apply(raw).unwrap_err();
        assert!(
            matches!(error, beacon_nd_dataset::Error::Nd(_)),
            "{error:?}"
        );
    }

    #[test]
    fn a_string_variable_is_not_decoded() {
        let raw = string_variable();
        let (decode, out) = decode(&raw);
        assert_eq!(decode, Decode::None);
        assert_eq!(
            decode.data_type(raw.data_type(), raw.attributes()),
            DataType::Utf8
        );
        let values: Vec<Option<&str>> = out.values().as_string::<i32>().iter().collect();
        assert_eq!(values, vec![Some("a"), Some("-")]);
    }

    #[test]
    fn data_type_matches_apply_for_each_case() {
        for raw in [
            no_cf_key(),
            fill_and_missing(),
            fill_of_another_type(),
            valid_range(),
            float32_scale(),
            integer_offset(),
            time(),
            packed_time(),
            time_of_day(),
            not_finite_times(),
            huge_time(),
            julian_time(),
            noleap_time(),
            nanosecond_time(),
            string_variable(),
            array(
                Arc::new(Float32Array::from(vec![1.0f32])),
                Attributes::new().with("add_offset", vec![1.0f64, 2.0]),
            ),
        ] {
            let (decode, out) = decode(&raw);
            assert_eq!(
                out.data_type(),
                &decode.data_type(raw.data_type(), raw.attributes()),
                "{decode:?} on {raw:?}"
            );
        }
    }
}
