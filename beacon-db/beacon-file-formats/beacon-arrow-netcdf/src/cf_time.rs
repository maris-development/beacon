//! CF-time conversion.
//!
//! This module converts numeric NetCDF time offsets with units like
//! `"days since 1970-01-01"` into nanosecond timestamps.

use beacon_nd_array::datatypes::TimestampNanosecond;

/// The timestamp of a CF offset that is not a time.
///
/// A CF time variable is numbers plus a rule for turning them into instants.
/// NaN and the infinities have no instant to turn into, and `hifitime` refuses
/// them by panicking, so they need an answer of their own before they reach it.
///
/// The value is unreachable as a real date -- `i64::MIN` nanoseconds before 1970
/// is the year -292277 -- so nothing a file could legitimately hold collides
/// with it. A decoder reports it as its fill value when the file declares none,
/// which is what makes such a cell arrive at a query as null rather than as that
/// date.
pub(crate) const NO_TIME: TimestampNanosecond =
    TimestampNanosecond(beacon_common::cf_time::NO_TIME_NANOS);

/// Convert one numeric CF time offset into a nanosecond timestamp.
///
/// Every CF time value takes this path, the data cells and the `_FillValue`
/// alike. One function keeps the two results comparable, so the engine nulls a
/// fill cell.
pub(crate) fn cf_offset_to_timestamp<T>(
    value: T,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
) -> TimestampNanosecond
where
    T: num_traits::cast::AsPrimitive<f64>,
{
    // One value at a time, so the scale is built and thrown away. That suits the
    // callers: a `_FillValue` is decoded once per variable. An array goes
    // through `convert_to_timestamp_nanoseconds`, which builds it once for all
    // of them.
    scale_of(epoch, unit)
        .nanos(value.as_())
        .map_or(NO_TIME, TimestampNanosecond)
}

/// The linear map a CF variable's epoch and unit describe.
///
/// Resolving it per value cost 16.8% of a CORA query, all of it in
/// `hifitime::Epoch::to_time_scale`, so it is resolved per array instead. See
/// [`beacon_common::cf_time::CfScale`].
fn scale_of(epoch: hifitime::Epoch, unit: hifitime::Unit) -> beacon_common::cf_time::CfScale {
    beacon_common::cf_time::CfScale::new(epoch, unit)
}

/// Convert numeric CF time offsets into nanosecond timestamps.
///
/// The [`oxcdf`](crate::oxcdf_reader) reader applies this arithmetic to every
/// time array.
///
/// `fill` is the variable's decoded fill value. A cell that is not a time takes
/// it, so a NaN in a file that marks its gaps some other way still nulls
/// alongside the gaps the file marked itself. With no fill it takes
/// [`NO_TIME`], which every decoder reports as its fill.
pub(crate) fn convert_to_timestamp_nanoseconds<T>(
    array: ndarray::ArrayViewD<T>,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
    fill: Option<TimestampNanosecond>,
) -> ndarray::ArrayD<TimestampNanosecond>
where
    T: num_traits::cast::AsPrimitive<f64>,
{
    let absent = fill.unwrap_or(NO_TIME);
    // Once for the array, not once per cell. The epoch and the unit are the
    // variable's, so every cell resolved the same two constants and paid a
    // calendar conversion to do it.
    let scale = scale_of(epoch, unit);
    array.mapv(|v| scale.nanos(v.as_()).map_or(absent, TimestampNanosecond))
}

/// Parse a CF `units` (and optional `calendar`) attribute into a reference
/// epoch and unit.
///
/// Thin wrapper over [`beacon_common::cf_time::parse_cf_time`]; returns `None`
/// when the attributes cannot be interpreted as a CF time reference (so a
/// non-time variable is simply left as plain numbers).
pub(crate) fn parse_time_units(
    units_str: &str,
    calendar: Option<&str>,
) -> Option<(hifitime::Epoch, hifitime::Unit)> {
    beacon_common::cf_time::parse_cf_time(units_str, calendar).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

    fn days_since_unix() -> (hifitime::Epoch, hifitime::Unit) {
        parse_time_units("days since 1970-01-01", None).expect("a CF time reference")
    }

    #[test]
    fn an_offset_in_days_becomes_nanoseconds() {
        let (epoch, unit) = days_since_unix();
        assert_eq!(
            cf_offset_to_timestamp(2.0_f64, epoch, unit),
            TimestampNanosecond(2 * NANOS_PER_DAY)
        );
    }

    #[test]
    fn an_offset_that_is_not_a_time_becomes_no_time() {
        let (epoch, unit) = days_since_unix();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(cf_offset_to_timestamp(value, epoch, unit), NO_TIME);
        }
    }

    #[test]
    fn a_cell_that_is_not_a_time_takes_the_fill() {
        let (epoch, unit) = days_since_unix();
        let fill = TimestampNanosecond(-1);
        let array = ndarray::arr1(&[1.0_f64, f64::NAN]).into_dyn();
        let converted = convert_to_timestamp_nanoseconds(array.view(), epoch, unit, Some(fill));
        assert_eq!(
            converted.into_raw_vec_and_offset().0,
            vec![TimestampNanosecond(NANOS_PER_DAY), fill]
        );
    }

    #[test]
    fn units_that_are_not_a_time_reference_parse_to_none() {
        assert!(parse_time_units("degrees_north", None).is_none());
    }
}
