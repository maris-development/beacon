//! CF-time decoding utilities.
//!
//! This module converts numeric NetCDF time variables with units like
//! `"days since 1970-01-01"` into nanosecond timestamps.

use std::sync::Arc;

use beacon_nd_array::datatypes::TimestampNanosecond;
use netcdf::NcTypeDescriptor;
use num_traits::AsPrimitive;

use crate::decoders::VariableDecoder;

/// Decoder that wraps a numeric decoder and converts values to timestamps.
#[derive(Debug)]
pub struct CFTimeVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64>,
{
    variable_name: String,
    inner_decoder: Arc<dyn VariableDecoder<T>>,
    epoch: hifitime::Epoch,
    unit: hifitime::Unit,
    fill_value: Option<TimestampNanosecond>,
}

impl<T> CFTimeVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64>,
{
    /// Create a CF-time decoder.
    ///
    /// `inner_decoder` provides the raw numeric values; `epoch` and `unit`
    /// define the CF conversion rule. `raw_fill_value` is the fill value in the
    /// *numeric* units of the variable; it goes through the same CF arithmetic
    /// so a raw fill cell maps exactly onto the decoded fill, which the engine
    /// then nulls.
    ///
    /// A variable that declares no fill still gets one: [`NO_TIME`]. Without it
    /// a NaN cell would decode to a timestamp nothing nulls, and reach a query
    /// as a date in the year -292277.
    pub fn new(
        variable_name: String,
        inner_decoder: Arc<dyn VariableDecoder<T>>,
        epoch: hifitime::Epoch,
        unit: hifitime::Unit,
        raw_fill_value: Option<f64>,
    ) -> Self {
        Self {
            variable_name,
            inner_decoder,
            epoch,
            unit,
            fill_value: raw_fill_value
                .map(|f| cf_offset_to_timestamp(f, epoch, unit))
                .or(Some(NO_TIME)),
        }
    }
}

impl<T> VariableDecoder<TimestampNanosecond> for CFTimeVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64> + Copy + std::fmt::Debug + Send + Sync + 'static,
{
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<ndarray::ArrayD<TimestampNanosecond>> {
        let array = self.inner_decoder.read(variable, extents)?;
        let ts_array = convert_to_timestamp_nanoseconds(
            array.view(),
            self.epoch,
            self.unit,
            self.fill_value,
        );
        Ok(ts_array)
    }

    fn fill_value(&self) -> Option<TimestampNanosecond> {
        self.fill_value
    }

    fn variable_name(&self) -> &str {
        &self.variable_name
    }
}

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
/// Shared with the [`oxcdf`](crate::oxcdf_reader) path, so both readers apply
/// the same arithmetic.
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
    use std::{str::FromStr, sync::Arc};

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use hifitime::{Duration, Epoch};
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

// ── non-finite offsets ─────────────────────────────────────────────────

    /// A `_FillValue` of NaN must not bring the process down.
    ///
    /// This decodes at open, not at read, so it reaches any request that only
    /// wants a schema. `hifitime` panics when it is asked to build a `Duration`
    /// from a non-finite number, so a NaN fill took out the worker thread rather
    /// than returning an error.
    #[test]
    fn a_nan_fill_value_does_not_panic() {
        let inner = Arc::new(DefaultVariableDecoder::<f64>::new("time".to_string(), None));

        let decoder = CFTimeVariableDecoder::new(
            "time".to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            Some(f64::NAN),
        );

        assert_eq!(
            decoder.fill_value(),
            Some(super::NO_TIME),
            "a fill that is not a time decodes to the absent timestamp"
        );
    }

    /// Infinity is not a time either.
    #[test]
    fn an_infinite_fill_value_does_not_panic() {
        for fill in [f64::INFINITY, f64::NEG_INFINITY] {
            let inner = Arc::new(DefaultVariableDecoder::<f64>::new("time".to_string(), None));
            let decoder = CFTimeVariableDecoder::new(
                "time".to_string(),
                inner,
                unix_epoch(),
                hifitime::Unit::Day,
                Some(fill),
            );
            assert_eq!(decoder.fill_value(), Some(super::NO_TIME), "fill={fill}");
        }
    }

    /// A NaN *cell* nulls, rather than reaching a query as a date.
    ///
    /// A file that marks its gaps with NaN and declares no `_FillValue` is the
    /// common case this covers: the cells decode to the absent timestamp, and
    /// the decoder reports that as its fill so the engine nulls them.
    #[test]
    fn nan_cells_decode_to_the_absent_timestamp() {
        let var_name = "time";
        let tmp = write_nc_f64(var_name, &[0.0, f64::NAN, 2.0]);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            var_name.to_string(),
            None,
        ));
        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder failed");
        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();

        assert_eq!(ts[0], 0);
        assert_eq!(ts[1], super::NO_TIME.0, "a NaN cell has no timestamp");
        assert!(
            (ts[2] - NANOS_PER_DAY * 2).abs() <= MAX_NS_ERROR,
            "the cells either side of it decode normally: {}",
            ts[2]
        );

        assert_eq!(
            decoder.fill_value(),
            Some(super::NO_TIME),
            "so the engine nulls it"
        );
    }

    /// A NaN cell takes the file's own fill value when it declares one, so it
    /// nulls alongside the cells the file marked itself.
    #[test]
    fn a_nan_cell_takes_the_declared_fill() {
        let var_name = "time";
        let tmp = write_nc_f64(var_name, &[0.0, f64::NAN, 99999.0]);

        let file = netcdf::open(tmp.path()).unwrap();
        let variable = file.variable(var_name).unwrap();

        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            var_name.to_string(),
            None,
        ));
        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            Some(99999.0),
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder failed");
        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();
        let fill = decoder.fill_value().expect("a declared fill").0;

        assert_eq!(ts[1], fill, "the NaN cell nulls with the declared fill");
        assert_eq!(ts[2], fill, "and so does the fill cell itself");
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

        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            var_name.to_string(),
            None,
        ));

        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder failed");

        assert_eq!(array.len(), 3);
        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();
        assert_eq!(ts[0], 0, "0 days should be Unix epoch (0 ns)");
        assert!(
            (ts[1] - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
            ts[1]
        );
        assert!(
            (ts[2] - 2 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "2 days mismatch: got {}, expected ~{}",
            ts[2],
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

        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            var_name.to_string(),
            None,
        ));

        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Second,
            None,
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (seconds) failed");

        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();
        assert_eq!(ts[0], 0, "0 seconds should be 0 ns");
        assert!(
            (ts[1] - NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
            "1 second mismatch: got {}, expected ~{NANOS_PER_SECOND}",
            ts[1]
        );
        assert!(
            (ts[2] - 3600 * NANOS_PER_SECOND).abs() <= MAX_NS_ERROR,
            "3600 seconds mismatch: got {}, expected ~{}",
            ts[2],
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

        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            var_name.to_string(),
            None,
        ));

        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (negative) failed");

        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();
        assert!(
            (ts[0] - (-NANOS_PER_DAY)).abs() <= MAX_NS_ERROR,
            "-1 day mismatch: got {}, expected ~{}",
            ts[0],
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

        let inner = Arc::new(DefaultVariableDecoder::<i32>::new(
            var_name.to_string(),
            None,
        ));

        let decoder = CFTimeVariableDecoder::new(
            var_name.to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        let array = decoder
            .read(&variable, netcdf::Extents::All)
            .expect("CF time decoder (i32 days) failed");

        let ts: Vec<i64> = array.iter().map(|x| x.0).collect();
        assert_eq!(ts[0], 0);
        assert!(
            (ts[1] - NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "i32: 1 day mismatch: got {}, expected ~{NANOS_PER_DAY}",
            ts[1]
        );
        assert!(
            (ts[2] - 365 * NANOS_PER_DAY).abs() <= MAX_NS_ERROR,
            "i32: 365 days mismatch: got {}, expected ~{}",
            ts[2],
            365 * NANOS_PER_DAY
        );
    }

    // ── variable_name ──────────────────────────────────────────────────────

    #[test]
    fn test_cf_time_variable_name() {
        let inner = Arc::new(DefaultVariableDecoder::<f64>::new("time".to_string(), None));

        let decoder = CFTimeVariableDecoder::new(
            "time".to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        assert_eq!(decoder.variable_name(), "time");
    }

    // ── _FillValue ─────────────────────────────────────────────────────────

    /// The decoded fill value equals the raw fill run through the same CF
    /// arithmetic, so a fill cell nulls out.
    #[test]
    fn decoded_fill_matches_cf_time_arithmetic() {
        let inner = Arc::new(DefaultVariableDecoder::<i16>::new(
            "time".to_string(),
            Some(-32768),
        ));

        let decoder = CFTimeVariableDecoder::new(
            "time".to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            Some(-32768.0),
        );

        let fill = decoder
            .fill_value()
            .expect("the decoder holds a fill value");
        let expected = -32768 * NANOS_PER_DAY;
        assert!(
            (fill.0 - expected).abs() <= MAX_NS_ERROR,
            "fill mismatch: got {}, expected ~{expected}",
            fill.0
        );
    }

    /// The fill value follows the unit of the variable, not only its number.
    #[test]
    fn decoded_fill_follows_the_cf_unit() {
        let inner = Arc::new(DefaultVariableDecoder::<f64>::new(
            "time".to_string(),
            Some(-999.0),
        ));

        let decoder = CFTimeVariableDecoder::new(
            "time".to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Second,
            Some(-999.0),
        );

        let fill = decoder
            .fill_value()
            .expect("the decoder holds a fill value");
        let expected = -999 * NANOS_PER_SECOND;
        assert!(
            (fill.0 - expected).abs() <= MAX_NS_ERROR,
            "fill mismatch: got {}, expected ~{expected}",
            fill.0
        );
    }

    /// A time variable that declares no `_FillValue` still reports one.
    ///
    /// This used to be `None`, on the principle that a variable which masks
    /// nothing should mask nothing. A CF time variable is the exception: its
    /// values can be NaN, NaN is not an instant, and without a fill to map it to
    /// such a cell reaches a query as a date in the year -292277 rather than as
    /// null. So it reports [`NO_TIME`], which no real date collides with.
    ///
    /// Nothing is masked that a file did not already leave empty: only a cell
    /// that is not a number decodes to this.
    #[test]
    fn a_time_variable_without_a_declared_fill_still_masks_nan() {
        let inner = Arc::new(DefaultVariableDecoder::<f64>::new("time".to_string(), None));

        let decoder = CFTimeVariableDecoder::new(
            "time".to_string(),
            inner,
            unix_epoch(),
            hifitime::Unit::Day,
            None,
        );

        assert_eq!(decoder.fill_value(), Some(super::NO_TIME));
    }

    #[test]
    fn test_time_extract() {
        let jul_cal = julian::Calendar::JULIAN;
        let start = jul_cal.at_ymd(-4713, julian::Month::January, 1).unwrap();

        println!("Start of Julian calendar: {start}");

        let num = start.julian_day_number();
        println!("Julian day number for {start}: {num}");

        // let epoch = Epoch::from
        // println!("Epoch: {epoch}");
    }
}
