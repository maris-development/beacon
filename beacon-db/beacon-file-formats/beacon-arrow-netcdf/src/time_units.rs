//! CF time units, such as `seconds since 1970-01-01`, for the flat NetCDF writer.

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    },
};
use chrono::{DateTime, NaiveDate, NaiveDateTime};

/// Nanoseconds in one second.
const SECOND: i128 = 1_000_000_000;

/// The `units` of a time variable: a unit and a reference time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TimeUnits {
    /// The text as the user gave it. The writer uses it as the `units` attribute.
    text: String,
    /// The length of one unit in nanoseconds.
    unit_ns: i128,
    /// The reference time in nanoseconds since 1970-01-01T00:00:00Z.
    reference_ns: i128,
}

impl TimeUnits {
    /// The `units` attribute.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The value of `ns` (nanoseconds since 1970) in these units, when it is a whole number
    /// that fits in an `i64`.
    fn to_integer(&self, ns: i128) -> Option<i64> {
        let offset = ns - self.reference_ns;
        if offset % self.unit_ns != 0 {
            return None;
        }
        i64::try_from(offset / self.unit_ns).ok()
    }

    /// The value of `ns` (nanoseconds since 1970) in these units.
    fn to_float(&self, ns: i128) -> f64 {
        let offset = ns - self.reference_ns;
        // Split the offset, so that a large offset keeps the precision of its fraction.
        (offset / self.unit_ns) as f64 + (offset % self.unit_ns) as f64 / self.unit_ns as f64
    }

    /// Whether every value of the timestamp `array` is a whole number in these units.
    pub fn fits_integer(&self, array: &ArrayRef) -> bool {
        nanoseconds(array)
            .flatten()
            .all(|ns| self.to_integer(ns).is_some())
    }

    /// The values of the timestamp `array` as whole numbers in these units.
    pub fn encode_integer(&self, array: &ArrayRef, fill: i64) -> Vec<i64> {
        nanoseconds(array)
            .map(|ns| ns.and_then(|ns| self.to_integer(ns)).unwrap_or(fill))
            .collect()
    }

    /// The values of the timestamp `array` as numbers in these units.
    pub fn encode_float(&self, array: &ArrayRef, fill: f64) -> Vec<f64> {
        nanoseconds(array)
            .map(|ns| ns.map_or(fill, |ns| self.to_float(ns)))
            .collect()
    }
}

/// The values of a timestamp array in nanoseconds since 1970. A null is `None`.
fn nanoseconds(array: &ArrayRef) -> Box<dyn Iterator<Item = Option<i128>> + '_> {
    fn scaled<'a, I: Iterator<Item = Option<i64>> + 'a>(
        values: I,
        ns: i128,
    ) -> Box<dyn Iterator<Item = Option<i128>> + 'a> {
        Box::new(values.map(move |v| v.map(|v| v as i128 * ns)))
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            scaled(array.as_primitive::<TimestampSecondType>().iter(), SECOND)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => scaled(
            array.as_primitive::<TimestampMillisecondType>().iter(),
            1_000_000,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scaled(
            array.as_primitive::<TimestampMicrosecondType>().iter(),
            1_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            scaled(array.as_primitive::<TimestampNanosecondType>().iter(), 1)
        }
        _ => Box::new((0..array.len()).map(|_| None)),
    }
}

impl std::str::FromStr for TimeUnits {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let error = || {
            format!(
                "invalid time_units '{text}': expected '<unit> since <date>', for example \
                 'seconds since 1970-01-01'. The unit is days, hours, minutes, seconds, \
                 milliseconds, microseconds or nanoseconds"
            )
        };
        let (unit, reference) = text.trim().split_once(" since ").ok_or_else(error)?;
        let unit_ns = match unit.trim().to_ascii_lowercase().as_str() {
            "days" | "day" => 86_400 * SECOND,
            "hours" | "hour" => 3_600 * SECOND,
            "minutes" | "minute" => 60 * SECOND,
            "seconds" | "second" => SECOND,
            "milliseconds" | "millisecond" => 1_000_000,
            "microseconds" | "microsecond" => 1_000,
            "nanoseconds" | "nanosecond" => 1,
            _ => return Err(error()),
        };
        let reference_ns = parse_reference(reference.trim()).ok_or_else(error)?;
        Ok(Self {
            text: text.trim().to_string(),
            unit_ns,
            reference_ns,
        })
    }
}

/// The reference time of a CF `units` text in nanoseconds since 1970, in UTC.
fn parse_reference(text: &str) -> Option<i128> {
    if let Ok(time) = DateTime::parse_from_rfc3339(text) {
        return time.timestamp_nanos_opt().map(i128::from);
    }
    let text = text.strip_suffix('Z').unwrap_or(text).trim();
    let naive = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ]
    .iter()
    .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
    .or_else(|| {
        NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .and_then(|date| date.and_hms_opt(0, 0, 0))
    })?;
    naive.and_utc().timestamp_nanos_opt().map(i128::from)
}

impl TryFrom<String> for TimeUnits {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<TimeUnits> for String {
    fn from(units: TimeUnits) -> Self {
        units.text
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{TimestampNanosecondArray, TimestampSecondArray};

    use super::*;

    fn units(text: &str) -> TimeUnits {
        text.parse().unwrap()
    }

    #[test]
    fn units_parse_the_unit_and_the_reference() {
        let days = units("days since 1950-01-01");
        assert_eq!(days.unit_ns, 86_400 * SECOND);
        assert_eq!(days.reference_ns, -631_152_000 * SECOND);
        assert_eq!(days.as_str(), "days since 1950-01-01");
        for text in [
            "seconds since 1970-01-01T00:00:00Z",
            "seconds since 1970-01-01 00:00:00",
            "Seconds since 1970-01-01T00:00",
        ] {
            assert_eq!(units(text).reference_ns, 0, "{text}");
        }
    }

    #[test]
    fn invalid_units_are_errors() {
        for text in [
            "seconds",
            "fortnights since 1970-01-01",
            "seconds since yesterday",
            "",
        ] {
            let error = text.parse::<TimeUnits>().unwrap_err();
            assert!(error.contains("time_units"), "{error}");
        }
    }

    #[test]
    fn whole_values_encode_as_integers() {
        let array: ArrayRef = Arc::new(TimestampSecondArray::from(vec![
            Some(0),
            None,
            Some(86_400 * 2),
        ]));
        let days = units("days since 1970-01-01");
        assert!(days.fits_integer(&array));
        assert_eq!(days.encode_integer(&array, i64::MAX), vec![0, i64::MAX, 2]);
        let since_1950 = units("days since 1950-01-01");
        assert_eq!(since_1950.encode_integer(&array, -1)[0], 7305);
    }

    #[test]
    fn a_fraction_needs_a_float() {
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_500_000_000]));
        let seconds = units("seconds since 1970-01-01");
        assert!(!seconds.fits_integer(&array));
        assert_eq!(seconds.encode_float(&array, f64::NAN), vec![1.5]);
    }
}
