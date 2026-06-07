//! Parsing of [CF-convention] time `units` strings.
//!
//! NetCDF and other CF-compliant datasets encode time as plain numbers (e.g.
//! `0, 1, 2`) plus a `units` string describing how to interpret them, such as
//! `"days since 1970-01-01"`. Decoding such a variable into absolute instants
//! requires two pieces of information:
//!
//! * a **reference epoch** — the `since <date>` part, the instant the count
//!   starts from; and
//! * a **unit** — the leading word (`seconds`, `days`, `weeks`, …) the numeric
//!   values are measured in.
//!
//! [`parse_cf_time`] extracts both and returns them as a
//! ([`hifitime::Epoch`], [`hifitime::Unit`]) pair. A caller decodes an
//! individual value `v` with `epoch + v * unit`.
//!
//! # Calendars
//!
//! CF datasets may declare a `calendar` attribute. This module supports the
//! two proleptic calendars that share the modern Gregorian/Julian month
//! structure:
//!
//! * **Gregorian** (the CF default, also accepted as `None`, and via the CF
//!   synonyms `standard` and `proleptic_gregorian`) — handled by
//!   `parse_cf_time_epoch_gregorian`, which parses the reference date with
//!   [`Epoch::from_str`].
//! * **Julian** — handled by `parse_cf_time_epoch_julian`, which resolves the
//!   reference date through the [`julian`] crate and converts it via its
//!   [Julian Day Number]. Because a Julian Day Number rolls over at *noon*, the
//!   integer JDN corresponds to 12:00 and midnight is half a day earlier; the
//!   function accounts for this offset.
//!
//! Calendar names are matched case-insensitively. Other calendars (`noleap`,
//! `360_day`, …) are rejected with an error.
//!
//! # Supported `units` syntax
//!
//! Both parsers expect `<unit> since <date>[ <time>]`:
//!
//! * `<unit>` is one of `seconds`, `milliseconds`, `microseconds`,
//!   `nanoseconds`, `minutes`, `hours`, `days`, or `weeks` (see
//!   `extract_units`).
//! * `<date>` is `[-]Y-M-D`; the optional `<time>` is `H:M:S`, separated by a
//!   space or `T`, with any trailing timezone marker (e.g. `Z`) ignored.
//!
//! Examples: `"seconds since 1970-01-01"`, `"days since 1950-01-01T00:00:00"`,
//! `"days since -4713-01-01T00:00:00Z"`.
//!
//! [CF-convention]: https://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate
//! [Julian Day Number]: https://en.wikipedia.org/wiki/Julian_day

use std::str::FromStr;

use hifitime::{Epoch, Unit};
use regex::Regex;

/// Parse a CF time `units` string into a reference epoch and unit.
///
/// This is the entry point of the module: it dispatches on the optional CF
/// `calendar` attribute and delegates to the matching calendar-specific parser.
/// See the [module documentation](self) for the accepted `units` syntax.
///
/// # Arguments
///
/// * `units` — a CF units string of the form `<unit> since <date>`, e.g.
///   `"days since 1970-01-01"`.
/// * `calendar` — the CF `calendar` attribute, matched case-insensitively.
///   `gregorian`, `standard`, `proleptic_gregorian`, or `None` select the
///   Gregorian calendar (the CF default); `julian` selects the Julian calendar.
///
/// # Returns
///
/// On success, the reference [`Epoch`] together with the [`Unit`] the numeric
/// time values are expressed in. A value `v` is then decoded as
/// `epoch + v * unit`.
///
/// # Errors
///
/// Returns `Err` if the `calendar` is unsupported, the reference date cannot be
/// parsed, or the time unit is missing or unrecognised.
pub fn parse_cf_time(units: &str, calendar: Option<&str>) -> Result<(Epoch, Unit), String> {
    // CF calendar names are case-insensitive.
    match calendar.map(str::to_ascii_lowercase).as_deref() {
        // The Gregorian calendar is the default when no calendar is specified.
        // `standard` and `proleptic_gregorian` are CF synonyms handled the same
        // way (hifitime parses dates on the proleptic Gregorian calendar).
        Some("gregorian") | Some("standard") | Some("proleptic_gregorian") | None => {
            parse_cf_time_epoch_gregorian(units)
        }
        Some("julian") => parse_cf_time_epoch_julian(units),
        // Report the original (un-lowercased) spelling in the error.
        Some(_) => Err(format!("Unsupported calendar: {}", calendar.unwrap())),
    }
}

/// Parse a CF time `units` string against the (proleptic) Gregorian calendar.
///
/// This is the CF default calendar. The reference date is read with
/// [`Epoch::from_str`], which accepts ISO-8601 / RFC-3339 forms such as
/// `1970-01-01` or `1970-01-01T00:00:00`, interpreted as UTC.
///
/// # Arguments
///
/// * `units` — a CF units string of the form `<unit> since <date>`, e.g.
///   `"seconds since 1970-01-01"` or `"days since 2000-01-01T00:00:00"`.
///
/// # Returns
///
/// On success, the reference [`Epoch`] together with the [`Unit`] the numeric
/// time values are expressed in.
///
/// # Errors
///
/// Returns `Err` if the reference date cannot be parsed, or if the leading
/// time unit is missing or unrecognised (see [`extract_units`]).
fn parse_cf_time_epoch_gregorian(units: &str) -> Result<(Epoch, Unit), String> {
    let re = Regex::new(r"since (?P<epoch>-?\d{1,4}-\d{1,2}-\d{1,2})").unwrap();
    let epoch_opt = re.captures(units).and_then(|caps| {
        let epoch_str = caps["epoch"].to_string();

        Epoch::from_str(&epoch_str).ok()
    });

    let units_opt = extract_units(units);
    match (epoch_opt, units_opt) {
        (Some(epoch), Some(unit)) => Ok((epoch, unit)),
        (None, _) => Err(format!("Failed to parse epoch from units string: {units}")),
        (_, None) => Err(format!(
            "Failed to extract time unit from units string: {units}"
        )),
    }
}

/// Parse a CF time `units` string against the (proleptic) Julian calendar.
///
/// The reference date is resolved through the [`julian`] crate and converted to
/// a [Julian Date] via its [Julian Day Number]. Note that the `julian` crate
/// uses *astronomical* year numbering (there is a year 0), so JDN 0 falls on
/// `-4712-01-01`, not `-4713-01-01`.
///
/// Because a Julian Day Number rolls over at **noon**, the integer JDN
/// corresponds to 12:00 on the calendar day; midnight (`00:00`) is therefore
/// half a day earlier (`JD = JDN - 0.5 + time_of_day`).
///
/// # Arguments
///
/// * `units` — a CF units string of the form `<unit> since <date>[ <time>]`,
///   e.g. `"days since 1950-01-01T00:00:00"`. See the
///   [module documentation](self) for the full accepted syntax.
///
/// # Returns
///
/// On success, the reference [`Epoch`] together with the [`Unit`] the numeric
/// time values are expressed in.
///
/// # Errors
///
/// Returns `Err` if the reference date or time cannot be parsed, the date is
/// not a valid Julian-calendar date, or the time unit is missing or
/// unrecognised (see [`extract_units`]).
///
/// [Julian Date]: https://en.wikipedia.org/wiki/Julian_day
/// [Julian Day Number]: https://en.wikipedia.org/wiki/Julian_day
fn parse_cf_time_epoch_julian(units: &str) -> Result<(Epoch, Unit), String> {
    // Match `... since <date>[ T]<time>` where `<date>` is `[-]Y-M-D` and the
    // optional `<time>` is `H:M:S`. Any trailing timezone marker (e.g. `Z`) is
    // ignored. Examples:
    //   `days since 1950-01-01`
    //   `days since 1950-01-01T00:00:00`
    //   `days since -4713-01-01T00:00:00Z`
    let re = Regex::new(
        r"since\s+(?P<year>-?\d{1,4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})(?:[ T](?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2}))?",
    )
    .unwrap();

    let caps = re
        .captures(units)
        .ok_or_else(|| format!("Failed to parse Julian epoch from units string: {units}"))?;

    // Parse the date into year, month, day
    let year: i32 = caps["year"]
        .parse()
        .map_err(|e| format!("Invalid year in units string: {units}. Error: {e}"))?;
    let month_num: u32 = caps["month"]
        .parse()
        .map_err(|e| format!("Invalid month in units string: {units}. Error: {e}"))?;
    let month = julian::Month::try_from(month_num)
        .map_err(|e| format!("Invalid month in units string: {units}. Error: {e:?}"))?;
    let day: u32 = caps["day"]
        .parse()
        .map_err(|e| format!("Invalid day in units string: {units}. Error: {e}"))?;

    let jul_cal = julian::Calendar::JULIAN;
    let epoch_date = jul_cal.at_ymd(year, month, day).map_err(|e| {
        format!("Failed to parse Julian epoch date from units string: {units}. Error: {e}")
    })?;

    // Parse the time into seconds for that day (defaults to midnight).
    let time_part = |name: &str| -> Result<i64, String> {
        caps.name(name)
            .map(|m| m.as_str().parse::<i64>())
            .transpose()
            .map_err(|e| format!("Invalid time in units string: {units}. Error: {e}"))
            .map(|v| v.unwrap_or(0))
    };
    let time_seconds: i64 =
        time_part("hour")? * 3600 + time_part("minute")? * 60 + time_part("second")?;

    // A Julian Day Number rolls over at *noon*, so the integer JDN corresponds
    // to 12:00 on the calendar day. Midnight (00:00) is therefore half a day
    // earlier: JD = JDN - 0.5 + time_of_day. This keeps `T12:00:00` mapping
    // exactly onto the integer JDN.
    let offset_juld =
        epoch_date.julian_day_number() as f64 - 0.5 + time_seconds as f64 / 86400.0;
    let base_epoch = Epoch::from_jde_utc(offset_juld);

    let units_opt = extract_units(units);

    match units_opt {
        Some(unit) => Ok((base_epoch, unit)),
        None => Err(format!(
            "Failed to extract time unit from units string: {units}"
        )),
    }
}

/// Extract the time unit from a CF units string.
///
/// Example accepted prefixes: `seconds since`, `days since`, `weeks since`.
fn extract_units(input: &str) -> Option<hifitime::Unit> {
    let re = Regex::new(r"^(?P<units>\w+) since").unwrap();
    re.captures(input)
        .and_then(|caps| match caps["units"].to_string().as_str() {
            "seconds" => Some(hifitime::Unit::Second),
            "milliseconds" => Some(hifitime::Unit::Millisecond),
            "microseconds" => Some(hifitime::Unit::Microsecond),
            "nanoseconds" => Some(hifitime::Unit::Nanosecond),
            "minutes" => Some(hifitime::Unit::Minute),
            "hours" => Some(hifitime::Unit::Hour),
            "days" => Some(hifitime::Unit::Day),
            "weeks" => Some(hifitime::Unit::Week),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tolerance (in days) for comparing Julian Dates derived through the
    /// f64 arithmetic in the conversion chain. ~1e-6 days is well below a
    /// second.
    const JD_EPS: f64 = 1e-6;

    /// Julian Day Number of the calendar date the proleptic Julian calendar
    /// assigns JDN 0 (noon). The `julian` crate uses astronomical year
    /// numbering, so this is `-4712-01-01`, not `-4713-01-01`.
    fn jdn(year: i32, month: u32, day: u32) -> i32 {
        julian::Calendar::JULIAN
            .at_ymd(year, julian::Month::try_from(month).unwrap(), day)
            .unwrap()
            .julian_day_number()
    }

    #[test]
    fn julian_noon_maps_to_integer_jdn() {
        // The JDN rolls over at noon, so 12:00:00 lands exactly on the integer
        // JDN. -4712-01-01 has JDN 0, so its noon is JD 0.0.
        assert_eq!(jdn(-4712, 1, 1), 0);
        let (epoch, unit) =
            parse_cf_time("days since -4712-01-01T12:00:00", Some("julian")).unwrap();
        assert_eq!(unit, Unit::Day);
        assert!((epoch.to_jde_utc_days() - 0.0).abs() < JD_EPS);
    }

    #[test]
    fn julian_midnight_is_half_a_day_before_jdn() {
        // Midnight (00:00) precedes the integer JDN by half a day.
        let (epoch, _) = parse_cf_time("days since -4712-01-01T00:00:00", Some("julian")).unwrap();
        assert!((epoch.to_jde_utc_days() - (-0.5)).abs() < JD_EPS);
    }

    #[test]
    fn julian_epoch_one_day_later() {
        // Consecutive midnights are exactly one day apart.
        let day1 = parse_cf_time("days since -4712-01-01T00:00:00", Some("julian"))
            .unwrap()
            .0
            .to_jde_utc_days();
        let day2 = parse_cf_time("days since -4712-01-02T00:00:00", Some("julian"))
            .unwrap()
            .0
            .to_jde_utc_days();
        assert!((day2 - day1 - 1.0).abs() < JD_EPS);
    }

    #[test]
    fn julian_epoch_with_time_of_day() {
        // 06:00 is a quarter day past midnight, i.e. JDN - 0.5 + 0.25 = -0.25.
        let (epoch, _) =
            parse_cf_time("seconds since -4712-01-01T06:00:00", Some("julian")).unwrap();
        assert!((epoch.to_jde_utc_days() - (-0.25)).abs() < JD_EPS);

        // 18:00 is three quarters past midnight: JDN - 0.5 + 0.75 = 0.25.
        let (epoch, _) =
            parse_cf_time("seconds since -4712-01-01T18:00:00", Some("julian")).unwrap();
        assert!((epoch.to_jde_utc_days() - 0.25).abs() < JD_EPS);
    }

    #[test]
    fn julian_epoch_real_world_date() {
        // Cross-check against the JDN the crate computes for the same date;
        // midnight is half a day before the integer JDN.
        let (epoch, unit) =
            parse_cf_time("days since 1950-01-01T00:00:00", Some("julian")).unwrap();
        assert_eq!(unit, Unit::Day);
        let expected = jdn(1950, 1, 1) as f64 - 0.5;
        assert!((epoch.to_jde_utc_days() - expected).abs() < JD_EPS);
    }

    #[test]
    fn julian_epoch_accepts_format_variants() {
        // Date-only, space separator, and trailing `Z` should all parse to the
        // same instant.
        let baseline = parse_cf_time("days since 1950-01-01T00:00:00", Some("julian"))
            .unwrap()
            .0
            .to_jde_utc_days();

        for units in [
            "days since 1950-01-01",
            "days since 1950-01-01 00:00:00",
            "days since 1950-01-01T00:00:00Z",
        ] {
            let jd = parse_cf_time(units, Some("julian"))
                .unwrap()
                .0
                .to_jde_utc_days();
            assert!((jd - baseline).abs() < JD_EPS, "mismatch for {units}");
        }
    }

    #[test]
    fn julian_unit_is_extracted() {
        for (units, expected) in [
            ("seconds since 1950-01-01", Unit::Second),
            ("milliseconds since 1950-01-01", Unit::Millisecond),
            ("days since 1950-01-01", Unit::Day),
            ("weeks since 1950-01-01", Unit::Week),
        ] {
            let (_, unit) = parse_cf_time(units, Some("julian")).unwrap();
            assert_eq!(unit, expected, "unit mismatch for {units}");
        }
    }

    #[test]
    fn unsupported_calendar_errors() {
        let err = parse_cf_time("days since 1950-01-01", Some("noleap")).unwrap_err();
        assert!(err.contains("Unsupported calendar"));
        // The original spelling is preserved in the message.
        assert!(err.contains("noleap"));
    }

    #[test]
    fn calendar_names_are_case_insensitive_with_synonyms() {
        // All of these select the Gregorian path and decode 1981-01-01 as the
        // proleptic-Gregorian reference date.
        let baseline = parse_cf_time("seconds since 1981-01-01", None)
            .unwrap()
            .0
            .to_unix_seconds();

        for cal in ["Gregorian", "GREGORIAN", "standard", "proleptic_gregorian"] {
            let (epoch, unit) = parse_cf_time("seconds since 1981-01-01", Some(cal)).unwrap();
            assert_eq!(unit, Unit::Second, "unit mismatch for calendar {cal}");
            assert_eq!(
                epoch.to_unix_seconds(),
                baseline,
                "epoch mismatch for calendar {cal}"
            );
        }

        // Julian is likewise case-insensitive.
        assert!(parse_cf_time("days since 1950-01-01", Some("JULIAN")).is_ok());
    }

    #[test]
    fn julian_missing_date_errors() {
        assert!(parse_cf_time("days since yesterday", Some("julian")).is_err());
    }

    #[test]
    fn julian_invalid_month_errors() {
        assert!(parse_cf_time("days since 1950-13-01", Some("julian")).is_err());
    }

    #[test]
    fn gregorian_is_the_default_calendar() {
        let (epoch, unit) = parse_cf_time("seconds since 1970-01-01", None).unwrap();
        assert_eq!(unit, Unit::Second);
        // Gregorian 1970-01-01 is the Unix epoch.
        assert_eq!(epoch.to_unix_seconds(), 0.0);
    }

    // ── gregorian ──────────────────────────────────────────────────────────

    #[test]
    fn gregorian_unix_epoch() {
        let (epoch, unit) = parse_cf_time_epoch_gregorian("seconds since 1970-01-01").unwrap();
        assert_eq!(unit, Unit::Second);
        assert_eq!(epoch.to_unix_seconds(), 0.0);
    }

    #[test]
    fn gregorian_known_date() {
        // 2000-01-01T00:00:00Z is 946_684_800 seconds after the Unix epoch.
        let (epoch, unit) = parse_cf_time_epoch_gregorian("days since 2000-01-01").unwrap();
        assert_eq!(unit, Unit::Day);
        assert_eq!(epoch.to_unix_seconds(), 946_684_800.0);
    }

    #[test]
    fn gregorian_accepts_explicit_time() {
        // A date-only reference and the same date at midnight resolve equally.
        let date_only = parse_cf_time_epoch_gregorian("seconds since 2000-01-01")
            .unwrap()
            .0;
        let with_time = parse_cf_time_epoch_gregorian("seconds since 2000-01-01T00:00:00")
            .unwrap()
            .0;
        assert_eq!(date_only.to_unix_seconds(), with_time.to_unix_seconds());
    }

    #[test]
    fn gregorian_extracts_each_unit() {
        for (units, expected) in [
            ("seconds since 1970-01-01", Unit::Second),
            ("milliseconds since 1970-01-01", Unit::Millisecond),
            ("microseconds since 1970-01-01", Unit::Microsecond),
            ("nanoseconds since 1970-01-01", Unit::Nanosecond),
            ("minutes since 1970-01-01", Unit::Minute),
            ("hours since 1970-01-01", Unit::Hour),
            ("days since 1970-01-01", Unit::Day),
            ("weeks since 1970-01-01", Unit::Week),
        ] {
            let (_, unit) = parse_cf_time_epoch_gregorian(units).unwrap();
            assert_eq!(unit, expected, "unit mismatch for {units}");
        }
    }

    #[test]
    fn gregorian_missing_date_errors() {
        let err = parse_cf_time_epoch_gregorian("days since yesterday").unwrap_err();
        assert!(err.contains("Failed to parse epoch"), "got: {err}");
    }

    #[test]
    fn gregorian_unrecognised_unit_errors() {
        // The date parses, but `fortnights` is not a known unit.
        let err = parse_cf_time_epoch_gregorian("fortnights since 1970-01-01").unwrap_err();
        assert!(err.contains("Failed to extract time unit"), "got: {err}");
    }
}
