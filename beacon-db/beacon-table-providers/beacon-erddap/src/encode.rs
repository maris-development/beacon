//! Text forms ERDDAP accepts: percent-encoding, quoted strings, Java regexes, literals.

use chrono::{DateTime, SecondsFormat};
use datafusion::common::ScalarValue;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

/// Everything except the RFC 3986 unreserved marks ERDDAP leaves alone.
const QUERY: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'_')
    .remove(b'-')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// `raw` percent-encoded for use in an ERDDAP query string.
pub fn query_part(raw: &str) -> String {
    utf8_percent_encode(raw, QUERY).to_string()
}

/// A double-quoted ERDDAP string with `\`, `"`, newline and tab escaped.
pub fn erddap_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `s` as a Java regex that matches only itself.
pub fn java_regex_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.[]{}()<>*+-=!?^$|".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// A SQL LIKE pattern as a full-match Java regex. `None` when it uses a backslash escape.
pub fn like_to_regex(pattern: &str) -> Option<String> {
    // `(?s)` lets `.` match line terminators, as SQL wildcards do.
    let mut out = String::from("(?s)");
    for c in pattern.chars() {
        match c {
            '\\' => return None,
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            c => out.push_str(&java_regex_literal(&c.to_string())),
        }
    }
    Some(out)
}

/// A SQL literal in the form a tabledap constraint needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiteralValue {
    /// A finite number in decimal text.
    Number(String),
    /// ISO 8601 UTC with a `Z` suffix.
    Time(String),
    /// A string, not yet quoted.
    Text(String),
}

/// The constraint form of `value`, or `None` for null, non-finite or unsupported values.
pub fn literal_value(value: &ScalarValue) -> Option<LiteralValue> {
    use ScalarValue::*;
    let number = |v: String| Some(LiteralValue::Number(v));
    match value {
        Int8(Some(v)) => number(v.to_string()),
        Int16(Some(v)) => number(v.to_string()),
        Int32(Some(v)) => number(v.to_string()),
        Int64(Some(v)) => number(v.to_string()),
        UInt8(Some(v)) => number(v.to_string()),
        UInt16(Some(v)) => number(v.to_string()),
        UInt32(Some(v)) => number(v.to_string()),
        UInt64(Some(v)) => number(v.to_string()),
        Float32(Some(v)) if v.is_finite() => number(v.to_string()),
        Float64(Some(v)) if v.is_finite() => number(v.to_string()),
        Utf8(Some(s)) | LargeUtf8(Some(s)) | Utf8View(Some(s)) => {
            Some(LiteralValue::Text(s.clone()))
        }
        _ => {
            let nanos = timestamp_nanos(value)?;
            let time = DateTime::from_timestamp_nanos(nanos);
            Some(LiteralValue::Time(
                time.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            ))
        }
    }
}

/// `ms` epoch milliseconds as ISO 8601 UTC with a `Z` suffix; `.SSS` only when nonzero.
pub fn iso_millis(ms: i64) -> Option<String> {
    let time = DateTime::from_timestamp_millis(ms)?;
    Some(time.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// A timestamp literal as epoch nanoseconds, or `None` for other or null values.
pub fn timestamp_nanos(value: &ScalarValue) -> Option<i64> {
    use ScalarValue::*;
    match value {
        TimestampSecond(Some(v), _) => v.checked_mul(1_000_000_000),
        TimestampMillisecond(Some(v), _) => v.checked_mul(1_000_000),
        TimestampMicrosecond(Some(v), _) => v.checked_mul(1_000),
        TimestampNanosecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;

    #[test]
    fn query_part_encodes_operators_and_plus() {
        assert_eq!(
            query_part("time>=2020-01-01T00:00:00Z"),
            "time%3E%3D2020-01-01T00%3A00%3A00Z"
        );
        assert_eq!(query_part("a=\"x+y z\""), "a%3D%22x%2By%20z%22");
        assert_eq!(query_part("sst[0:1]"), "sst%5B0%3A1%5D");
    }

    #[test]
    fn erddap_string_escapes() {
        assert_eq!(erddap_string("a\"b\\c\nd\te"), r#""a\"b\\c\nd\te""#);
    }

    #[test]
    fn regex_literal_escapes_metacharacters() {
        assert_eq!(java_regex_literal("a.b|c(d)"), r"a\.b\|c\(d\)");
    }

    #[test]
    fn like_becomes_a_full_match_regex() {
        assert_eq!(like_to_regex("ab%c_").as_deref(), Some("(?s)ab.*c."));
        assert_eq!(like_to_regex("a.b%").as_deref(), Some(r"(?s)a\.b.*"));
        assert_eq!(like_to_regex(r"a\%"), None);
    }

    #[test]
    fn formats_millisecond_times() {
        assert_eq!(
            iso_millis(1_577_836_800_000).as_deref(),
            Some("2020-01-01T00:00:00Z")
        );
        assert_eq!(iso_millis(-1).as_deref(), Some("1969-12-31T23:59:59.999Z"));
    }

    #[test]
    fn formats_literals() {
        assert_eq!(
            literal_value(&ScalarValue::Int32(Some(5))),
            Some(LiteralValue::Number("5".into()))
        );
        assert_eq!(
            literal_value(&ScalarValue::Float64(Some(1.5))),
            Some(LiteralValue::Number("1.5".into()))
        );
        assert_eq!(literal_value(&ScalarValue::Float64(Some(f64::NAN))), None);
        assert_eq!(literal_value(&ScalarValue::Int32(None)), None);
        assert_eq!(
            literal_value(&ScalarValue::TimestampNanosecond(
                Some(1_577_836_800_000_000_000),
                None
            )),
            Some(LiteralValue::Time("2020-01-01T00:00:00Z".into()))
        );
        assert_eq!(
            literal_value(&ScalarValue::TimestampMillisecond(
                Some(1_577_836_800_500),
                None
            )),
            Some(LiteralValue::Time("2020-01-01T00:00:00.500Z".into()))
        );
        assert_eq!(
            literal_value(&ScalarValue::Utf8(Some("x".into()))),
            Some(LiteralValue::Text("x".into()))
        );
    }
}
