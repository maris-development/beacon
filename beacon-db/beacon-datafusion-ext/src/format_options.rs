//! Lookup of the `OPTIONS` of a `CREATE EXTERNAL TABLE`.
//!
//! One option reaches a format factory under two spellings. DataFusion's SQL
//! planner renames a key without a `.` to `format.<key>`, and it lowercases the
//! key. The crawler and a persisted `table.json` pass the key unchanged.
//!
//! A factory that reads one spelling therefore drops the option of the other
//! path. [`format_option`] reads both.

use std::collections::HashMap;

use datafusion::common::exec_datafusion_err;
use datafusion::error::Result;

/// Read one `CREATE EXTERNAL TABLE ... OPTIONS (...)` key.
///
/// Reads the bare key and the `format.`-prefixed key that DataFusion's SQL
/// planner produces for it. The bare key wins.
pub fn format_option<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a String> {
    options
        .get(key)
        .or_else(|| options.get(&format!("format.{key}")))
}

/// Parse the boolean value of one `OPTIONS` key.
///
/// Accepts `true`/`false`, `1`/`0`, `yes`/`no` and `on`/`off`, in any case.
pub fn parse_bool_option(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(exec_datafusion_err!(
            "invalid boolean for option '{key}': '{other}'"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_boolean_option_reads_the_usual_spellings() {
        for value in ["true", "1", "yes", "ON", " True "] {
            assert!(parse_bool_option("k", value).unwrap(), "{value}");
        }
        for value in ["false", "0", "no", "Off"] {
            assert!(!parse_bool_option("k", value).unwrap(), "{value}");
        }
        let error = parse_bool_option("skip", "maybe").unwrap_err().to_string();
        assert!(error.contains("skip") && error.contains("maybe"), "{error}");
    }

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn reads_a_bare_key() {
        let options = options(&[("read_dimensions", "lat,lon")]);
        assert_eq!(
            format_option(&options, "read_dimensions").map(String::as_str),
            Some("lat,lon")
        );
    }

    #[test]
    fn reads_the_key_the_sql_planner_produces() {
        let options = options(&[("format.read_dimensions", "lat,lon")]);
        assert_eq!(
            format_option(&options, "read_dimensions").map(String::as_str),
            Some("lat,lon")
        );
    }

    #[test]
    fn the_bare_key_wins_over_the_prefixed_one() {
        let options = options(&[
            ("read_dimensions", "lat"),
            ("format.read_dimensions", "lon"),
        ]);
        assert_eq!(
            format_option(&options, "read_dimensions").map(String::as_str),
            Some("lat")
        );
    }

    #[test]
    fn an_absent_key_reads_as_none() {
        assert!(format_option(&options(&[]), "read_dimensions").is_none());
    }
}
