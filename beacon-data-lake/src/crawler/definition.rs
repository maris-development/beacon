//! The persisted, serializable description of a crawler.
//!
//! A [`CrawlerDefinition`] is the analogue of an AWS Glue crawler: it points at a
//! prefix in the datasets store and describes how discovered files should be turned
//! into external tables. It is plain `serde` data — unlike a table it is **not** a
//! `TableDefinition`, because a crawler is not itself a queryable table provider.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Option key stamped into every crawled table's `OPTIONS` so the crawler can tell
/// which tables it owns and must never clobber a hand-created table.
pub const CRAWLER_OWNER_OPTION: &str = "__crawler__";

/// Control keys consumed by the crawler itself; everything else in a `WITH (...)`
/// clause is forwarded into each discovered table's format `OPTIONS`.
const CONTROL_KEYS: &[&str] = &[
    "target_prefix",
    "format",
    "detect_partitions",
    "schedule",
    "event_driven",
    "table_naming",
];

/// How a discovered group of files is turned into a table name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TableNaming {
    /// Use the leaf component of the group's base prefix (`argo/floats` -> `floats`).
    #[default]
    LeafPrefix,
    /// Prefix the leaf with the crawler name (`<crawler>_<leaf>`).
    CrawlerPrefixed,
}

/// A persisted crawler description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrawlerDefinition {
    /// Unique crawler name.
    pub name: String,
    /// Datasets-store prefix to scan, e.g. `argo/`.
    pub target_prefix: String,
    /// Restrict discovery to these format identifiers (e.g. `["parquet", "nc"]`).
    /// `None` means every registered format.
    #[serde(default)]
    pub format_filter: Option<Vec<String>>,
    /// How discovered groups are named.
    #[serde(default)]
    pub table_naming: TableNaming,
    /// Detect Hive-style `key=value/` partitions (default `true`).
    #[serde(default = "default_true")]
    pub detect_partitions: bool,
    /// Periodic crawl interval, in seconds. `None` means no timer.
    #[serde(default)]
    pub schedule_secs: Option<u64>,
    /// Subscribe to datasets-store events under `target_prefix` for incremental crawls.
    #[serde(default)]
    pub event_driven: bool,
    /// Extra format options forwarded into every discovered table's `OPTIONS`.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

fn default_true() -> bool {
    true
}

impl CrawlerDefinition {
    /// The configured schedule as a [`Duration`], if any.
    pub fn schedule(&self) -> Option<Duration> {
        self.schedule_secs.map(Duration::from_secs)
    }

    /// Build a definition from the SQL surface: `CREATE CRAWLER <name> ON '<prefix>'
    /// WITH (k 'v', ...)`. `target_prefix` is the `ON` value (or `with["target_prefix"]`
    /// when `ON` is omitted). Recognised control keys are consumed; all other keys are
    /// forwarded into [`CrawlerDefinition::options`].
    pub fn from_sql(
        name: impl Into<String>,
        target_prefix: Option<String>,
        with: &HashMap<String, String>,
    ) -> Result<Self, String> {
        let name = name.into();

        let target_prefix = target_prefix
            .or_else(|| with.get("target_prefix").cloned())
            .ok_or_else(|| {
                format!("crawler '{name}' requires a target prefix (use `ON '<prefix>'`)")
            })?;

        let format_filter = with.get("format").map(|raw| {
            raw.split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        });

        let detect_partitions = match with.get("detect_partitions") {
            Some(v) => parse_bool(v)
                .ok_or_else(|| format!("invalid detect_partitions value '{v}'"))?,
            None => true,
        };

        let event_driven = match with.get("event_driven") {
            Some(v) => parse_bool(v).ok_or_else(|| format!("invalid event_driven value '{v}'"))?,
            None => false,
        };

        let schedule_secs = match with.get("schedule") {
            Some(v) => Some(parse_duration_secs(v)?),
            None => None,
        };

        let table_naming = match with.get("table_naming").map(|s| s.as_str()) {
            None | Some("leaf_prefix") => TableNaming::LeafPrefix,
            Some("crawler_prefixed") => TableNaming::CrawlerPrefixed,
            Some(other) => return Err(format!("unknown table_naming '{other}'")),
        };

        let options = with
            .iter()
            .filter(|(k, _)| !CONTROL_KEYS.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Self {
            name,
            target_prefix,
            format_filter,
            table_naming,
            detect_partitions,
            schedule_secs,
            event_driven,
            options,
        })
    }
}

/// Parse a boolean option value (`true`/`false`/`1`/`0`/`yes`/`no`).
fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Parse a duration like `30s`, `15m`, `2h`, `1d`, or a bare integer (seconds).
fn parse_duration_secs(value: &str) -> Result<u64, String> {
    let v = value.trim();
    if v.is_empty() {
        return Err("empty schedule".to_string());
    }
    let (num, mult) = match v.chars().last().unwrap() {
        's' => (&v[..v.len() - 1], 1),
        'm' => (&v[..v.len() - 1], 60),
        'h' => (&v[..v.len() - 1], 3600),
        'd' => (&v[..v.len() - 1], 86_400),
        c if c.is_ascii_digit() => (v, 1),
        other => return Err(format!("invalid schedule unit '{other}' in '{value}'")),
    };
    num.trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid schedule number in '{value}'"))
        .map(|n| n * mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration_secs("30s"), Ok(30));
        assert_eq!(parse_duration_secs("15m"), Ok(900));
        assert_eq!(parse_duration_secs("2h"), Ok(7200));
        assert_eq!(parse_duration_secs("1d"), Ok(86_400));
        assert_eq!(parse_duration_secs("45"), Ok(45));
        assert!(parse_duration_secs("15x").is_err());
        assert!(parse_duration_secs("").is_err());
    }

    #[test]
    fn from_sql_extracts_control_keys_and_forwards_rest() {
        let def = CrawlerDefinition::from_sql(
            "argo",
            Some("argo/".to_string()),
            &with(&[
                ("format", "parquet, nc"),
                ("detect_partitions", "false"),
                ("schedule", "15m"),
                ("event_driven", "true"),
                ("table_naming", "crawler_prefixed"),
                ("read_dimensions", "lat,lon"),
            ]),
        )
        .unwrap();

        assert_eq!(def.target_prefix, "argo/");
        assert_eq!(
            def.format_filter,
            Some(vec!["parquet".to_string(), "nc".to_string()])
        );
        assert!(!def.detect_partitions);
        assert!(def.event_driven);
        assert_eq!(def.schedule(), Some(Duration::from_secs(900)));
        assert_eq!(def.table_naming, TableNaming::CrawlerPrefixed);
        // Non-control keys are forwarded verbatim.
        assert_eq!(def.options.get("read_dimensions").map(String::as_str), Some("lat,lon"));
        assert!(!def.options.contains_key("format"));
        assert!(!def.options.contains_key("schedule"));
    }

    #[test]
    fn from_sql_defaults() {
        let def = CrawlerDefinition::from_sql("c", Some("p/".to_string()), &with(&[])).unwrap();
        assert!(def.detect_partitions);
        assert!(!def.event_driven);
        assert_eq!(def.schedule_secs, None);
        assert_eq!(def.format_filter, None);
        assert_eq!(def.table_naming, TableNaming::LeafPrefix);
    }

    #[test]
    fn from_sql_requires_prefix() {
        assert!(CrawlerDefinition::from_sql("c", None, &with(&[])).is_err());
        // ...unless supplied via the with-map.
        let def =
            CrawlerDefinition::from_sql("c", None, &with(&[("target_prefix", "p/")])).unwrap();
        assert_eq!(def.target_prefix, "p/");
    }

    #[test]
    fn round_trips_through_json() {
        let def = CrawlerDefinition::from_sql(
            "argo",
            Some("argo/".to_string()),
            &with(&[("schedule", "1h"), ("read_dimensions", "lat")]),
        )
        .unwrap();
        let json = serde_json::to_string(&def).unwrap();
        let back: CrawlerDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(def, back);
    }
}
