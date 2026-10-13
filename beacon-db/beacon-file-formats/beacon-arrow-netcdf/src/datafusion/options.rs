use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
};

use beacon_datafusion_ext::format_options::{format_option, parse_bool_option};
use datafusion::common::exec_datafusion_err;

use crate::{attributes::CombineAttrs, time_units::TimeUnits};

/// Configuration options for the NetCDF file format in DataFusion queries.
///
/// Controls both reading (schema inference, dimension handling) and writing
/// (flat vs. gridded output mode, flat output options).
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfOptions {
    /// Column names whose unique values should be tracked during execution
    /// for downstream consumers (e.g. sink nodes).
    #[serde(default)]
    pub unique_value_columns: Vec<String>,
    /// Number of rows per batch when replaying buffered data.
    #[serde(default = "default_replay_batch_size")]
    pub replay_batch_size: usize,
    /// Columns to treat as NetCDF dimensions when **reading**.
    ///
    /// When `Some`, the reader unpacks the listed dimensions into extra
    /// columns in the resulting Arrow schema.
    #[serde(default)]
    pub read_dimensions: Option<Vec<String>>,
    /// Skip a file that does not fit `read_dimensions`, with a warning,
    /// instead of failing the query.
    #[serde(default)]
    pub skip_unbroadcastable: bool,
    /// Columns to use as NetCDF dimensions when **writing**.
    ///
    /// * `None` / empty → flat output via [`NetCDFSink`](super::sink::NetCDFSink)
    ///   with a single `obs` dimension.
    /// * `Some(["lat", "lon", …])` → gridded output via
    ///   [`NetCDFNdSink`](super::sink::NetCDFNdSink) where the listed columns
    ///   become named dimensions and remaining columns become data variables.
    #[serde(default)]
    pub write_dimensions: Option<Vec<String>>,
    /// Options of the flat writer.
    #[serde(default)]
    pub writer: NetcdfWriterOptions,
}

fn default_replay_batch_size() -> usize {
    128 * 1024
}

/// Options of the flat NetCDF writer. The query API shows them as `NetcdfOutputOptions`.
#[derive(
    Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(deny_unknown_fields)]
#[schema(as = NetcdfOutputOptions)]
pub struct NetcdfWriterOptions {
    /// How the writer combines `variable.attribute` columns. When not set, they stay variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combine_attrs: Option<CombineAttrs>,
    /// Deflate compression of each variable. When not set, the writer does not compress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    /// Number of rows in one chunk of the `obs` dimension. The default is 65536.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>, minimum = 1)]
    pub chunk_rows: Option<NonZeroUsize>,
    /// CF units of each timestamp variable, for example `seconds since 1970-01-01`.
    /// When not set, a timestamp keeps its Arrow unit since 1970-01-01T00:00:00Z.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, example = "seconds since 1970-01-01")]
    pub time_units: Option<TimeUnits>,
    /// Global attributes of the file. A value here wins over a `combine_attrs` value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub global_attributes: BTreeMap<String, AttributeOption>,
    /// Attributes of each variable, by column name. A value here wins over a `combine_attrs`
    /// value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variable_attributes: BTreeMap<String, BTreeMap<String, AttributeOption>>,
}

/// The value of an attribute option: a string or a number.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum AttributeOption {
    /// An integer. It is `int` in the file when it fits, else `int64`.
    Integer(i64),
    /// A floating-point number. It is `double` in the file.
    Float(f64),
    /// A string.
    Text(String),
}

impl AttributeOption {
    /// The NetCDF attribute value.
    pub fn to_netcdf(&self) -> netcdf::AttributeValue {
        match self {
            Self::Integer(value) => i32::try_from(*value).map_or(
                netcdf::AttributeValue::Longlong(*value),
                netcdf::AttributeValue::Int,
            ),
            Self::Float(value) => netcdf::AttributeValue::Double(*value),
            Self::Text(value) => netcdf::AttributeValue::Str(value.clone()),
        }
    }
}

/// Deflate compression of the flat NetCDF output.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Compression {
    /// Deflate level, from 0 (no compression) to 9 (maximum compression).
    #[schema(value_type = u8, minimum = 0, maximum = 9)]
    pub level: DeflateLevel,
    /// Apply the shuffle filter before deflate. The default is `true`.
    #[serde(default = "default_shuffle")]
    pub shuffle: bool,
}

fn default_shuffle() -> bool {
    true
}

/// A deflate level from 0 to 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct DeflateLevel(u8);

impl DeflateLevel {
    /// The level as a number from 0 to 9.
    pub fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for DeflateLevel {
    type Error = String;

    fn try_from(level: u8) -> Result<Self, Self::Error> {
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(format!(
                "invalid compression level {level}: expected 0 to 9"
            ))
        }
    }
}

impl From<DeflateLevel> for u8 {
    fn from(level: DeflateLevel) -> Self {
        level.0
    }
}

impl NetcdfWriterOptions {
    /// The default number of rows in one chunk of the `obs` dimension.
    pub const DEFAULT_CHUNK_ROWS: usize = 65536;

    /// Apply the flat output keys of `COPY ... OPTIONS (...)`.
    pub fn apply_format_options(
        &mut self,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<()> {
        if let Some(value) = format_option(format_options, "combine_attrs") {
            self.combine_attrs = Some(
                value
                    .parse()
                    .map_err(|e: String| exec_datafusion_err!("{e}"))?,
            );
        }
        let level = format_option(format_options, "compression.level");
        let shuffle = format_option(format_options, "compression.shuffle");
        if level.is_some() || shuffle.is_some() {
            let Some(level) = level else {
                return Err(exec_datafusion_err!(
                    "the NetCDF option 'compression.shuffle' needs 'compression.level'"
                ));
            };
            let level = level
                .trim()
                .parse::<u8>()
                .map_err(|_| {
                    exec_datafusion_err!("invalid compression level '{level}': expected 0 to 9")
                })
                .and_then(|level| {
                    DeflateLevel::try_from(level).map_err(|e| exec_datafusion_err!("{e}"))
                })?;
            let shuffle = match shuffle {
                Some(value) => parse_bool_option("compression.shuffle", value)?,
                None => default_shuffle(),
            };
            self.compression = Some(Compression { level, shuffle });
        }
        if let Some(value) = format_option(format_options, "chunk_rows") {
            self.chunk_rows = Some(value.trim().parse().map_err(|_| {
                exec_datafusion_err!("invalid chunk_rows '{value}': expected a number above 0")
            })?);
        }
        if let Some(value) = format_option(format_options, "time_units") {
            self.time_units = Some(
                value
                    .parse()
                    .map_err(|e: String| exec_datafusion_err!("{e}"))?,
            );
        }
        if let Some(value) = format_option(format_options, "global_attributes") {
            self.global_attributes = serde_json::from_str(value).map_err(|e| {
                exec_datafusion_err!(
                    "invalid global_attributes: expected a JSON object of name to string \
                     or number: {e}"
                )
            })?;
        }
        if let Some(value) = format_option(format_options, "variable_attributes") {
            self.variable_attributes = serde_json::from_str(value).map_err(|e| {
                exec_datafusion_err!(
                    "invalid variable_attributes: expected a JSON object of column to \
                     attributes: {e}"
                )
            })?;
        }
        Ok(())
    }

    /// The names of the options that have a value.
    pub fn set_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::new();
        if self.combine_attrs.is_some() {
            keys.push("combine_attrs");
        }
        if self.compression.is_some() {
            keys.push("compression");
        }
        if self.chunk_rows.is_some() {
            keys.push("chunk_rows");
        }
        if self.time_units.is_some() {
            keys.push("time_units");
        }
        if !self.global_attributes.is_empty() {
            keys.push("global_attributes");
        }
        if !self.variable_attributes.is_empty() {
            keys.push("variable_attributes");
        }
        keys
    }

    /// The number of rows in one chunk of the `obs` dimension.
    pub fn chunk_rows(&self) -> usize {
        self.chunk_rows
            .map_or(Self::DEFAULT_CHUNK_ROWS, NonZeroUsize::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(pairs: &[(&str, &str)]) -> datafusion::error::Result<NetcdfWriterOptions> {
        let map = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut options = NetcdfWriterOptions::default();
        options.apply_format_options(&map)?;
        Ok(options)
    }

    #[test]
    fn compression_reads_the_level_and_the_shuffle() {
        let options = apply(&[
            ("compression.level", "4"),
            ("compression.shuffle", "false"),
            ("format.chunk_rows", "1000"),
        ])
        .unwrap();
        assert_eq!(
            options.compression,
            Some(Compression {
                level: DeflateLevel(4),
                shuffle: false
            })
        );
        assert_eq!(options.chunk_rows(), 1000);
    }

    #[test]
    fn shuffle_is_on_by_default() {
        let options = apply(&[("compression.level", "1")]).unwrap();
        assert!(options.compression.unwrap().shuffle);
        assert_eq!(
            options.chunk_rows(),
            NetcdfWriterOptions::DEFAULT_CHUNK_ROWS
        );
    }

    #[test]
    fn bad_compression_values_are_errors() {
        for pairs in [
            &[("compression.level", "10")][..],
            &[("compression.level", "high")][..],
            &[("compression.shuffle", "true")][..],
            &[("chunk_rows", "0")][..],
        ] {
            assert!(apply(pairs).is_err(), "{pairs:?}");
        }
    }

    #[test]
    fn json_compression_checks_the_level() {
        let options: NetcdfWriterOptions =
            serde_json::from_str(r#"{"compression": {"level": 9}, "chunk_rows": 10}"#).unwrap();
        assert_eq!(options.compression.unwrap().level.get(), 9);
        let error =
            serde_json::from_str::<NetcdfWriterOptions>(r#"{"compression": {"level": 10}}"#)
                .unwrap_err()
                .to_string();
        assert!(error.contains("expected 0 to 9"), "{error}");
        assert!(serde_json::from_str::<NetcdfWriterOptions>(r#"{"chunk_rows": 0}"#).is_err());
    }

    #[test]
    fn time_units_are_checked_in_json_and_in_sql() {
        let options: NetcdfWriterOptions =
            serde_json::from_str(r#"{"time_units": "days since 1950-01-01"}"#).unwrap();
        assert_eq!(
            options.time_units.unwrap().as_str(),
            "days since 1950-01-01"
        );
        let error = serde_json::from_str::<NetcdfWriterOptions>(r#"{"time_units": "days"}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("time_units"), "{error}");
        let options = apply(&[("time_units", "seconds since 1970-01-01")]).unwrap();
        assert!(options.time_units.is_some());
        assert!(apply(&[("time_units", "weeks since 1970-01-01")]).is_err());
    }

    #[test]
    fn attributes_read_strings_and_numbers() {
        let options = apply(&[
            ("global_attributes", r#"{"title": "Argo", "version": 2}"#),
            (
                "variable_attributes",
                r#"{"temp": {"units": "degC", "scale": 0.5}}"#,
            ),
        ])
        .unwrap();
        assert_eq!(
            options.global_attributes["title"],
            AttributeOption::Text("Argo".to_string())
        );
        assert_eq!(
            options.global_attributes["version"].to_netcdf(),
            netcdf::AttributeValue::Int(2)
        );
        assert_eq!(
            options.variable_attributes["temp"]["scale"],
            AttributeOption::Float(0.5)
        );
        assert!(apply(&[("global_attributes", "title=Argo")]).is_err());
        assert!(apply(&[("variable_attributes", r#"{"temp": "degC"}"#)]).is_err());
    }
}
