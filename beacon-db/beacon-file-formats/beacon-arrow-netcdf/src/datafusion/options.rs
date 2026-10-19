use std::{collections::HashMap, num::NonZeroUsize};

use beacon_datafusion_ext::format_options::{format_option, parse_bool_option};
use datafusion::common::exec_datafusion_err;

use crate::attributes::CombineAttrs;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfReaderOptions {
    /// Whether to skip a file that causes an error during reading, instead of failing the query.
    pub skip_on_error: bool,
}

impl Default for NetcdfReaderOptions {
    fn default() -> Self {
        Self {
            skip_on_error: false,
        }
    }
}

impl NetcdfReaderOptions {
    pub fn apply_format_options(
        &mut self,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<()> {
        if let Some(value) = format_option(format_options, "skip_on_error") {
            self.skip_on_error = parse_bool_option("skip_on_error", value)?;
        }
        Ok(())
    }
}

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
}
