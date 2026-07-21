//! Configuration for the Zarr file format in DataFusion queries.
//!
//! Controls both reading (which dimensions to unpack) and writing (flat vs.
//! gridded output, chunking, compression, and whether the resulting store is
//! packed into a single zip file).

/// Compression applied to each chunk of a written zarr array.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ZarrCompression {
    /// No `bytes -> bytes` codec; chunks are stored raw.
    None,
    /// Zstandard, the zarr v3 default in most toolchains.
    #[default]
    Zstd,
    /// Gzip, for readers without zstd support.
    Gzip,
}

/// Zarr specification version to emit.
///
/// Only v3 can be written today: `zarrs` models v2 purely as a read-side format
/// that is converted to v3 in memory, and Beacon's own zarr reader discovers v3
/// stores only. The option exists so adding a v2 writer later is non-breaking.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ZarrVersion {
    #[default]
    #[serde(rename = "v3", alias = "3")]
    V3,
    #[serde(rename = "v2", alias = "2")]
    V2,
}

/// Configuration options for the Zarr file format.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ZarrOptions {
    /// Explicit dimensions requested when **reading**, via `read_zarr(paths,
    /// ['dims'])` or `CREATE EXTERNAL TABLE ... OPTIONS (read_dimensions '...')`.
    /// When `None`, a broadcast-compatible default set is auto-selected.
    #[serde(default)]
    pub read_dimensions: Option<Vec<String>>,
    /// Columns to use as zarr dimensions when **writing**.
    ///
    /// * `None` / empty → flat output via [`ZarrSink`](super::sink::ZarrSink):
    ///   every column becomes a 1-D array over a shared `obs` dimension.
    /// * `Some(["lat", "lon", …])` → gridded output via
    ///   [`ZarrNdSink`](super::sink::ZarrNdSink): the listed columns become
    ///   coordinate arrays and the remaining columns become data arrays indexed
    ///   by them.
    #[serde(default)]
    pub write_dimensions: Option<Vec<String>>,
    /// Column names whose unique values are collected during execution to form
    /// the dimension axes of a gridded write.
    #[serde(default)]
    pub unique_value_columns: Vec<String>,
    /// Pack the written store into a single zip file at the output path.
    ///
    /// The HTTP query API sets this because it can only hand back one file; a
    /// `COPY TO` leaves it off and writes a plain directory store.
    #[serde(default)]
    pub zip_output: bool,
    /// Chunk length along each dimension. A zarr store is a directory of chunk
    /// files, so this trades file count against per-read granularity.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// Chunk compression codec.
    #[serde(default)]
    pub compression: ZarrCompression,
    /// Zarr specification version to emit.
    #[serde(default)]
    pub zarr_version: ZarrVersion,
}

impl Default for ZarrOptions {
    fn default() -> Self {
        Self {
            read_dimensions: None,
            write_dimensions: None,
            unique_value_columns: Vec::new(),
            zip_output: false,
            chunk_size: default_chunk_size(),
            compression: ZarrCompression::default(),
            zarr_version: ZarrVersion::default(),
        }
    }
}

fn default_chunk_size() -> usize {
    64 * 1024
}
