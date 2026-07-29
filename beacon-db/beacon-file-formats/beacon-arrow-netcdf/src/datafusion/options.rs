/// Configuration options for the NetCDF file format in DataFusion queries.
///
/// Controls both reading (schema inference, dimension handling) and writing
/// (flat vs. gridded output mode, compression).
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfOptions {
    /// Optional compression algorithm name (e.g. `"zlib"`, `"zstd"`).
    pub compression: Option<String>,
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
    /// Columns to use as NetCDF dimensions when **writing**.
    ///
    /// * `None` / empty → flat output via [`NetCDFSink`](super::sink::NetCDFSink)
    ///   with a single unlimited `obs` dimension.
    /// * `Some(["lat", "lon", …])` → gridded output via
    ///   [`NetCDFNdSink`](super::sink::NetCDFNdSink) where the listed columns
    ///   become named dimensions and remaining columns become data variables.
    #[serde(default)]
    pub write_dimensions: Option<Vec<String>>,
}

fn default_replay_batch_size() -> usize {
    128 * 1024
}
