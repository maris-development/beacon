/// Runtime configuration for the NetCDF format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — some
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetCDFConfig {
    /// Whether to generate per-file statistics during planning.
    pub enable_statistics: bool,
    /// The output directory for generated NetCDF files.
    pub output_dir: std::path::PathBuf,
    /// Enable partitioning
    pub enable_partitioning: bool,
}

impl Default for NetCDFConfig {
    fn default() -> Self {
        Self {
            enable_statistics: true,
            output_dir: std::path::PathBuf::from("/tmp"),
            enable_partitioning: false,
        }
    }
}
