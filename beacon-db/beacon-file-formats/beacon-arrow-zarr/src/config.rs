//! [`ZarrConfig`]: the runtime settings of the Zarr format.

/// Runtime configuration for the Zarr format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — each
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct ZarrConfig {
    /// Whether to generate per-file statistics.
    ///
    /// The Zarr format generates no statistics yet, so every store reports unknown statistics.
    pub enable_statistics: bool,
    /// The maximum number of opened leaf groups to cache.
    pub dataset_cached_size: usize,
}

impl Default for ZarrConfig {
    fn default() -> Self {
        Self {
            enable_statistics: true,
            dataset_cached_size: 32,
        }
    }
}
