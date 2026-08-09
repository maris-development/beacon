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
    /// On by default. A zarr store answers from its metadata where it can, and
    /// otherwise reads only the rank-0 and rank-1 arrays — the coordinates a
    /// `WHERE` clause names. A data grid of rank 2 or higher is never read.
    /// The switch exists because a collection of many small stores turns even
    /// that into real I/O, and an operator needs a way to stop it.
    pub enable_statistics: bool,
}

impl Default for ZarrConfig {
    fn default() -> Self {
        Self {
            enable_statistics: true,
        }
    }
}
