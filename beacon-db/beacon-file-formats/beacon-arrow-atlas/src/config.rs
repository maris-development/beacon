//! [`AtlasConfig`]: the runtime settings of the Atlas format.

/// Runtime configuration for the Atlas format.
///
/// Plain data with sensible defaults; the caller populates it. There is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from. Each field is the *default* for a runtime, and
/// each can be overridden per table via
/// `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct AtlasConfig {
    /// Whether a read consults the shared reader cache.
    pub use_reader_cache: bool,
    pub reader_cache_size: u64,
    pub use_pruning: bool,
}

impl Default for AtlasConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 32,
            use_pruning: true,
        }
    }
}
