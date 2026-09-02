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
    ///
    /// A collection is immutable, so a cached handle stays valid until its
    /// deletion mask changes. The cache saves the footer read and keeps the
    /// decompressed blocks of a collection between queries.
    pub use_reader_cache: bool,
    /// How many opened collections the shared reader cache holds.
    ///
    /// Each entry owns its own block cache — 256 MiB of decompressed blocks and
    /// 64 MiB of raw slabs — so this is also a memory bound: the default of 32
    /// admits up to 10 GiB of cached blocks across every open collection.
    pub reader_cache_size: u64,
    /// Whether a predicate scan drops the datasets that cannot match, before it
    /// reads them.
    ///
    /// A pure optimization: pruning only ever removes datasets that hold no
    /// matching row, and every path fails open. Off trades throughput for
    /// skipping the index build.
    pub use_pruning: bool,
    /// Whether the file analyzer measures a collection's column ranges.
    ///
    /// The ranges come from the footer, so they cost no array read. The switch
    /// exists because a listing of many collections turns even a footer read
    /// per collection into real I/O.
    pub enable_statistics: bool,
}

impl Default for AtlasConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 32,
            use_pruning: true,
            enable_statistics: true,
        }
    }
}
