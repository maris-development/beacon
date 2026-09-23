//! Per-query settings of the Atlas format.

/// Settings that change what a scan reads, not how fast it reads it. Set by
/// `read_atlas(paths, dimensions)` or `CREATE EXTERNAL TABLE` options.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AtlasOptions {
    /// The dimensions the table reads, or `None` for a broadcast-compatible
    /// default per dataset. Drops arrays with a dimension outside the list.
    pub read_dimensions: Option<Vec<String>>,
    /// Skip a dataset whose columns fit no one grid, with a warning, instead
    /// of failing the query.
    pub skip_unbroadcastable: bool,
}
