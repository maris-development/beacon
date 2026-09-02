//! Per-query settings of the Atlas format.

/// Settings that change *what* a scan reads, as opposed to how fast it does so.
///
/// The runtime settings live in [`AtlasConfig`](crate::AtlasConfig). These come
/// from the query: `read_atlas(paths, dimensions)` sets them, and so does
/// `CREATE EXTERNAL TABLE ... OPTIONS ('read_dimensions' '…')`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AtlasOptions {
    /// The dimensions the table reads, or `None` to pick a broadcast-compatible
    /// default per dataset.
    ///
    /// An array survives only when every one of its dimensions is in the list,
    /// so this is how a query drops the wide grids of a collection and keeps its
    /// coordinates.
    pub read_dimensions: Option<Vec<String>>,
}
