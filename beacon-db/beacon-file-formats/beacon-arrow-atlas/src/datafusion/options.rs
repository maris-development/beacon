/// Configuration options for the atlas file format in DataFusion queries.
#[derive(Debug, Default, Clone)]
pub struct AtlasOptions {
    /// Optional list of dimension names used to filter which arrays are read.
    ///
    /// When `Some`, only arrays whose dimensions match the listed dimensions
    /// are kept (applied via [`beacon_nd_array::projection::DatasetProjection`]).
    /// When `None` (default), all arrays are read.
    pub read_dimensions: Option<Vec<String>>,
}
