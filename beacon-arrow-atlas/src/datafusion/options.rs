/// Configuration options for the atlas file format in DataFusion queries.
///
/// Reserved for future expansion. The current read path has no tunables
/// (atlas already maintains its own internal array-file cache).
#[derive(Debug, Default, Clone)]
pub struct AtlasOptions {}
