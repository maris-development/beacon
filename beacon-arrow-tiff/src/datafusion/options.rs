/// Configuration options for the TIFF file format in DataFusion queries.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TiffOptions {
    /// Columns to treat as dimensions when reading.
    #[serde(default)]
    pub read_dimensions: Option<Vec<String>>,
}
