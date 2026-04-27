#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetcdfOptions {
    pub compression: Option<String>,
    #[serde(default)]
    pub unique_value_columns: Vec<String>,
    #[serde(default = "default_replay_batch_size")]
    pub replay_batch_size: usize,
    #[serde(default)]
    pub read_dimensions: Option<Vec<String>>,
}

fn default_replay_batch_size() -> usize {
    128 * 1024
}
