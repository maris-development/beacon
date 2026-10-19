#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetCDFConfig {
    pub enable_statistics: bool,
}
