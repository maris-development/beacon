#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TableExtensionDescription {
    pub extension_name: String,
    pub extension_description: String,
    pub function_name: String,
    pub function_args: Vec<String>,
    pub example_usage: String,
}
