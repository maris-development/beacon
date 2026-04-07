use datafusion::{datasource::file_format::FileFormatFactory, object_store::ObjectMeta};

pub trait FileFormatFactoryExt: FileFormatFactory + Send + Sync {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>>;
    fn file_format_name(&self) -> String;
    fn list_with_file_extension(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetMetadata {
    pub file_path: String,
    pub format: String,
    pub can_inspect: bool,
    pub can_partial_explore: bool,
}

impl DatasetMetadata {
    pub fn new(file_path: String, format: String) -> Self {
        Self {
            file_path,
            format,
            can_inspect: false,
            can_partial_explore: false,
        }
    }
}
