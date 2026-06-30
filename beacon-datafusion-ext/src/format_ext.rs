use std::sync::Arc;

use datafusion::{
    datasource::file_format::{FileFormat, FileFormatFactory},
    object_store::ObjectMeta,
    prelude::SessionContext,
};

pub trait FileFormatFactoryExt: FileFormatFactory + Send + Sync {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>>;
    fn file_format_name(&self) -> String;
    fn list_with_file_extension(&self) -> bool {
        true
    }

    /// The filename extensions this format recognizes (e.g. `["tiff", "tif"]`).
    ///
    /// DataFusion's session registry keys a format only under its canonical
    /// `get_ext()`, so resolving a format from a raw filename extension must
    /// consult this list to honor aliases. Defaults to the canonical extension;
    /// override when a format accepts more than one spelling.
    fn file_extensions(&self) -> Vec<String> {
        vec![self.get_ext()]
    }
}

pub fn file_format_by_ext(ext: &str, session_ctx: &SessionContext) -> Option<Arc<dyn FileFormat>> {
    let state = session_ctx.state();
    let factory = state.get_file_format_factory(ext)?;
    Some(factory.default())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetMetadata {
    pub file_path: String,
    pub format: String,
    pub can_inspect: bool,
    pub can_partial_explore: bool,
    /// Total size in bytes of the underlying object(s), when known. Filled in by
    /// `list_datasets` from the object listing; `None` for datasets whose size
    /// can't be resolved (e.g. no matching object).
    pub size: Option<u64>,
    /// Last-modified time of the underlying object(s) (RFC 3339), when known.
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

impl DatasetMetadata {
    pub fn new(file_path: String, format: String) -> Self {
        Self {
            file_path,
            format,
            can_inspect: false,
            can_partial_explore: false,
            size: None,
            last_modified: None,
        }
    }
}
