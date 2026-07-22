use std::collections::HashMap;
use std::sync::Arc;

use datafusion::{
    catalog::Session,
    datasource::file_format::{FileFormat, FileFormatFactory},
    object_store::ObjectMeta,
    prelude::SessionContext,
};

use crate::listing_factory::RootStore;

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

    /// Whether this format reads files *natively* — opened by local path / URL
    /// (netCDF-c, Atlas) rather than streamed through the object store. Native
    /// readers can only open local files and http/https, so the listing layer
    /// resolves a [`RootStore`] for them at plan time (rejecting object stores like
    /// s3/gs/az) and hands it to [`Self::create_with_native_root`]. Object-store
    /// formats (Parquet, CSV, …) read any scheme and return `false`.
    fn native_read_only(&self) -> bool {
        false
    }

    /// Create a [`FileFormat`] that translates object paths to native-reader paths
    /// against `root` (a local dir or an https base, resolved by the listing
    /// factory). Only meaningful when [`Self::native_read_only`] is `true`; the
    /// default ignores `root` and delegates to [`FileFormatFactory::create`].
    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        _root: RootStore,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.create(state, format_options)
    }
}

/// Beacon's [`FileFormatFactoryExt`] factories, keyed by the format names and file
/// extensions they answer to. Registered (via a late-filled `OnceLock`) as a
/// session-config extension so plan-time code — the external-table builder, table
/// functions — can recover format capabilities (like
/// [`FileFormatFactoryExt::native_read_only`]) that DataFusion's plain
/// `FileFormatFactory` registry erases once the concrete `Ext` type is gone.
#[derive(Default)]
pub struct FileFormatRegistry {
    by_key: HashMap<String, Arc<dyn FileFormatFactoryExt>>,
}

impl FileFormatRegistry {
    pub fn new(formats: &[Arc<dyn FileFormatFactoryExt>]) -> Self {
        let mut by_key = HashMap::new();
        for format in formats {
            by_key
                .entry(format.file_format_name().to_ascii_lowercase())
                .or_insert_with(|| format.clone());
            for ext in format.file_extensions() {
                by_key
                    .entry(ext.to_ascii_lowercase())
                    .or_insert_with(|| format.clone());
            }
        }
        Self { by_key }
    }

    /// The factory answering to `key` (a format name or file extension), if any.
    pub fn get(&self, key: &str) -> Option<&Arc<dyn FileFormatFactoryExt>> {
        self.by_key.get(&key.to_ascii_lowercase())
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
