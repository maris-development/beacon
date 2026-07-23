use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::{
    catalog::Session,
    datasource::{
        file_format::{FileFormat, FileFormatFactory},
        listing::ListingTableUrl,
    },
    object_store::ObjectMeta,
    prelude::SessionContext,
};

use crate::listing_factory::ListingFactory;

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

    /// Create a [`FileFormat`] for files located at `url`.
    ///
    /// A format read *natively* — opened by local path or http(s) URL by an
    /// external reader (netCDF-c), never streamed through the object store — needs
    /// to know which [`RootStore`](crate::listing_factory::RootStore) its objects
    /// live under so it can turn each
    /// listed object into a path that reader can open. `listing` resolves that
    /// from `url` via [`ListingFactory::native_read_root`].
    ///
    /// The default ignores both and delegates to [`FileFormatFactory::create`], so
    /// object-store formats (Parquet, CSV, …) are unaffected and no location is
    /// rejected on their behalf. Only an overriding format calls
    /// `native_read_root`, so the "only local files and http/https" error is raised
    /// exactly for the formats that have that limitation.
    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.create(state, format_options)
    }
}

/// Shared, late-filled handle to the [`FileFormatRegistry`], registered as a
/// session-config extension. The formats are built *after* the session (they need
/// it), so the cell is registered empty during config construction and filled once
/// the factories exist — the same pattern as the session and crawler handles.
pub type FileFormatRegistryHandle = Arc<OnceLock<FileFormatRegistry>>;

/// Create an empty registry handle to register as a session extension.
pub fn new_file_format_registry_handle() -> FileFormatRegistryHandle {
    Arc::new(OnceLock::new())
}

/// The beacon [`FileFormatFactoryExt`] answering to `key` (a format name or file
/// extension), recovered from the session's registry handle.
///
/// This is the only way back to the `Ext` trait: DataFusion's own registry hands
/// out `Arc<dyn FileFormatFactory>`, and there is no upcast from that to
/// [`FileFormatFactoryExt`] (nor any way to downcast to a concrete factory from
/// this crate, which the format crates depend on rather than the reverse).
///
/// `None` when the registry is absent or unfilled, or nothing answers to `key`.
pub fn try_file_format_factory_ext(
    session: &dyn Session,
    key: &str,
) -> Option<Arc<dyn FileFormatFactoryExt>> {
    session
        .config()
        .get_extension::<OnceLock<FileFormatRegistry>>()?
        .get()?
        .get(key)
        .cloned()
}

/// Beacon's [`FileFormatFactoryExt`] factories, keyed by the format names and file
/// extensions they answer to. Registered (via a late-filled `OnceLock`) as a
/// session-config extension so plan-time code — the external-table builder, table
/// functions — can recover format capabilities (like
/// [`FileFormatFactoryExt::create_with_native_root`]) that DataFusion's plain
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
