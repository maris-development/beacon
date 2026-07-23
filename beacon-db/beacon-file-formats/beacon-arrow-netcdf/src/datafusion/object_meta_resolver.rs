use std::{fmt::Debug, sync::Arc};

use beacon_datafusion_ext::listing_factory::RootStore;
use object_store::ObjectMeta;

pub trait NetCDFObjectResolver: Send + Sync + Debug {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String>;
}

/// The resolver a format gets when it was built without a location — through
/// [`FileFormatFactory::default`](datafusion::datasource::file_format::FileFormatFactory::default),
/// or plain `create`. It cannot resolve anything: which root store the objects
/// live under is exactly the information that was not supplied.
///
/// Reaching this in a scan means the format was built off the location-less path;
/// see `FileFormatFactoryExt::create_with_native_root`.
#[derive(Debug)]
pub struct DefaultNetCDFObjectResolver;

impl NetCDFObjectResolver for DefaultNetCDFObjectResolver {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String> {
        Err(anyhow::anyhow!(
            "Unable to resolve object metadata (path): {} to netcdf native path \
             which only supports local files and http/https.",
            object.location.as_ref()
        ))
    }
}

/// Resolves each object against the [`RootStore`] its table's location resolved
/// to — a local directory or an https base.
///
/// The path construction itself is [`RootStore::to_native_path`], so netCDF and
/// the listing layer cannot disagree about where a file is.
#[derive(Debug)]
pub struct RootStoreObjectResolver {
    root: RootStore,
}

impl RootStoreObjectResolver {
    pub fn new(root: RootStore) -> Self {
        Self { root }
    }
}

impl NetCDFObjectResolver for RootStoreObjectResolver {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String> {
        let path = self.root.to_native_path(&object.location);

        // Local files are checked up front: netcdf-c reports a missing file as an
        // opaque error, so failing here names the path that was actually tried.
        // An https base is not probed — that would cost a request per object.
        if matches!(self.root, RootStore::FileSystem(_)) && !std::path::Path::new(&path).exists() {
            return Err(anyhow::anyhow!(
                "File does not exist or could not be resolved to a valid path: {}",
                path
            ));
        }

        Ok(path)
    }
}

pub fn create_object_resolver(root_store: &RootStore) -> Arc<dyn NetCDFObjectResolver> {
    Arc::new(RootStoreObjectResolver::new(root_store.clone()))
}
