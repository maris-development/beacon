use std::{fmt::Debug, sync::Arc};

use beacon_datafusion_ext::listing_factory::RootStore;
use object_store::ObjectMeta;

pub trait NetCDFObjectResolver: Send + Sync + Debug {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String>;
}

#[derive(Debug)]
pub struct DefaultNetCDFObjectResolver;

impl NetCDFObjectResolver for DefaultNetCDFObjectResolver {
    fn resolve(&self, _object: &ObjectMeta) -> anyhow::Result<String> {
        Err(anyhow::anyhow!("Unable to resolve object metadata (path): {} to netcdf native path which only supports local files and http/https.", _object.location.as_ref()))
    }
}

#[derive(Debug)]
pub struct NetCDFLocalObjectResolver {
    pub base_path: String,
}

impl NetCDFLocalObjectResolver {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

impl NetCDFObjectResolver for NetCDFLocalObjectResolver {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String> {
        let path = format!("{}/{}", self.base_path, object.location.as_ref());
        // Check if the path exists
        if !std::path::Path::new(&path).exists() {
            return Err(anyhow::anyhow!(
                "File does not exist or could not be resolved to a valid path: {}",
                path
            ));
        }
        Ok(path)
    }
}

#[derive(Debug)]
pub struct NetCDFHttpObjectResolver {
    pub base_url: String,
}

impl NetCDFHttpObjectResolver {
    pub fn new(base_url: String) -> Self {
        Self { base_url }
    }
}

impl NetCDFObjectResolver for NetCDFHttpObjectResolver {
    fn resolve(&self, object: &ObjectMeta) -> anyhow::Result<String> {
        Ok(format!(
            "{}/{}#mode=bytes",
            self.base_url,
            object.location.as_ref()
        ))
    }
}

pub fn create_object_resolver(root_store: &RootStore) -> Arc<dyn NetCDFObjectResolver> {
    match root_store {
        RootStore::FileSystem(base_path) => Arc::new(NetCDFLocalObjectResolver::new(
            base_path.to_string_lossy().to_string(),
        )),
        RootStore::HttpsStore(base_url) => {
            Arc::new(NetCDFHttpObjectResolver::new(base_url.clone()))
        }
    }
}
