//! Beacon as the host of plugin table code: its object stores and its `LOCATION` rule.

use std::sync::Arc;

use beacon_object_store::resolve_location;
use beacon_plugin_sdk::host::ffi::FFI_Host;
use beacon_plugin_sdk::host::{HostApi, ResolvedLocation};
use beacon_plugin_sdk::object_store::store::ObjectStore;
use datafusion::error::Result;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use tokio::runtime::Handle;
use url::Url;

/// The settings of the plugin host. Register one as a session config extension.
#[derive(Debug, Clone)]
pub struct PluginHostInfo {
    /// The runtime that runs the store I/O of a plugin: the query runtime.
    pub runtime: Handle,
    /// The URL of the datasets store. `None` when Beacon has no datasets store.
    pub datasets_url: Option<ObjectStoreUrl>,
    /// The location of the datasets store, for a reader that opens a path or URL itself.
    pub datasets_base_url: Option<Url>,
}

impl PluginHostInfo {
    /// The host of one plugin as FFI data, with the stores of `registry`.
    pub fn ffi_host(&self, registry: Arc<dyn ObjectStoreRegistry>) -> FFI_Host {
        let host = BeaconHost {
            stores: registry,
            datasets_url: self.datasets_url.clone(),
            datasets_base_url: self.datasets_base_url.clone(),
        };
        FFI_Host::new(Arc::new(host), self.runtime.clone())
    }
}

/// Beacon as a [`HostApi`]. The `LOCATION` rule is `beacon_object_store::resolve_location`.
#[derive(Debug, Clone)]
pub struct BeaconHost {
    /// The object stores of the session.
    pub stores: Arc<dyn ObjectStoreRegistry>,
    /// The URL of the datasets store. `None` when Beacon has no datasets store.
    pub datasets_url: Option<ObjectStoreUrl>,
    /// The location of the datasets store.
    pub datasets_base_url: Option<Url>,
}

impl HostApi for BeaconHost {
    fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.stores.get_store(url)
    }

    fn datasets_url(&self) -> Option<ObjectStoreUrl> {
        self.datasets_url.clone()
    }

    fn datasets_base_url(&self) -> Option<Url> {
        self.datasets_base_url.clone()
    }

    fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
        let found = resolve_location(self.datasets_url.as_ref(), location)?;
        Ok(ResolvedLocation {
            store_url: found.store_url,
            path: found.path,
        })
    }
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::object_store::store::memory::InMemory;
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;

    use super::*;

    #[test]
    fn the_host_gives_the_stores_and_urls_of_the_session() {
        let stores = Arc::new(DefaultObjectStoreRegistry::new());
        let datasets = ObjectStoreUrl::parse("datasets://").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        stores.register_store(datasets.as_ref(), Arc::clone(&store));
        let base = Url::parse("s3://bucket/").unwrap();
        let host = BeaconHost {
            stores,
            datasets_url: Some(datasets.clone()),
            datasets_base_url: Some(base.clone()),
        };
        let found = host.object_store(datasets.as_ref()).unwrap();
        assert!(Arc::ptr_eq(&store, &found));
        assert!(host.object_store(&Url::parse("nope://x").unwrap()).is_err());
        assert_eq!(host.datasets_url(), Some(datasets));
        assert_eq!(host.datasets_base_url(), Some(base));
    }

    #[test]
    fn the_host_resolves_with_the_rule_of_beacon() {
        let host = BeaconHost {
            stores: Arc::new(DefaultObjectStoreRegistry::new()),
            datasets_url: Some(ObjectStoreUrl::parse("datasets://").unwrap()),
            datasets_base_url: None,
        };
        let found = host.resolve("datasets://obs/a.txt").unwrap();
        assert_eq!(found.store_url.unwrap().as_str(), "datasets:///");
        assert_eq!(found.path, "obs/a.txt");
        let found = host.resolve("/data/a.txt").unwrap();
        assert_eq!(found.store_url, None);
    }
}
