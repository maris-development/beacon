//! The object stores of a host as a DataFusion `ObjectStoreRegistry`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::ObjectStore;
use url::{Position, Url};

use super::HostApi;

/// An `ObjectStoreRegistry` that reads the stores of a [`HostApi`] in each lookup.
///
/// `register_store` and `deregister_store` change only this value. A registered store
/// hides the host store of the same URL.
#[derive(Debug)]
pub struct HostStores {
    host: Arc<dyn HostApi>,
    /// The stores that the plugin registered, by scheme and authority.
    own: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl HostStores {
    /// A registry over the stores of `host`.
    pub fn new(host: Arc<dyn HostApi>) -> Self {
        Self {
            host,
            own: Mutex::default(),
        }
    }

    fn own(&self) -> MutexGuard<'_, HashMap<String, Arc<dyn ObjectStore>>> {
        self.own.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The `scheme://authority` text that identifies the store of `url`.
fn store_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[Position::BeforeHost..Position::AfterPort]
    )
}

impl ObjectStoreRegistry for HostStores {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.own().insert(store_key(url), store)
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = store_key(url);
        self.own().remove(&key).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the plugin registered no object store for {key}, and a Beacon store cannot be removed"
            ))
        })
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.own().get(&store_key(url)) {
            return Ok(Arc::clone(store));
        }
        // No cache: the host can replace a store, and its stores are the one source.
        self.host.object_store(url)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::testing::TestHost;

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    #[test]
    fn get_store_reads_the_host() {
        let host = TestHost::new();
        let stores = HostStores::new(host.clone());
        let want = host.object_store(&url("datasets://")).unwrap();
        let found = stores.get_store(&url("datasets:///a/x.txt")).unwrap();
        assert!(Arc::ptr_eq(&want, &found));
        assert!(stores.get_store(&url("nope://x")).is_err());
    }

    #[test]
    fn register_store_stays_in_the_plugin() {
        let host = TestHost::new();
        let stores = HostStores::new(host.clone());
        let own: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            stores
                .register_store(&url("mem://own"), Arc::clone(&own))
                .is_none()
        );
        let back = stores.get_store(&url("mem://own/some/path")).unwrap();
        assert!(Arc::ptr_eq(&own, &back));
        assert!(host.object_store(&url("mem://own")).is_err());
        let removed = stores.deregister_store(&url("mem://own")).unwrap();
        assert!(Arc::ptr_eq(&own, &removed));
        assert!(stores.get_store(&url("mem://own")).is_err());
    }

    #[test]
    fn a_plugin_store_hides_the_host_store() {
        let stores = HostStores::new(TestHost::new());
        let own: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        stores.register_store(&url("datasets://"), Arc::clone(&own));
        let back = stores.get_store(&url("datasets://")).unwrap();
        assert!(Arc::ptr_eq(&own, &back));
    }

    #[test]
    fn a_host_store_cannot_be_removed() {
        let stores = HostStores::new(TestHost::new());
        let error = stores.deregister_store(&url("datasets://")).unwrap_err();
        assert!(error.to_string().contains("cannot be removed"), "{error}");
        assert!(stores.get_store(&url("datasets://")).is_ok());
    }
}
