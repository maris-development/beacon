//! What Beacon offers to the table code of a plugin: its object stores and its datasets store.
//!
//! - [`HostApi`] is the interface. Beacon implements it.
//! - [`Host`] is what plugin code uses. Get it with [`Host::of`].
//! - [`ffi`] carries a [`HostApi`] across the plugin boundary.
//! - [`HostStores`] gives the stores of a host to DataFusion code.

pub mod ffi;
mod stores;

use std::fmt;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::plan_err;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;
use url::Url;

pub use stores::HostStores;

/// A `LOCATION` as an object store and a path in that store.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLocation {
    /// The URL of the store. `None` for a path of the local machine.
    pub store_url: Option<ObjectStoreUrl>,
    /// The path in the store, with glob characters unchanged. No leading `/` in a store.
    pub path: String,
}

/// An open `LOCATION`: the object store and the path in it.
#[derive(Debug, Clone)]
pub struct OpenLocation {
    /// The object store of Beacon that holds the location.
    pub store: Arc<dyn ObjectStore>,
    /// The URL of the store.
    pub store_url: ObjectStoreUrl,
    /// The path in the store, with glob characters unchanged.
    pub path: String,
}

/// The services of a Beacon server for plugin table code.
///
/// Beacon implements this trait. A test of a plugin can implement it too.
pub trait HostApi: fmt::Debug + Send + Sync {
    /// The object store of `url`, found by scheme and authority.
    fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>>;

    /// The store and the path of the `LOCATION` text of a table. An error refuses the location.
    fn resolve(&self, location: &str) -> Result<ResolvedLocation>;

    /// The URL of the datasets store, such as `datasets://`. `None` without a datasets store.
    fn datasets_url(&self) -> Option<ObjectStoreUrl>;

    /// The location of the datasets store, such as `file:///beacon/data/datasets/`.
    fn datasets_base_url(&self) -> Option<Url>;
}

/// The Beacon server that runs the table code.
#[derive(Debug, Clone)]
pub struct Host {
    api: Arc<dyn HostApi>,
}

impl Host {
    /// A host over `api`. Table code gets its host from [`Host::of`]; a test builds one here.
    pub fn new(api: Arc<dyn HostApi>) -> Self {
        Self { api }
    }

    /// The host of `state`, the session that the table code got.
    ///
    /// Fails for a session that is not from Beacon.
    pub fn of(state: &dyn Session) -> Result<Self> {
        match state.as_any().downcast_ref::<Host>() {
            Some(host) => Ok(host.clone()),
            None => plan_err!("this session has no Beacon host: use the session of the table call"),
        }
    }

    /// The interface of this host.
    pub fn api(&self) -> &Arc<dyn HostApi> {
        &self.api
    }

    /// The object store of `url`, found by scheme and authority.
    pub fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.api.object_store(url)
    }

    /// The store URL and the path of the `LOCATION` text of a table. It opens nothing.
    ///
    /// Beacon owns the rule. A relative location is a path in the datasets store.
    pub fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
        self.api.resolve(location)
    }

    /// Opens the object store of the `LOCATION` text of a table.
    ///
    /// Fails for a path of the local machine, which has no store.
    pub fn open(&self, location: &str) -> Result<OpenLocation> {
        let found = self.resolve(location)?;
        let Some(store_url) = found.store_url else {
            return plan_err!("the location `{location}` is not in an object store");
        };
        let url: &Url = store_url.as_ref();
        Ok(OpenLocation {
            store: self.object_store(url)?,
            store_url,
            path: found.path,
        })
    }

    /// The `LOCATION` text of a table as a DataFusion `ListingTableUrl`.
    ///
    /// Use it for a `ListingTable`, or with `list_all_files` for a location with a glob.
    pub fn listing_url(&self, location: &str) -> Result<ListingTableUrl> {
        let found = self.resolve(location)?;
        let Some(store_url) = found.store_url else {
            return ListingTableUrl::parse(&found.path);
        };
        // DataFusion reads a glob only in a path with no scheme, so the glob goes in apart.
        let (folder, glob) = match found.path.find(['?', '*', '[']) {
            Some(at) => {
                let split = found.path[..at].rfind('/').map_or(0, |slash| slash + 1);
                (&found.path[..split], Some(&found.path[split..]))
            }
            None => (found.path.as_str(), None),
        };
        // The store URL ends with `/`.
        let url = ListingTableUrl::parse(format!("{}{folder}", store_url.as_str()))?;
        match glob {
            Some(glob) => url.with_glob(glob),
            None => Ok(url),
        }
    }

    /// The URL of the datasets store, such as `datasets://`. `None` without a datasets store.
    pub fn datasets_url(&self) -> Option<ObjectStoreUrl> {
        self.api.datasets_url()
    }

    /// The location of the datasets store, such as `file:///beacon/data/datasets/` or
    /// `s3://bucket/`. Use it for a reader that opens a path or URL by itself.
    ///
    /// The object store of this URL is not the datasets store. Read that one with [`Host::open`].
    pub fn datasets_base_url(&self) -> Option<Url> {
        self.api.datasets_base_url()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    pub(crate) const BASE_URL: &str = "file:///beacon/data/datasets/";

    /// A host for tests: `a/x.txt` = `hi` in `datasets://`, and a small location rule.
    #[derive(Debug)]
    pub(crate) struct TestHost {
        pub(crate) stores: DefaultObjectStoreRegistry,
    }

    impl TestHost {
        pub(crate) fn new() -> Arc<Self> {
            let store = InMemory::new();
            futures::executor::block_on(
                store.put(&Path::from("a/x.txt"), PutPayload::from_static(b"hi")),
            )
            .unwrap();
            let stores = DefaultObjectStoreRegistry::new();
            stores.register_store(&Url::parse("datasets://").unwrap(), Arc::new(store));
            Arc::new(Self { stores })
        }
    }

    impl HostApi for TestHost {
        fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
            if url.scheme() == "boom" {
                panic!("boom store");
            }
            self.stores.get_store(url)
        }

        fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
            if location == "boom" {
                panic!("boom resolve");
            }
            if location.starts_with("bad") {
                return plan_err!("the host refuses {location}");
            }
            let store_url = match location.starts_with('/') {
                true => None,
                false => self.datasets_url(),
            };
            Ok(ResolvedLocation {
                store_url,
                path: location.to_string(),
            })
        }

        fn datasets_url(&self) -> Option<ObjectStoreUrl> {
            Some(ObjectStoreUrl::parse("datasets://").unwrap())
        }

        fn datasets_base_url(&self) -> Option<Url> {
            Some(Url::parse(BASE_URL).unwrap())
        }
    }

    fn host() -> Host {
        Host::new(TestHost::new())
    }

    #[test]
    fn open_reads_the_store_of_the_location() {
        let open = host().open("a/x.txt").unwrap();
        assert_eq!(open.store_url.as_str(), "datasets:///");
        let bytes = futures::executor::block_on(async {
            open.store.get(&Path::from(open.path)).await?.bytes().await
        })
        .unwrap();
        assert_eq!(bytes.as_ref(), b"hi");
    }

    #[test]
    fn open_refuses_a_local_path() {
        let error = host().open("/data/x").unwrap_err().to_string();
        assert!(
            error.contains("the location `/data/x` is not in an object store"),
            "{error}"
        );
    }

    #[test]
    fn open_gives_the_error_of_the_host() {
        let error = host().open("bad/x").unwrap_err().to_string();
        assert!(error.contains("the host refuses bad/x"), "{error}");
    }

    #[test]
    fn listing_url_keeps_the_store_and_the_glob() {
        let host = host();
        let url = host.listing_url("a/*.txt").unwrap();
        assert_eq!(url.object_store().as_str(), "datasets:///");
        assert_eq!(url.prefix().as_ref(), "a");
        let store = host.object_store(url.object_store().as_ref()).unwrap();
        let state = SessionContext::new().state();
        let files: Vec<String> = futures::executor::block_on(async {
            let listed = url.list_all_files(&state, store.as_ref(), "").await?;
            listed.try_collect::<Vec<_>>().await
        })
        .unwrap()
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
        assert_eq!(files, vec!["a/x.txt".to_string()]);
    }

    #[test]
    fn listing_url_of_a_local_path_is_a_file_url() {
        let url = host().listing_url("/data/x.txt").unwrap();
        assert_eq!(url.scheme(), "file");
    }

    #[test]
    fn a_session_of_another_kind_has_no_host() {
        let state = SessionContext::new().state();
        let error = Host::of(&state).unwrap_err().to_string();
        assert!(error.contains("this session has no Beacon host"), "{error}");
    }
}
