//! Test helpers: a host with one store, and its FFI table through the foreign path.
//!
//! Other crates get this module with the `test-util` feature.

use std::sync::Arc;

use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::runtime::Handle;
use url::Url;

use crate::ffi::FFI_Host;
use crate::{HostApi, ResolvedLocation};

/// The datasets base URL of [`TestHost`].
pub const BASE_URL: &str = "file:///beacon/data/datasets/";

/// A host for tests: `a/x.txt` = `hi` in `datasets://`, and a small location rule.
///
/// The location `boom` and the scheme `boom` panic. A location that starts with `bad` is an
/// error. A location that starts with `/` has no store.
#[derive(Debug)]
pub struct TestHost {
    /// The stores of the host. A test can add or replace one.
    pub stores: DefaultObjectStoreRegistry,
}

impl TestHost {
    /// A host with the object `a/x.txt` in the store `datasets://`.
    pub fn new() -> Arc<Self> {
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

/// A library marker that differs from the marker of this build.
pub extern "C" fn foreign_marker_id() -> usize {
    datafusion_ffi::get_library_marker_id() + 1
}

/// A runtime for the host of the tests. It lives to the end of the test binary.
pub fn test_handle() -> Handle {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            // The exports only enter this runtime. A current-thread runtime starts no
            // thread, and Miri refuses a thread that lives to the end.
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
        })
        .handle()
        .clone()
}

/// `host` as a table that imports through the foreign path.
pub fn foreign_table(host: Arc<TestHost>) -> FFI_Host {
    let mut ffi = FFI_Host::new(host, test_handle());
    ffi.library_marker_id = foreign_marker_id;
    ffi
}
