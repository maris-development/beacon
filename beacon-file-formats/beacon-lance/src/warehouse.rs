//! Runtime-scoped Lance warehouse, backed by beacon's tables object store.
//!
//! Rather than raw filesystem paths, managed Lance datasets are read/written
//! through the same `object_store` instance beacon uses for the tables store. We
//! do this with Lance's [`ObjectStoreProvider`]/[`ObjectStoreRegistry`]: a
//! provider that hands Lance our store for a custom `beacon-tables://` scheme is
//! registered in a registry, which travels to Lance through a [`Session`]. Reads
//! (`DatasetBuilder::with_session`) and writes (`WriteParams.session`) both pull
//! the registry from that session.
//!
//! The warehouse is **runtime-scoped** (built per `Runtime`, threaded through the
//! session as an extension), so multiple runtimes stay isolated. Beacon always
//! backs the tables store with a local filesystem, so this stays local in
//! practice — the abstraction just keeps Lance consistent with the rest of beacon.

use std::collections::HashMap;
use std::sync::Arc;

use lance::session::Session;
use lance_core::Result as LanceResult;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry,
};
use object_store::DynObjectStore;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use url::Url;

/// The single namespace beacon creates managed tables under (kept parallel to
/// the Iceberg integration so table layouts are predictable).
pub const BEACON_NAMESPACE: &str = "beacon";

/// Custom URI scheme used to route managed Lance datasets through beacon's tables
/// object store via the registered [`BeaconTablesProvider`].
const SCHEME: &str = "beacon-tables";

/// Sub-prefix within the tables store under which managed Lance datasets live.
const PREFIX: &str = "lance";

/// The namespace beacon uses, in the `Vec<String>` form the rest of the crate
/// expects (mirrors `beacon_iceberg::beacon_namespace`).
pub fn beacon_namespace() -> Vec<String> {
    vec![BEACON_NAMESPACE.to_string()]
}

/// A Lance object-store provider that hands Lance beacon's tables store for the
/// `beacon-tables://` scheme.
#[derive(Debug)]
struct BeaconTablesProvider {
    inner: Arc<DynObjectStore>,
}

#[async_trait::async_trait]
impl ObjectStoreProvider for BeaconTablesProvider {
    async fn new_store(
        &self,
        base_path: Url,
        _params: &ObjectStoreParams,
    ) -> LanceResult<ObjectStore> {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        Ok(ObjectStore::new(
            self.inner.clone(),
            base_path,
            None,  // block_size
            None,  // wrapper
            false, // use_constant_size_upload_parts
            false, // list_is_lexically_ordered (local FS isn't; let Lance sort)
            parallelism,
            3, // download_retry_count
            None,
        ))
    }

    /// Give each dataset its own registry/cache entry (and thus an `ObjectStore`
    /// with the correct base path), rather than sharing one keyed only by scheme.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> LanceResult<String> {
        Ok(format!(
            "{}${}{}",
            url.scheme(),
            url.authority(),
            url.path()
        ))
    }
}

/// A runtime-scoped Lance warehouse over beacon's tables object store.
#[derive(Debug)]
pub struct LanceWarehouse {
    /// The tables object store (used directly for `DROP` cleanup).
    store: Arc<DynObjectStore>,
    /// Lance session carrying the registry that resolves `beacon-tables://`.
    session: Arc<Session>,
    /// Per-dataset write locks, keyed by dataset URI.
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl LanceWarehouse {
    /// Build a warehouse over `store` (beacon's tables object store).
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        let registry = ObjectStoreRegistry::default();
        registry.insert(
            SCHEME,
            Arc::new(BeaconTablesProvider {
                inner: store.clone(),
            }),
        );
        // Cache sizes mirror Lance's classic entry-count defaults; they affect
        // only caching, not correctness.
        let session = Arc::new(Session::new(128, 128, Arc::new(registry)));
        Self {
            store,
            session,
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// The Lance session (carries the object-store registry) for open/write.
    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }

    /// The tables object store, for `DROP` cleanup.
    pub fn store(&self) -> &Arc<DynObjectStore> {
        &self.store
    }

    /// The `beacon-tables://` URI of a managed table:
    /// `beacon-tables:///lance/<namespace>/<name>.lance`.
    pub fn table_uri(&self, namespace: &[String], name: &str) -> String {
        let ns = namespace.join("/");
        format!("{SCHEME}:///{PREFIX}/{ns}/{name}.lance")
    }

    /// The object-store key prefix for a table URI (everything after the scheme
    /// authority), used to enumerate a dataset's objects on `DROP`.
    pub fn object_path(uri: &str) -> object_store::path::Path {
        let parsed = Url::parse(uri).ok();
        let path = parsed
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_default();
        object_store::path::Path::from(path)
    }

    /// The write lock guarding a given dataset URI (created on first use).
    pub fn lock(&self, uri: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock();
        locks
            .entry(uri.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}
