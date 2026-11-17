//! Carries a read-only `ObjectStoreRegistry` across the plugin boundary.
//!
//! The table exports `get_store` only. The receiver cannot change the exporter registry.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};

use ::object_store::ObjectStore;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion_ffi::util::FFI_Result;
use stabby::string::String as SString;
use tokio::runtime::Handle;
use url::{Position, Url};

use super::export::{FFI_ObjectStore, drop_quietly};
use crate::panic_safe::panic_message;
use crate::{ABI_VERSION, sdk_abi_version};

/// An `Arc<dyn ObjectStoreRegistry>` as a read-only FFI table.
///
/// The exporter builds it with [`FFI_ObjectStoreRegistry::new`]. The receiver turns it into
/// an `Arc<dyn ObjectStoreRegistry>` with `TryFrom`. The first five fields stay at the same
/// place in every version.
#[repr(C)]
pub struct FFI_ObjectStoreRegistry {
    /// Returns the ABI version of the table. See [`crate::BEACON_PLUGIN_SDK_ABI`].
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
    /// Makes a new table that shares the inner registry.
    pub clone: unsafe extern "C" fn(registry: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(registry: &mut Self),
    /// See `ObjectStoreRegistry::get_store`. The error is the text of the exporter error.
    pub get_store:
        unsafe extern "C" fn(registry: &Self, url: SString) -> FFI_Result<FFI_ObjectStore>,
}

// The exporter state is an `Arc<dyn ObjectStoreRegistry>` and a `Handle`; both are `Send + Sync`.
unsafe impl Send for FFI_ObjectStoreRegistry {}
unsafe impl Sync for FFI_ObjectStoreRegistry {}

struct RegistryPrivateData {
    registry: Arc<dyn ObjectStoreRegistry>,
    handle: Handle,
}

impl FFI_ObjectStoreRegistry {
    /// Exports `registry`. Each exported store runs its I/O inside `handle`.
    pub fn new(registry: Arc<dyn ObjectStoreRegistry>, handle: Handle) -> Self {
        let private_data = Box::new(RegistryPrivateData { registry, handle });
        Self {
            version: sdk_abi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn,
            release: release_fn,
            get_store: get_store_fn,
        }
    }

    fn data(&self) -> &RegistryPrivateData {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const RegistryPrivateData) }
    }
}

impl Clone for FFI_ObjectStoreRegistry {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_ObjectStoreRegistry {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

unsafe extern "C" fn clone_fn(registry: &FFI_ObjectStoreRegistry) -> FFI_ObjectStoreRegistry {
    let cloned = catch_unwind(AssertUnwindSafe(|| {
        let data = registry.data();
        FFI_ObjectStoreRegistry::new(Arc::clone(&data.registry), data.handle.clone())
    }));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(registry: &mut FFI_ObjectStoreRegistry) {
    if registry.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(registry.private_data as *mut RegistryPrivateData) };
    registry.private_data = std::ptr::null_mut();
    drop_quietly(move || drop(data));
}

unsafe extern "C" fn get_store_fn(
    registry: &FFI_ObjectStoreRegistry,
    url: SString,
) -> FFI_Result<FFI_ObjectStore> {
    let found = catch_unwind(AssertUnwindSafe(|| {
        let data = registry.data();
        let url = Url::parse(url.as_str()).map_err(|error| error.to_string())?;
        // A lazy registry can build the store here.
        let _guard = data.handle.enter();
        let store = data
            .registry
            .get_store(&url)
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(FFI_ObjectStore::new(store, data.handle.clone()))
    }));
    match found {
        Ok(Ok(store)) => FFI_Result::Ok(store),
        Ok(Err(message)) => FFI_Result::Err(SString::from(message.as_str())),
        Err(payload) => FFI_Result::Err(SString::from(
            format!(
                "panic in object store registry: {}",
                panic_message(payload.as_ref())
            )
            .as_str(),
        )),
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

type Stores = Mutex<HashMap<String, Arc<dyn ObjectStore>>>;

/// An `ObjectStoreRegistry` over an [`FFI_ObjectStoreRegistry`] from another library.
///
/// `get_store` reads the exporter registry. `register_store` and `deregister_store` change
/// only this value, and a registered store hides the exporter store of the same URL.
pub struct ForeignObjectStoreRegistry {
    table: FFI_ObjectStoreRegistry,
    /// The stores that this side registered.
    own: Stores,
    /// The imported stores of the exporter.
    imported: Stores,
}

impl fmt::Debug for ForeignObjectStoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignObjectStoreRegistry")
            .finish_non_exhaustive()
    }
}

impl TryFrom<FFI_ObjectStoreRegistry> for Arc<dyn ObjectStoreRegistry> {
    type Error = DataFusionError;

    /// Returns the original registry when this library made the table.
    ///
    /// Fails when the table has another [`crate::BEACON_PLUGIN_SDK_ABI`].
    fn try_from(table: FFI_ObjectStoreRegistry) -> Result<Self> {
        if (table.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
            return Ok(Arc::clone(&table.data().registry));
        }
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (table.version)() };
        if version != ABI_VERSION {
            return Err(DataFusionError::Plan(format!(
                "object store registry FFI version {version} is not supported (want {ABI_VERSION})"
            )));
        }
        Ok(Arc::new(ForeignObjectStoreRegistry {
            table,
            own: Mutex::default(),
            imported: Mutex::default(),
        }))
    }
}

fn lock(stores: &Stores) -> std::sync::MutexGuard<'_, HashMap<String, Arc<dyn ObjectStore>>> {
    stores.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ObjectStoreRegistry for ForeignObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        lock(&self.own).insert(store_key(url), store)
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = store_key(url);
        lock(&self.own).remove(&key).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the plugin registered no object store for {key}, and a Beacon store cannot be removed"
            ))
        })
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = store_key(url);
        if let Some(store) = lock(&self.own).get(&key) {
            return Ok(Arc::clone(store));
        }
        if let Some(store) = lock(&self.imported).get(&key) {
            return Ok(Arc::clone(store));
        }
        // SAFETY: the live table has this version and matches its pointers.
        let found = unsafe { (self.table.get_store)(&self.table, SString::from(url.as_str())) };
        let table = match found {
            FFI_Result::Ok(table) => table,
            FFI_Result::Err(message) => {
                return Err(DataFusionError::Execution(message.as_str().to_string()));
            }
        };
        let store = Arc::<dyn ObjectStore>::try_from(table)
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        Ok(Arc::clone(lock(&self.imported).entry(key).or_insert(store)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::object_store::memory::InMemory;
    use ::object_store::path::Path;
    use ::object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
    use tokio::runtime::Handle;
    use url::Url;

    use super::*;
    use crate::object_store::tests::foreign_marker_id;

    extern "C" fn next_version() -> u64 {
        crate::ABI_VERSION + 1
    }

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    /// Exports `inner` and imports it through the foreign path.
    fn foreign_registry(inner: Arc<dyn ObjectStoreRegistry>) -> Arc<dyn ObjectStoreRegistry> {
        let mut ffi = FFI_ObjectStoreRegistry::new(inner, Handle::current());
        ffi.library_marker_id = foreign_marker_id;
        Arc::<dyn ObjectStoreRegistry>::try_from(ffi).expect("import the registry")
    }

    async fn host() -> Arc<dyn ObjectStoreRegistry> {
        let store = InMemory::new();
        store
            .put(&Path::from("a/x.txt"), PutPayload::from_static(b"hi"))
            .await
            .unwrap();
        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(&url("datasets://"), Arc::new(store));
        Arc::new(registry)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_store_reads_the_host_store() {
        let registry = foreign_registry(host().await);
        let store = registry.get_store(&url("datasets://")).unwrap();
        let bytes = store.get(&Path::from("a/x.txt")).await.unwrap();
        assert_eq!(bytes.bytes().await.unwrap().as_ref(), b"hi");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_scheme_returns_the_host_error() {
        let host = host().await;
        let want = host.get_store(&url("nope://x")).unwrap_err().to_string();
        let registry = foreign_registry(host);
        let error = registry.get_store(&url("nope://x")).unwrap_err().to_string();
        assert!(error.contains(&want), "{error} does not contain {want}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_store_stays_in_the_plugin() {
        let host = host().await;
        let registry = foreign_registry(Arc::clone(&host));
        let own: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            registry
                .register_store(&url("mem://own"), Arc::clone(&own))
                .is_none()
        );
        let back = registry.get_store(&url("mem://own/some/path")).unwrap();
        assert!(Arc::ptr_eq(&own, &back));
        assert!(host.get_store(&url("mem://own")).is_err());
        let removed = registry.deregister_store(&url("mem://own")).unwrap();
        assert!(Arc::ptr_eq(&own, &removed));
        assert!(registry.get_store(&url("mem://own")).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_plugin_store_hides_the_host_store() {
        let registry = foreign_registry(host().await);
        let own: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        registry.register_store(&url("datasets://"), Arc::clone(&own));
        let back = registry.get_store(&url("datasets://")).unwrap();
        assert!(Arc::ptr_eq(&own, &back));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_store_caches_the_import() {
        let registry = foreign_registry(host().await);
        let first = registry.get_store(&url("datasets://")).unwrap();
        let second = registry.get_store(&url("datasets:///a/x.txt")).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_marker_returns_the_original_registry() {
        let host = host().await;
        let ffi = FFI_ObjectStoreRegistry::new(Arc::clone(&host), Handle::current());
        let back = Arc::<dyn ObjectStoreRegistry>::try_from(ffi).unwrap();
        assert!(Arc::ptr_eq(&host, &back));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wrong_version_is_refused() {
        let mut ffi = FFI_ObjectStoreRegistry::new(host().await, Handle::current());
        ffi.library_marker_id = foreign_marker_id;
        ffi.version = next_version;
        let error = Arc::<dyn ObjectStoreRegistry>::try_from(ffi)
            .unwrap_err()
            .to_string();
        let want = format!(
            "object store registry FFI version {} is not supported (want {})",
            crate::ABI_VERSION + 1,
            crate::ABI_VERSION
        );
        assert!(error.contains(&want), "{error}");
    }

    #[derive(Debug)]
    struct PanicRegistry;

    impl ObjectStoreRegistry for PanicRegistry {
        fn register_store(
            &self,
            _url: &Url,
            _store: Arc<dyn ObjectStore>,
        ) -> Option<Arc<dyn ObjectStore>> {
            None
        }

        fn get_store(&self, _url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
            panic!("boom registry");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_panic_in_get_store_is_an_error() {
        let registry = foreign_registry(Arc::new(PanicRegistry));
        let error = registry.get_store(&url("datasets://")).unwrap_err();
        assert!(error.to_string().contains("boom registry"), "{error}");
    }
}
