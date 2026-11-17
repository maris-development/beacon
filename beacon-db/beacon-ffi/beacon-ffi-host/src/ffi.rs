//! Carries a [`HostApi`] across the plugin boundary.
//!
//! This module holds all FFI code of the host. Beacon exports its host with
//! [`FFI_Host::new`]. The plugin imports it with `Arc::<dyn HostApi>::try_from`.

use std::ffi::c_void;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_ffi::ffi_option::FFI_Option;
use datafusion_ffi::util::FFI_Result;
use object_store::ObjectStore;
use stabby::string::String as SString;
use tokio::runtime::Handle;
use url::Url;

use super::{HostApi, ResolvedLocation};
use beacon_ffi_abi::drop_quietly;
use beacon_ffi_abi::panic_message;
use beacon_ffi_abi::{ABI_VERSION, sdk_abi_version};
use beacon_ffi_object_store::FFI_ObjectStore;

/// A [`ResolvedLocation`] as FFI data.
#[repr(C)]
pub struct FFI_ResolvedLocation {
    /// The store URL. Empty for a path of the local machine.
    pub store_url: FFI_Option<SString>,
    /// The path in the store.
    pub path: SString,
}

/// An `Arc<dyn HostApi>` as an FFI table. It has no function that changes the host.
///
/// The first five fields stay at the same place in every version.
#[repr(C)]
pub struct FFI_Host {
    /// Returns the ABI version of the table. See [`beacon_ffi_abi::BEACON_PLUGIN_SDK_ABI`].
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
    /// Makes a new table that shares the inner host.
    pub clone: unsafe extern "C" fn(host: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(host: &mut Self),
    /// See [`HostApi::object_store`]. The error is the text of the exporter error.
    pub object_store:
        unsafe extern "C" fn(host: &Self, url: SString) -> FFI_Result<FFI_ObjectStore>,
    /// See [`HostApi::resolve`]. The error is the text of the exporter error.
    pub resolve:
        unsafe extern "C" fn(host: &Self, location: SString) -> FFI_Result<FFI_ResolvedLocation>,
    /// See [`HostApi::datasets_url`].
    pub datasets_url: unsafe extern "C" fn(host: &Self) -> FFI_Option<SString>,
    /// See [`HostApi::datasets_base_url`].
    pub datasets_base_url: unsafe extern "C" fn(host: &Self) -> FFI_Option<SString>,
}

// The exporter state is an `Arc<dyn HostApi>` and a `Handle`; both are `Send + Sync`.
unsafe impl Send for FFI_Host {}
unsafe impl Sync for FFI_Host {}

struct HostPrivateData {
    api: Arc<dyn HostApi>,
    handle: Handle,
}

impl FFI_Host {
    /// Exports `api`. Each call, and the I/O of each exported store, runs inside `handle`.
    pub fn new(api: Arc<dyn HostApi>, handle: Handle) -> Self {
        let private_data = Box::new(HostPrivateData { api, handle });
        Self {
            version: sdk_abi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn,
            release: release_fn,
            object_store: object_store_fn,
            resolve: resolve_fn,
            datasets_url: datasets_url_fn,
            datasets_base_url: datasets_base_url_fn,
        }
    }

    fn data(&self) -> &HostPrivateData {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const HostPrivateData) }
    }
}

impl Clone for FFI_Host {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_Host {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

/// Runs `work` inside the exporter runtime. A panic becomes the error text.
fn guarded<T>(host: &FFI_Host, work: impl FnOnce(&dyn HostApi) -> Result<T>) -> Result<T, String> {
    let done = catch_unwind(AssertUnwindSafe(|| {
        let data = host.data();
        let _guard = data.handle.enter();
        work(data.api.as_ref())
    }));
    match done {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(payload) => Err(format!(
            "panic in the Beacon host: {}",
            panic_message(payload.as_ref())
        )),
    }
}

fn ffi_result<T>(result: Result<T, String>) -> FFI_Result<T> {
    match result {
        Ok(value) => FFI_Result::Ok(value),
        Err(message) => FFI_Result::Err(SString::from(message.as_str())),
    }
}

unsafe extern "C" fn clone_fn(host: &FFI_Host) -> FFI_Host {
    let cloned = catch_unwind(AssertUnwindSafe(|| {
        let data = host.data();
        FFI_Host::new(Arc::clone(&data.api), data.handle.clone())
    }));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(host: &mut FFI_Host) {
    if host.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(host.private_data as *mut HostPrivateData) };
    host.private_data = std::ptr::null_mut();
    drop_quietly(move || drop(data));
}

unsafe extern "C" fn object_store_fn(host: &FFI_Host, url: SString) -> FFI_Result<FFI_ObjectStore> {
    ffi_result(guarded(host, |api| {
        let url = Url::parse(url.as_str()).map_err(|error| {
            DataFusionError::Plan(format!("invalid store URL `{url}`: {error}"))
        })?;
        let store = api.object_store(&url)?;
        Ok(FFI_ObjectStore::new(store, host.data().handle.clone()))
    }))
}

unsafe extern "C" fn resolve_fn(
    host: &FFI_Host,
    location: SString,
) -> FFI_Result<FFI_ResolvedLocation> {
    ffi_result(guarded(host, |api| {
        let found = api.resolve(location.as_str())?;
        Ok(FFI_ResolvedLocation {
            store_url: found
                .store_url
                .map(|url| SString::from(url.as_str()))
                .into(),
            path: SString::from(found.path.as_str()),
        })
    }))
}

unsafe extern "C" fn datasets_url_fn(host: &FFI_Host) -> FFI_Option<SString> {
    // A panic gives no URL: this call has no error value.
    guarded(host, |api| Ok(api.datasets_url()))
        .ok()
        .flatten()
        .map(|url| SString::from(url.as_str()))
        .into()
}

unsafe extern "C" fn datasets_base_url_fn(host: &FFI_Host) -> FFI_Option<SString> {
    // A panic gives no URL: this call has no error value.
    guarded(host, |api| Ok(api.datasets_base_url()))
        .ok()
        .flatten()
        .map(|url| SString::from(url.as_str()))
        .into()
}

/// A [`HostApi`] over an [`FFI_Host`] from another library. Each call crosses the table.
pub struct ForeignHost(FFI_Host);

impl fmt::Debug for ForeignHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignHost").finish_non_exhaustive()
    }
}

impl TryFrom<FFI_Host> for Arc<dyn HostApi> {
    type Error = DataFusionError;

    /// Returns the original host when this library made the table.
    ///
    /// Fails when the table has another [`beacon_ffi_abi::BEACON_PLUGIN_SDK_ABI`].
    fn try_from(table: FFI_Host) -> Result<Self> {
        if (table.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
            return Ok(Arc::clone(&table.data().api));
        }
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (table.version)() };
        if version != ABI_VERSION {
            return Err(DataFusionError::Plan(format!(
                "plugin host FFI version {version} is not supported (want {ABI_VERSION})"
            )));
        }
        Ok(Arc::new(ForeignHost(table)))
    }
}

// SAFETY for each call below: the live table has this version and matches its pointers.
impl HostApi for ForeignHost {
    fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let found = unsafe { (self.0.object_store)(&self.0, SString::from(url.as_str())) };
        match found {
            FFI_Result::Ok(store) => Arc::<dyn ObjectStore>::try_from(store)
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error))),
            FFI_Result::Err(message) => {
                Err(DataFusionError::Execution(message.as_str().to_string()))
            }
        }
    }

    fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
        let found = unsafe { (self.0.resolve)(&self.0, SString::from(location)) };
        let found = match found {
            FFI_Result::Ok(found) => found,
            FFI_Result::Err(message) => {
                return Err(DataFusionError::Plan(message.as_str().to_string()));
            }
        };
        let store_url = found
            .store_url
            .into_option()
            .map(|url| ObjectStoreUrl::parse(url.as_str()))
            .transpose()?;
        Ok(ResolvedLocation {
            store_url,
            path: found.path.as_str().to_string(),
        })
    }

    fn datasets_url(&self) -> Option<ObjectStoreUrl> {
        let url = unsafe { (self.0.datasets_url)(&self.0) }.into_option()?;
        ObjectStoreUrl::parse(url.as_str()).ok()
    }

    fn datasets_base_url(&self) -> Option<Url> {
        let url = unsafe { (self.0.datasets_base_url)(&self.0) }.into_option()?;
        Url::parse(url.as_str()).ok()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::ObjectStoreRegistry;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;
    use crate::testing::{BASE_URL, TestHost, foreign_table, test_handle};

    extern "C" fn next_version() -> u64 {
        ABI_VERSION + 1
    }

    fn foreign(host: Arc<TestHost>) -> Arc<dyn HostApi> {
        Arc::<dyn HostApi>::try_from(foreign_table(host)).expect("import the host")
    }

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    fn read(store: &Arc<dyn ObjectStore>, path: &str) -> object_store::Result<bytes::Bytes> {
        futures::executor::block_on(async { store.get(&Path::from(path)).await?.bytes().await })
    }

    #[test]
    fn object_store_reads_the_host_store() {
        let host = foreign(TestHost::new());
        let store = host.object_store(&url("datasets://")).unwrap();
        assert_eq!(read(&store, "a/x.txt").unwrap().as_ref(), b"hi");
    }

    #[test]
    fn an_unknown_scheme_returns_the_host_error() {
        let inner = TestHost::new();
        let want = inner
            .object_store(&url("nope://x"))
            .unwrap_err()
            .to_string();
        let error = foreign(inner)
            .object_store(&url("nope://x"))
            .unwrap_err()
            .to_string();
        assert!(error.contains(&want), "{error} does not contain {want}");
    }

    #[test]
    fn a_store_that_the_host_replaces_is_seen() {
        let inner = TestHost::new();
        let host = foreign(Arc::clone(&inner));
        let first = host.object_store(&url("datasets://")).unwrap();
        assert!(read(&first, "a/x.txt").is_ok());
        // The host puts an empty store at the same URL.
        inner
            .stores
            .register_store(&url("datasets://"), Arc::new(InMemory::new()));
        let second = host.object_store(&url("datasets://")).unwrap();
        assert!(read(&second, "a/x.txt").is_err());
    }

    #[test]
    fn a_store_that_the_host_adds_later_is_seen() {
        let inner = TestHost::new();
        let host = foreign(Arc::clone(&inner));
        assert!(host.object_store(&url("late://")).is_err());
        inner
            .stores
            .register_store(&url("late://"), Arc::new(InMemory::new()));
        assert!(host.object_store(&url("late://")).is_ok());
    }

    #[test]
    fn a_location_resolves_on_the_host() {
        let host = foreign(TestHost::new());
        let found = host.resolve("obs/part_*.lance").unwrap();
        assert_eq!(found.store_url.unwrap().as_str(), "datasets:///");
        assert_eq!(found.path, "obs/part_*.lance");
        let found = host.resolve("/data/x").unwrap();
        assert_eq!(found.store_url, None);
        assert_eq!(found.path, "/data/x");
    }

    #[test]
    fn a_host_error_crosses() {
        let error = foreign(TestHost::new())
            .resolve("bad/x")
            .unwrap_err()
            .to_string();
        assert!(error.contains("the host refuses bad/x"), "{error}");
    }

    #[test]
    fn a_panic_in_the_host_is_an_error() {
        let host = foreign(TestHost::new());
        let error = host.resolve("boom").unwrap_err().to_string();
        assert!(error.contains("boom resolve"), "{error}");
        let error = host.object_store(&url("boom://x")).unwrap_err().to_string();
        assert!(error.contains("boom store"), "{error}");
    }

    #[test]
    fn the_urls_cross() {
        let host = foreign(TestHost::new());
        assert_eq!(host.datasets_url().unwrap().as_str(), "datasets:///");
        assert_eq!(host.datasets_base_url().unwrap().as_str(), BASE_URL);
    }

    #[test]
    fn the_local_marker_returns_the_original_host() {
        let inner: Arc<dyn HostApi> = TestHost::new();
        let ffi = FFI_Host::new(Arc::clone(&inner), test_handle());
        let back = Arc::<dyn HostApi>::try_from(ffi).unwrap();
        assert!(Arc::ptr_eq(&inner, &back));
    }

    #[test]
    fn another_version_is_refused() {
        let mut ffi = foreign_table(TestHost::new());
        ffi.version = next_version;
        let error = Arc::<dyn HostApi>::try_from(ffi).unwrap_err().to_string();
        let want = format!(
            "plugin host FFI version {} is not supported (want {})",
            ABI_VERSION + 1,
            ABI_VERSION
        );
        assert!(error.contains(&want), "{error}");
    }
}
