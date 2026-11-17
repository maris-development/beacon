//! Carries the location rule of Beacon across the plugin boundary.
//!
//! Beacon resolves the `LOCATION` text of a table. The plugin gets a store URL and a path.

use std::ffi::c_void;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_ffi::ffi_option::FFI_Option;
use datafusion_ffi::util::FFI_Result;
use stabby::string::String as SString;
use tokio::runtime::Handle;

use crate::object_store::export::drop_quietly;
use crate::panic_safe::panic_message;
use crate::{ABI_VERSION, sdk_abi_version};

/// A `LOCATION` as an object store and a path in that store.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLocation {
    /// The store, for `runtime_env().object_store`. `None` for a path of the local machine.
    pub store_url: Option<ObjectStoreUrl>,
    /// The path in the store, with glob characters unchanged. No leading `/` in a store.
    pub path: String,
}

/// The rule that maps the `LOCATION` text of a table to a store and a path.
///
/// Beacon implements it. A plugin calls it through `resolve_location`.
pub trait LocationResolver: fmt::Debug + Send + Sync {
    /// Resolves `location`. An error refuses the location.
    fn resolve(&self, location: &str) -> Result<ResolvedLocation>;
}

/// A [`ResolvedLocation`] as FFI data.
#[repr(C)]
pub struct FFI_ResolvedLocation {
    /// The store URL. Empty for a path of the local machine.
    pub store_url: FFI_Option<SString>,
    /// The path in the store.
    pub path: SString,
}

/// An `Arc<dyn LocationResolver>` as an FFI table.
///
/// The first five fields stay at the same place in every version.
#[repr(C)]
pub struct FFI_LocationResolver {
    /// Returns the ABI version of the table. See [`crate::BEACON_PLUGIN_SDK_ABI`].
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
    /// Makes a new table that shares the inner resolver.
    pub clone: unsafe extern "C" fn(resolver: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(resolver: &mut Self),
    /// See [`LocationResolver::resolve`]. The error is the text of the exporter error.
    pub resolve: unsafe extern "C" fn(
        resolver: &Self,
        location: SString,
    ) -> FFI_Result<FFI_ResolvedLocation>,
}

// The exporter state is an `Arc<dyn LocationResolver>` and a `Handle`; both are `Send + Sync`.
unsafe impl Send for FFI_LocationResolver {}
unsafe impl Sync for FFI_LocationResolver {}

struct ResolverPrivateData {
    resolver: Arc<dyn LocationResolver>,
    handle: Handle,
}

impl FFI_LocationResolver {
    /// Exports `resolver`. Each call runs inside `handle`.
    pub fn new(resolver: Arc<dyn LocationResolver>, handle: Handle) -> Self {
        let private_data = Box::new(ResolverPrivateData { resolver, handle });
        Self {
            version: sdk_abi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn,
            release: release_fn,
            resolve: resolve_fn,
        }
    }

    fn data(&self) -> &ResolverPrivateData {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const ResolverPrivateData) }
    }
}

impl Clone for FFI_LocationResolver {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_LocationResolver {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

unsafe extern "C" fn clone_fn(resolver: &FFI_LocationResolver) -> FFI_LocationResolver {
    let cloned = catch_unwind(AssertUnwindSafe(|| {
        let data = resolver.data();
        FFI_LocationResolver::new(Arc::clone(&data.resolver), data.handle.clone())
    }));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(resolver: &mut FFI_LocationResolver) {
    if resolver.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(resolver.private_data as *mut ResolverPrivateData) };
    resolver.private_data = std::ptr::null_mut();
    drop_quietly(move || drop(data));
}

unsafe extern "C" fn resolve_fn(
    resolver: &FFI_LocationResolver,
    location: SString,
) -> FFI_Result<FFI_ResolvedLocation> {
    let found = catch_unwind(AssertUnwindSafe(|| {
        let data = resolver.data();
        let _guard = data.handle.enter();
        data.resolver.resolve(location.as_str())
    }));
    let message = match found {
        Ok(Ok(found)) => {
            return FFI_Result::Ok(FFI_ResolvedLocation {
                store_url: found
                    .store_url
                    .map(|url| SString::from(url.as_str()))
                    .into(),
                path: SString::from(found.path.as_str()),
            });
        }
        Ok(Err(error)) => error.to_string(),
        Err(payload) => format!(
            "panic in location resolver: {}",
            panic_message(payload.as_ref())
        ),
    };
    FFI_Result::Err(SString::from(message.as_str()))
}

/// A [`LocationResolver`] over an [`FFI_LocationResolver`] from another library.
pub struct ForeignLocationResolver(FFI_LocationResolver);

impl fmt::Debug for ForeignLocationResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignLocationResolver")
            .finish_non_exhaustive()
    }
}

impl TryFrom<FFI_LocationResolver> for Arc<dyn LocationResolver> {
    type Error = DataFusionError;

    /// Returns the original resolver when this library made the table.
    ///
    /// Fails when the table has another [`crate::BEACON_PLUGIN_SDK_ABI`].
    fn try_from(table: FFI_LocationResolver) -> Result<Self> {
        if (table.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
            return Ok(Arc::clone(&table.data().resolver));
        }
        // SAFETY: `version` has the same place in every table version.
        let version = unsafe { (table.version)() };
        if version != ABI_VERSION {
            return Err(DataFusionError::Plan(format!(
                "location resolver FFI version {version} is not supported (want {ABI_VERSION})"
            )));
        }
        Ok(Arc::new(ForeignLocationResolver(table)))
    }
}

impl LocationResolver for ForeignLocationResolver {
    fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
        // SAFETY: the live table has this version and matches its pointers.
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
}

#[cfg(test)]
pub(crate) mod tests {
    use datafusion::common::plan_err;
    use tokio::runtime::Handle;

    use super::*;
    use crate::object_store::tests::foreign_marker_id;

    /// A host rule for tests: a relative text is a path in `datasets://`.
    #[derive(Debug)]
    pub(crate) struct TestResolver;

    impl LocationResolver for TestResolver {
        fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
            if location == "boom" {
                panic!("boom resolver");
            }
            if location.starts_with("bad") {
                return plan_err!("the host refuses {location}");
            }
            let store_url = match location.starts_with('/') {
                true => None,
                false => Some(ObjectStoreUrl::parse("datasets://")?),
            };
            Ok(ResolvedLocation {
                store_url,
                path: location.to_string(),
            })
        }
    }

    extern "C" fn next_version() -> u64 {
        crate::ABI_VERSION + 1
    }

    fn handle() -> Handle {
        crate::table_type::tests::test_handle()
    }

    /// Exports the test rule and imports it through the foreign path.
    fn foreign_resolver() -> Arc<dyn LocationResolver> {
        let mut ffi = FFI_LocationResolver::new(Arc::new(TestResolver), handle());
        ffi.library_marker_id = foreign_marker_id;
        Arc::<dyn LocationResolver>::try_from(ffi).expect("import the resolver")
    }

    #[test]
    fn a_location_resolves_on_the_host() {
        let found = foreign_resolver().resolve("obs/part_*.lance").unwrap();
        assert_eq!(found.store_url.unwrap().as_str(), "datasets:///");
        assert_eq!(found.path, "obs/part_*.lance");
    }

    #[test]
    fn a_location_without_a_store_crosses() {
        let found = foreign_resolver().resolve("/data/x").unwrap();
        assert_eq!(found.store_url, None);
        assert_eq!(found.path, "/data/x");
    }

    #[test]
    fn a_host_error_crosses() {
        let error = foreign_resolver().resolve("bad/x").unwrap_err().to_string();
        assert!(error.contains("the host refuses bad/x"), "{error}");
    }

    #[test]
    fn a_panic_in_the_host_rule_is_an_error() {
        let error = foreign_resolver().resolve("boom").unwrap_err().to_string();
        assert!(error.contains("boom resolver"), "{error}");
    }

    #[test]
    fn the_local_marker_returns_the_original_resolver() {
        let resolver: Arc<dyn LocationResolver> = Arc::new(TestResolver);
        let ffi = FFI_LocationResolver::new(Arc::clone(&resolver), handle());
        let back = Arc::<dyn LocationResolver>::try_from(ffi).unwrap();
        assert!(Arc::ptr_eq(&resolver, &back));
    }

    #[test]
    fn another_version_is_refused() {
        let mut ffi = FFI_LocationResolver::new(Arc::new(TestResolver), handle());
        ffi.library_marker_id = foreign_marker_id;
        ffi.version = next_version;
        let error = Arc::<dyn LocationResolver>::try_from(ffi)
            .unwrap_err()
            .to_string();
        let want = format!(
            "location resolver FFI version {} is not supported (want {})",
            crate::ABI_VERSION + 1,
            crate::ABI_VERSION
        );
        assert!(error.contains(&want), "{error}");
    }
}
