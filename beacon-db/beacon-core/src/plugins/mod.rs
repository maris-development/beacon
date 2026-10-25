pub(crate) mod loader;

pub use beacon_plugin_registry::manifest::{
    beacon_version, NativeSection, PluginKind, PluginManifest, SqlSection, MANIFEST_FILE,
};
pub use beacon_plugin_registry::registry::{PluginEntry, PluginRegistry, PluginStatus};
pub use beacon_plugin_registry::store::InstallRecord;
pub use beacon_plugin_registry::{archive, manifest, native, registry, source, store};

use beacon_datafusion_ext::remote::LocalFunctions;
use beacon_plugin_registry::names::PluginFunctionNames;

/// The loaded native plugin functions, which never go to a remote Beacon.
#[derive(Debug)]
pub(crate) struct NativePluginFunctions(pub(crate) PluginFunctionNames);

impl LocalFunctions for NativePluginFunctions {
    fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
