//! Imports the table types of a plugin, checks their names, and keeps them per session.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use beacon_plugin_formats::PluginFileFormatFactory;
use beacon_plugin_sdk::table_type::{FFI_PluginHost, FFI_PluginTableType, import_table_type};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use parking_lot::Mutex;

use crate::location::DatasetsLocationResolver;
use tokio::runtime::Handle;
use url::Url;

/// The `STORED AS` names that Beacon itself handles.
pub const BUILT_IN_TABLE_TYPES: &[&str] = &[
    "remote",
    "delta",
    "iceberg",
    "icechunk",
    "erddap",
    "postgres",
    "postgresql",
    "mysql",
    "odbc",
];

/// What Beacon gives to the table types of each plugin. Register one as a session config
/// extension.
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
    /// The host value for one plugin, with `registry` as read-only FFI data.
    pub fn ffi_host(&self, registry: Arc<dyn ObjectStoreRegistry>) -> FFI_PluginHost {
        FFI_PluginHost::new(
            registry,
            self.runtime.clone(),
            Arc::new(DatasetsLocationResolver::new(self.datasets_url.clone())),
            self.datasets_url.as_ref(),
            self.datasets_base_url.as_ref(),
        )
    }
}

/// A table type of a plugin: the `STORED AS` name and the imported factory.
#[derive(Debug, Clone)]
pub struct PluginTableType {
    name: String,
    factory: Arc<dyn TableProviderFactory>,
    installed: Arc<AtomicBool>,
}

impl PluginTableType {
    /// The `STORED AS` name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The factory, as `datafusion-ffi` imports it.
    pub fn factory(&self) -> &Arc<dyn TableProviderFactory> {
        &self.factory
    }

    /// False after the plugin is uninstalled.
    pub fn is_installed(&self) -> bool {
        self.installed.load(Ordering::Acquire)
    }

    pub(crate) fn installed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.installed)
    }

    #[cfg(test)]
    pub(crate) fn new(name: &str, factory: Arc<dyn TableProviderFactory>) -> Self {
        Self {
            name: name.to_string(),
            factory,
            installed: Arc::new(AtomicBool::new(true)),
        }
    }
}

/// Imports the table types that a plugin library exports.
pub fn import_table_types(exported: Vec<FFI_PluginTableType>) -> Result<Vec<PluginTableType>> {
    exported
        .into_iter()
        .map(|ffi| {
            let (name, factory) = import_table_type(ffi)?;
            Ok(PluginTableType {
                name,
                factory,
                installed: Arc::new(AtomicBool::new(true)),
            })
        })
        .collect()
}

fn valid_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// True when a file format of `state` holds `name`. An uninstalled plugin format holds nothing.
fn is_file_format(state: &SessionState, name: &str) -> bool {
    match state.get_file_format_factory(name) {
        Some(factory) => {
            let any: &dyn Any = factory.as_ref();
            any.downcast_ref::<PluginFileFormatFactory>()
                .is_none_or(PluginFileFormatFactory::is_installed)
        }
        None => state
            .table_factories()
            .contains_key(&name.to_ascii_uppercase()),
    }
}

/// Checks the names of `types` against each other, the built-in types, the file formats
/// of `state` and the types in `installed`. One failure refuses all types.
pub fn check_table_types(
    state: &SessionState,
    installed: &InstalledTableTypes,
    types: &[PluginTableType],
) -> Result<()> {
    let mut seen = HashSet::new();
    for table_type in types {
        let name = table_type.name();
        if !valid_name(name) {
            return plan_err!("invalid table type name `{name}`: use lower case a-z, 0-9 and _");
        }
        if !seen.insert(name) {
            return plan_err!("the plugin exports the table type `{name}` twice");
        }
        if BUILT_IN_TABLE_TYPES.contains(&name) {
            return plan_err!("the table type `{name}` is a built-in table type");
        }
        if is_file_format(state, name) {
            return plan_err!("the table type `{name}` is already in use by a file format");
        }
        if let Some((plugin, _)) = installed.find(name) {
            return plan_err!("the table type `{name}` is already in use by plugin '{plugin}'");
        }
    }
    Ok(())
}

/// The plugin table types of a session, by plugin name. Register one as a session config
/// extension.
#[derive(Debug, Default)]
pub struct InstalledTableTypes(Mutex<HashMap<String, Vec<PluginTableType>>>);

impl InstalledTableTypes {
    /// Records the types of `plugin`.
    pub fn insert(&self, plugin: &str, types: Vec<PluginTableType>) {
        self.0.lock().insert(plugin.to_string(), types);
    }

    /// Removes and returns the types of `plugin`. Their tables fail from now on.
    pub fn take(&self, plugin: &str) -> Vec<PluginTableType> {
        let types = self.0.lock().remove(plugin).unwrap_or_default();
        for table_type in &types {
            table_type.installed.store(false, Ordering::Release);
        }
        types
    }

    /// Finds the type `name`, without case. Returns the plugin name and the type.
    pub fn find(&self, name: &str) -> Option<(String, PluginTableType)> {
        self.0.lock().iter().find_map(|(plugin, types)| {
            types
                .iter()
                .find(|table_type| table_type.name.eq_ignore_ascii_case(name))
                .map(|table_type| (plugin.clone(), table_type.clone()))
        })
    }

    /// The type names of `plugin`.
    pub fn names(&self, plugin: &str) -> Vec<String> {
        self.0
            .lock()
            .get(plugin)
            .map(|types| types.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }
}
