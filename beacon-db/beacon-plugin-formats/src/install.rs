//! Installs the formats of a plugin in a session, and removes them again.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use beacon_plugin_sdk::file_format::{FFI_FileFormatFactory, import_format_factory};
use datafusion::common::plan_err;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::listing_table_factory::ListingTableFactory;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use tokio::runtime::Handle;

use crate::factory::PluginFileFormatFactory;
use crate::table_function::PluginReadFunc;

/// Imports the factories that a plugin library exports. `handle` is the runtime that
/// runs the store I/O of the plugin.
pub fn import_formats(
    exported: Vec<FFI_FileFormatFactory>,
    handle: Handle,
) -> Result<Vec<PluginFileFormatFactory>> {
    exported
        .into_iter()
        .map(|ffi| import_format_factory(ffi, handle.clone()).map(PluginFileFormatFactory::new))
        .collect()
}

/// The functions that a format adds: `read_<name>` and `read_<name>_schema`.
pub fn function_names(format: &PluginFileFormatFactory) -> [String; 2] {
    [
        format!("read_{}", format.name()),
        format!("read_{}_schema", format.name()),
    ]
}

fn valid_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A key that an uninstalled plugin format holds is free again.
fn is_free(factory: &Arc<dyn FileFormatFactory>) -> bool {
    let any: &dyn Any = factory.as_ref();
    any.downcast_ref::<PluginFileFormatFactory>()
        .is_some_and(|plugin| !plugin.is_installed())
}

/// Checks the names of `formats` against each other and against `state`.
///
/// A name or an extension must be lower case `[a-z0-9_]+`. No key (name or extension)
/// and no function (`read_<name>`, `read_<name>_schema`) may already be in use. One failure
/// refuses all formats.
pub fn check_formats(state: &SessionState, formats: &[PluginFileFormatFactory]) -> Result<()> {
    let mut seen = HashSet::new();
    for format in formats {
        let extensions = format.plugin().extensions();
        for text in std::iter::once(format.name().to_string()).chain(extensions) {
            if !valid_name(&text) {
                return plan_err!(
                    "invalid file format name or extension `{text}`: use lower case a-z, 0-9 and _"
                );
            }
        }
        for key in format.keys() {
            let taken = !seen.insert(key.clone())
                || state
                    .get_file_format_factory(&key)
                    .is_some_and(|existing| !is_free(&existing));
            if taken {
                return plan_err!("the file format key `{key}` is already in use");
            }
        }
        for function in function_names(format) {
            if state.table_functions().contains_key(&function) {
                return plan_err!("the function `{function}` is already in use");
            }
        }
    }
    Ok(())
}

/// Registers `formats` in `ctx`: each key for `STORED AS`, and `read_<name>`.
///
/// Returns the `read_<name>` functions, so that the caller can add `read_<name>_schema` over
/// them. Call [`check_formats`] first.
pub fn install_formats(
    ctx: &SessionContext,
    formats: &[PluginFileFormatFactory],
    handle: Handle,
) -> Result<Vec<Arc<PluginReadFunc>>> {
    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        for format in formats {
            for key in format.keys() {
                state.register_file_format(Arc::new(format.with_ext(&key)), true)?;
                // A plain DataFusion session resolves `STORED AS <key>` through this.
                state.table_factories_mut().insert(
                    key.to_ascii_uppercase(),
                    Arc::new(ListingTableFactory::new()),
                );
            }
        }
    }
    let mut readers = Vec::new();
    for format in formats {
        let reader = Arc::new(PluginReadFunc::new(format.clone(), handle.clone()));
        let [read, _] = function_names(format);
        ctx.register_udtf(&read, reader.clone());
        readers.push(reader);
    }
    Ok(readers)
}

/// Removes `formats` from `ctx`. Tables on them fail with "not installed" from now on,
/// and their keys are free for a new install. The caller removes `read_<name>_schema`.
pub fn uninstall_formats(ctx: &SessionContext, formats: &[PluginFileFormatFactory]) {
    for format in formats {
        format.mark_uninstalled();
        let [read, _] = function_names(format);
        ctx.deregister_udtf(&read);
    }
}

/// The plugin formats of a session, by plugin name, so that an uninstall finds them.
/// Register one as a session config extension.
#[derive(Debug, Default)]
pub struct InstalledFormats(Mutex<HashMap<String, Vec<PluginFileFormatFactory>>>);

impl InstalledFormats {
    /// Records the formats of `plugin`.
    pub fn insert(&self, plugin: &str, formats: Vec<PluginFileFormatFactory>) {
        self.lock().insert(plugin.to_string(), formats);
    }

    /// Removes and returns the formats of `plugin`.
    pub fn take(&self, plugin: &str) -> Vec<PluginFileFormatFactory> {
        self.lock().remove(plugin).unwrap_or_default()
    }

    /// The names of the formats of `plugin`.
    pub fn names(&self, plugin: &str) -> Vec<String> {
        self.lock()
            .get(plugin)
            .map(|formats| formats.iter().map(|f| f.name().to_string()).collect())
            .unwrap_or_default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<PluginFileFormatFactory>>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
