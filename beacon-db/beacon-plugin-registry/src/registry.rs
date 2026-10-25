//! The installed plugins of a runtime, and the owner of each plugin function.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::names::PluginFunctionNames;
use parking_lot::Mutex;

use crate::{manifest::PluginKind, store::InstallRecord};

/// Every native library that opened. Never dropped: plugin code may still run.
static LIBRARIES: Mutex<Vec<libloading::Library>> = Mutex::new(Vec::new());

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginStatus {
    Loaded,
    /// The plugin registered nothing; the text is the reason.
    Failed(String),
    Uninstalled,
}

#[derive(Debug, Clone)]
pub struct PluginEntry {
    /// The folder name in the plugins directory.
    pub name: String,
    /// `None` when `install.json` could not be read.
    pub record: Option<InstallRecord>,
    /// `None` when the manifest could not be read.
    pub kind: Option<PluginKind>,
    pub status: PluginStatus,
    pub functions: Vec<String>,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<String, PluginEntry>,
    owners: HashMap<String, String>,
    installing: HashSet<String>,
}

/// Held in the session config, so statement planning can find function owners.
#[derive(Default)]
pub struct PluginRegistry {
    state: Mutex<State>,
    native_functions: PluginFunctionNames,
    allowed_sources: Vec<String>,
}

/// Marks an install of one plugin name as running until it drops.
pub struct InstallGuard<'a> {
    registry: &'a PluginRegistry,
    name: String,
}

impl Drop for InstallGuard<'_> {
    fn drop(&mut self) {
        self.registry.state.lock().installing.remove(&self.name);
    }
}

/// Releases claimed function names when it drops, unless [`ClaimGuard::disarm`] runs.
pub struct ClaimGuard<'a> {
    registry: &'a PluginRegistry,
    plugin: String,
    functions: Vec<String>,
    armed: bool,
}

impl ClaimGuard<'_> {
    /// Keeps the claim: the install committed.
    pub fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.release_claim(&self.plugin, &self.functions);
        }
    }
}

impl PluginRegistry {
    /// Guards a claim of `functions` for `plugin`, so a failed or cancelled install releases it.
    pub fn claim_guard(&self, plugin: &str, functions: Vec<String>) -> ClaimGuard<'_> {
        ClaimGuard {
            registry: self,
            plugin: plugin.to_string(),
            functions,
            armed: true,
        }
    }

    /// A registry that allows remote sources with these text prefixes.
    pub fn with_allowed_sources(allowed_sources: Vec<String>) -> Self {
        Self {
            allowed_sources,
            ..Self::default()
        }
    }

    /// The allowed remote source prefixes. Empty allows only `file://`.
    pub fn allowed_sources(&self) -> &[String] {
        &self.allowed_sources
    }

    /// Keeps `library` loaded until the process stops.
    pub fn keep_library(&self, library: libloading::Library) {
        LIBRARIES.lock().push(library);
    }

    /// The names and aliases of the loaded native functions, for the federation rule.
    pub fn native_function_names(&self) -> PluginFunctionNames {
        self.native_functions.clone()
    }

    /// Marks `names` as native functions, which never go to a remote Beacon.
    pub fn add_native_functions(&self, names: Vec<String>) {
        self.native_functions.insert_all(names);
    }

    /// The plugin that owns `function`, if any.
    pub fn owner_of(&self, function: &str) -> Option<String> {
        self.state.lock().owners.get(function).cloned()
    }

    /// Every entry, in name order.
    pub fn snapshot(&self) -> Vec<PluginEntry> {
        self.state.lock().entries.values().cloned().collect()
    }

    /// Reserves `name` for an install. Only an absent or uninstalled name is free.
    pub fn begin_install(&self, name: &str) -> anyhow::Result<InstallGuard<'_>> {
        let mut state = self.state.lock();
        let occupied = state.installing.contains(name)
            || state
                .entries
                .get(name)
                .is_some_and(|entry| entry.status != PluginStatus::Uninstalled);
        anyhow::ensure!(!occupied, "plugin '{name}' is already installed");
        state.installing.insert(name.to_string());
        Ok(InstallGuard {
            registry: self,
            name: name.to_string(),
        })
    }

    /// Reserves a loaded or failed plugin for an uninstall.
    pub fn begin_uninstall(&self, name: &str) -> anyhow::Result<InstallGuard<'_>> {
        let mut state = self.state.lock();
        let installed = state
            .entries
            .get(name)
            .is_some_and(|entry| entry.status != PluginStatus::Uninstalled);
        anyhow::ensure!(installed, "plugin '{name}' is not installed");
        anyhow::ensure!(
            !state.installing.contains(name),
            "an install or uninstall of plugin '{name}' is running"
        );
        state.installing.insert(name.to_string());
        Ok(InstallGuard {
            registry: self,
            name: name.to_string(),
        })
    }

    /// Every function name and alias that `plugin` owns, in name order.
    pub fn owned_names(&self, plugin: &str) -> Vec<String> {
        let state = self.state.lock();
        let mut names: Vec<String> = state
            .owners
            .iter()
            .filter(|(_, owner)| *owner == plugin)
            .map(|(function, _)| function.clone())
            .collect();
        names.sort();
        names
    }

    /// Lets `names` go to a remote Beacon again.
    pub fn remove_native_functions(&self, names: &[String]) {
        self.native_functions.remove_all(names);
    }

    /// Marks plugin `name` as uninstalled. Its name is free for a new install.
    pub fn set_uninstalled(&self, name: &str) {
        if let Some(entry) = self.state.lock().entries.get_mut(name) {
            entry.status = PluginStatus::Uninstalled;
            entry.functions.clear();
        }
    }

    /// Takes ownership of all `functions` for `plugin`, or of none when one has an owner.
    pub fn try_claim(&self, plugin: &str, functions: &[String]) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        if let Some((function, owner)) = functions
            .iter()
            .find_map(|function| state.owners.get(function).map(|owner| (function, owner)))
        {
            anyhow::bail!(
                "function '{function}' of plugin '{plugin}' collides with a function of plugin '{owner}'"
            );
        }
        for function in functions {
            state.owners.insert(function.clone(), plugin.to_string());
        }
        Ok(())
    }

    /// Gives up the functions that `plugin` owns among `functions`.
    pub fn release_claim(&self, plugin: &str, functions: &[String]) {
        let mut state = self.state.lock();
        for function in functions {
            if state
                .owners
                .get(function)
                .is_some_and(|owner| owner == plugin)
            {
                state.owners.remove(function);
            }
        }
    }

    /// Records a loaded plugin. Its functions must be claimed already.
    pub fn add_loaded(&self, record: InstallRecord, kind: PluginKind, functions: Vec<String>) {
        let mut state = self.state.lock();
        state.entries.insert(
            record.name.clone(),
            PluginEntry {
                name: record.name.clone(),
                record: Some(record),
                kind: Some(kind),
                status: PluginStatus::Loaded,
                functions,
            },
        );
    }

    /// Records a plugin that registered nothing.
    pub fn add_failed(
        &self,
        name: String,
        record: Option<InstallRecord>,
        kind: Option<PluginKind>,
        reason: String,
    ) {
        self.state.lock().entries.insert(
            name.clone(),
            PluginEntry {
                name,
                record,
                kind,
                status: PluginStatus::Failed(reason),
                functions: Vec::new(),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> InstallRecord {
        InstallRecord {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            source: "test".to_string(),
            sha256: String::new(),
            installed_at: String::new(),
        }
    }

    #[test]
    fn a_running_install_occupies_the_name() {
        let registry = PluginRegistry::default();
        let guard = registry.begin_install("demo").unwrap();
        let err = registry.begin_install("demo").err().unwrap().to_string();
        assert_eq!(err, "plugin 'demo' is already installed");
        drop(guard);
        registry.begin_install("demo").unwrap();
    }

    #[test]
    fn loaded_and_failed_plugins_occupy_the_name() {
        let registry = PluginRegistry::default();
        registry.add_loaded(record("a"), PluginKind::Sql, vec!["fa".to_string()]);
        registry.add_failed("b".to_string(), Some(record("b")), None, "bad".to_string());
        assert!(registry.begin_install("a").is_err());
        assert!(registry.begin_install("b").is_err());
        assert!(registry.begin_install("c").is_ok());
        let names: Vec<_> = registry.snapshot().into_iter().map(|e| e.name).collect();
        assert_eq!(names, ["a", "b"]);
    }

    #[test]
    fn uninstall_frees_the_name_and_the_functions() {
        let registry = PluginRegistry::default();
        let names = vec!["f".to_string(), "f_alias".to_string()];
        let err = registry.begin_uninstall("a").err().unwrap().to_string();
        assert_eq!(err, "plugin 'a' is not installed");
        registry.try_claim("a", &names).unwrap();
        registry.add_native_functions(names.clone());
        registry.add_loaded(record("a"), PluginKind::Native, vec!["f".to_string()]);
        let guard = registry.begin_uninstall("a").unwrap();
        assert!(registry.begin_install("a").is_err());
        assert!(registry.begin_uninstall("a").is_err());
        assert_eq!(registry.owned_names("a"), names);
        registry.remove_native_functions(&names);
        registry.release_claim("a", &names);
        registry.set_uninstalled("a");
        drop(guard);
        assert!(registry.native_function_names().is_empty());
        assert_eq!(registry.owner_of("f_alias"), None);
        let entry = &registry.snapshot()[0];
        assert_eq!(entry.status, PluginStatus::Uninstalled);
        assert!(entry.functions.is_empty());
        assert!(registry.begin_uninstall("a").is_err());
        registry.begin_install("a").unwrap();
    }

    #[test]
    fn a_failed_plugin_can_be_uninstalled() {
        let registry = PluginRegistry::default();
        registry.add_failed("b".to_string(), None, None, "bad".to_string());
        let _guard = registry.begin_uninstall("b").unwrap();
        assert!(registry.owned_names("b").is_empty());
    }

    #[test]
    fn native_names_reach_the_shared_set() {
        let registry = PluginRegistry::default();
        let shared = registry.native_function_names();
        assert!(shared.is_empty());
        registry.add_native_functions(vec!["f".to_string(), "f_alias".to_string()]);
        assert!(shared.contains("f") && shared.contains("f_alias"));
    }

    #[test]
    fn a_claim_guard_releases_unless_disarmed() {
        let registry = PluginRegistry::default();
        let names = vec!["f".to_string(), "g".to_string()];
        registry.try_claim("a", &names).unwrap();
        drop(registry.claim_guard("a", names.clone()));
        assert_eq!(registry.owner_of("f"), None);
        assert_eq!(registry.owner_of("g"), None);

        registry.try_claim("a", &names).unwrap();
        registry.claim_guard("a", names.clone()).disarm();
        assert_eq!(registry.owner_of("f").as_deref(), Some("a"));
        assert_eq!(registry.owner_of("g").as_deref(), Some("a"));
    }

    #[test]
    fn a_claim_takes_all_names_or_none() {
        let registry = PluginRegistry::default();
        let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        registry.try_claim("a", &names(&["f", "g"])).unwrap();
        let err = registry
            .try_claim("b", &names(&["h", "g"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("plugin 'a'"), "{err}");
        assert_eq!(registry.owner_of("g").as_deref(), Some("a"));
        assert_eq!(registry.owner_of("h"), None);
        registry.release_claim("b", &names(&["g"]));
        assert_eq!(registry.owner_of("g").as_deref(), Some("a"));
        registry.release_claim("a", &names(&["f", "g"]));
        assert_eq!(registry.owner_of("f"), None);
        registry.try_claim("b", &names(&["h", "g"])).unwrap();
    }
}
