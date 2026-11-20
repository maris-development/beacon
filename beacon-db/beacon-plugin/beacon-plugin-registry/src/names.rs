//! The names of the loaded native plugin functions.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// The names and aliases of the loaded native plugin functions. Clones share one set.
#[derive(Debug, Clone, Default)]
pub struct PluginFunctionNames(Arc<RwLock<HashSet<String>>>);

impl PluginFunctionNames {
    pub fn insert_all(&self, names: impl IntoIterator<Item = String>) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .extend(names);
    }

    pub fn remove_all<'a>(&self, names: impl IntoIterator<Item = &'a String>) {
        let mut set = self.0.write().unwrap_or_else(|e| e.into_inner());
        for name in names {
            set.remove(name);
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(name)
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }
}
