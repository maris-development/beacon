//! Local-filesystem warehouse for beacon-managed Lance tables.
//!
//! Unlike Iceberg, Lance has no external catalog: a Lance dataset *is* a
//! directory on disk, so the path is the source of truth. The
//! [`LanceTableDefinition`](crate::definition::LanceTableDefinition) persists the
//! absolute table location so discovery can reopen the dataset directly.
//!
//! The warehouse is **runtime-scoped**, not a process global: the runtime builds
//! a [`LanceWarehouse`] from its configured tables directory and threads it
//! through the session as an extension. This keeps multiple runtimes in one
//! process (e.g. tests) isolated — each has its own root and write-lock map.
//!
//! Managed Lance tables are **local only** by design: they use raw filesystem
//! paths and never go through beacon's object-store abstraction.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

/// The single namespace beacon creates managed tables under (kept parallel to
/// the Iceberg integration so table layouts are predictable).
pub const BEACON_NAMESPACE: &str = "beacon";

/// The namespace beacon uses, in the `Vec<String>` form the rest of the crate
/// expects (mirrors `beacon_iceberg::beacon_namespace`).
pub fn beacon_namespace() -> Vec<String> {
    vec![BEACON_NAMESPACE.to_string()]
}

/// Lance opens datasets by URI string; a local path is its own URI.
pub fn location_uri(location: &Path) -> String {
    location.to_string_lossy().to_string()
}

/// A runtime-scoped Lance warehouse: the local root directory under which managed
/// tables live, plus per-location write locks.
///
/// Lance's atomic, versioned commits already give readers a consistent snapshot
/// with no torn reads; the per-location lock additionally serializes concurrent
/// *writers* to the same dataset so they don't race on commit.
#[derive(Debug)]
pub struct LanceWarehouse {
    root: PathBuf,
    locks: Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>,
}

impl LanceWarehouse {
    /// Build a warehouse rooted at `root` (e.g. `<tables_dir>/lance`).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// The warehouse root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Absolute on-disk location of a managed table:
    /// `<root>/<namespace>/<name>.lance`.
    pub fn table_location(&self, namespace: &[String], name: &str) -> PathBuf {
        let mut path = self.root.clone();
        for part in namespace {
            path.push(part);
        }
        path.push(format!("{name}.lance"));
        path
    }

    /// The write lock guarding a given table location (created on first use).
    pub fn lock(&self, location: &Path) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock();
        locks
            .entry(location.to_path_buf())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}
