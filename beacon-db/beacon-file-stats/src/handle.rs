//! Reaching the file-statistics store from a DataFusion session.
//!
//! The store is put on the session as an extension at startup, and read back
//! here by whatever needs it during planning. It lives in this crate so the
//! table providers can reach it without this crate depending on any of them.
//!
//! Pruning itself used to live here too, as `prune_scan`, rewriting the file
//! list of a plan that had already been built. `FastObjectTable` is the only
//! provider that prunes now, and it carries its own copy in
//! `beacon_datafusion_ext::fast_object::prune`, so the second implementation
//! was removed rather than left to drift.

use std::sync::{Arc, OnceLock};

use datafusion::catalog::Session;

use crate::store::FileStatsStore;

/// Shared, late-filled handle to the store.
///
/// The store is built by the runtime, after the session it needs, so the handle
/// is registered empty as a session-config extension and filled once the store
/// exists. Empty means no pruning, which is always a correct answer.
pub type FileStatsHandle = Arc<OnceLock<Arc<FileStatsStore>>>;

/// Create an empty handle to register as a session extension.
pub fn new_file_stats_handle() -> FileStatsHandle {
    Arc::new(OnceLock::new())
}

/// The store this session prunes against, if it has one.
pub fn try_file_stats_from_session(session: &dyn Session) -> Option<Arc<FileStatsStore>> {
    session
        .config()
        .get_extension::<OnceLock<Arc<FileStatsStore>>>()?
        .get()
        .cloned()
}
