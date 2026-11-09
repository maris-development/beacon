//! Beacon's side of the file statistics store.
//!
//! [`beacon_file_stats`] stores per-file column ranges and knows no file
//! format. This crate connects it to the rest of Beacon:
//!
//! | Module | Holds |
//! |---|---|
//! | [`analyzer`] | [`Analyzer`], whose workers read the statistics of queued files. |
//! | [`file_format_statistics`] | [`FileFormatStatistics`], which reads the statistics of one file. |
//! | [`config`] | [`Config`], the settings of the subsystem. |
//! | [`index`] | [`Index`], the known objects and the version of each. |
//! | [`store_walker`] | The walkers that queue new and changed objects of each store. |
//!
//! The flow: store walker, then [`Index`], then the analyzer queue, then the
//! [`Analyzer`] workers, then the recorder, which updates the [`Index`].

pub mod analyzer;
pub mod config;
pub mod file_format_statistics;
pub mod stamp;
pub mod state;
pub mod store;
pub mod store_walker;

pub use file_format_statistics::{FileFormatStatistics, FileStatisticsOutput};

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use beacon_external_table::statistics_cache::{
    ExternalTableStatisticsProvider, SharedPruningStatistics,
};
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

use futures::prelude::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};

use crate::config::Config;
use crate::store_walker::StoreWalker;

/// The file statistics subsystem: store walkers, analyzer workers and the recorder.
///
/// Drop it to stop all its tasks.
pub struct FileStatistics {
    config: Config,
    runtime_handle: tokio::runtime::Handle,
    walkers: parking_lot::Mutex<HashMap<ObjectStoreUrl, StoreWalker>>,
}

impl FileStatistics {
    /// Starts the analyzer workers and the recorder. No store is walked yet.
    ///
    /// # Arguments
    ///
    /// * `config` - The settings of the subsystem.
    /// * `runtime_handle` - The runtime that runs all tasks.
    /// * `session` - The session that the analyzer reads the files through.
    ///
    /// # Errors
    ///
    /// Returns no error now. The `Result` keeps room for a store that fails to open.
    pub fn new(
        config: Config,
        runtime_handle: tokio::runtime::Handle,
        session: SessionCell,
    ) -> anyhow::Result<Self> {
        todo!()
    }

    /// Starts a walker for `store`. A walker for the same url stops.
    ///
    /// # Errors
    ///
    /// Returns no error now.
    pub fn register_store(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        todo!()
    }
}

impl ExternalTableStatisticsProvider for FileStatistics {
    fn is_stale(&self) -> BoxFuture<'static, bool> {
        todo!()
    }

    fn fetch_statistics(
        &self,
        store_url: ObjectStoreUrl,
        objects: &[ObjectMeta],
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
        schema: SchemaRef,
    ) -> BoxFuture<'static, anyhow::Result<(Vec<ObjectMeta>, SharedPruningStatistics)>> {
        todo!()
    }

    fn invalidate_objects(
        &self,
        store_url: ObjectStoreUrl,
        _store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        todo!()
    }

    fn register_store(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        self.register_store(store_url, store)
    }
}

/// A late-filled, weak reference to the session.
///
/// Weak, because the session owns the runtime that owns the service. A strong
/// reference forms a cycle that keeps the database file open.
pub type SessionCell = Arc<OnceLock<Weak<SessionContext>>>;

/// The session in `cell`, or an error that names `who` when it is gone.
///
/// # Errors
///
/// Returns an error when the cell is empty or the session was dropped.
pub fn upgrade_session(cell: &SessionCell, who: &str) -> anyhow::Result<Arc<SessionContext>> {
    cell.get()
        .and_then(Weak::upgrade)
        .ok_or_else(|| anyhow::anyhow!("{who}: beacon session context is unavailable"))
}
