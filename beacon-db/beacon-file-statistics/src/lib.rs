//! File statistics for external tables, for file pruning.
//!
//! An external table that sets `use_statistics_pruning` registers with
//! [`FileStatistics`] on each build and each refresh. The engine keeps one Lance
//! statistics table for each registered table, with one row for each file:
//!
//! | Columns | Hold |
//! |---|---|
//! | `_stamp`, `_path`, `_size`, `_last_modified` | The file version. |
//! | `_failed`, `_row_count` | The outcome of the analysis. |
//! | One struct column for each statistics column | `min`, `max` and `null_count`, typed like the table column. |
//!
//! | Module | Job |
//! |---|---|
//! | `layout` | The statistics schema of a table, the column choice and the 1,000-column limit. |
//! | `analyze` | The statistics of one file, cast to the table types. |
//! | `rows` | The record batch of analyzed files. |
//! | `table` | Open, refresh (only new, changed and deleted files), compact, read. |
//! | `read` | Pruning statistics for the files of one scan. |
//!
//! A refresh runs when a table registers, on the refresh interval of the table,
//! and on [`FileStatistics::request_refresh`]. The stamp of a file is a hash of
//! its store url, path, mtime and size: a file that changed has a new stamp, so
//! the scan keeps it until the next refresh analyzes it again.

mod analyze;
mod config;
mod format;
mod layout;
mod read;
mod rows;
mod stamp;
mod table;

pub use config::Config;
pub use layout::{LayoutError, MAX_STATISTICS_COLUMNS};
pub use table::RefreshSummary;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use beacon_external_table::statistics_cache::{
    ExternalTableStatisticsProvider, SharedPruningStatistics, StatisticsRegistration,
};
use beacon_lance::LanceWarehouse;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use object_store::ObjectMeta;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use table::TableStatistics;

/// A late-filled, weak reference to the session.
///
/// Weak, because the session owns the runtime that owns the engine. A strong
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

/// The file statistics engine: one statistics table and one refresh task for each registered table.
///
/// It is cheap to clone: each clone shares the registered tables. Drop the
/// last clone to stop all refresh tasks.
#[derive(Clone)]
pub struct FileStatistics {
    inner: Arc<Inner>,
}

struct Inner {
    config: Config,
    runtime: tokio::runtime::Handle,
    session: SessionCell,
    warehouse: Arc<LanceWarehouse>,
    /// The analyses that run now, over all tables.
    analysis: Arc<Semaphore>,
    /// The tables that refresh now. A table waits here for a free slot.
    refreshes: Arc<Semaphore>,
    tables: Mutex<HashMap<String, Registered>>,
}

/// A registered table and its refresh task.
struct Registered {
    statistics: Arc<TableStatistics>,
    /// Wakes the refresh task for a refresh now.
    requests: Arc<Notify>,
    task: JoinHandle<()>,
}

/// What [`FileStatistics::request_refresh`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshRequest {
    /// No refresh of the table ran. One starts when a refresh slot is free.
    Started,
    /// A refresh of the table runs now. A new one starts after it.
    Queued,
}

impl RefreshRequest {
    /// The lowercase name: `started` or `queued`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Queued => "queued",
        }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for FileStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStatistics")
            .field("config", &self.inner.config)
            .field("tables", &self.inner.tables.lock().len())
            .finish_non_exhaustive()
    }
}

impl FileStatistics {
    /// Creates the engine. No table is registered yet.
    ///
    /// # Arguments
    ///
    /// * `config` - The settings of the engine.
    /// * `runtime` - The runtime that runs the refresh tasks.
    /// * `session` - The session that the refreshes list and read the files through.
    /// * `warehouse` - The Lance warehouse that holds the statistics tables.
    pub fn new(
        config: Config,
        runtime: tokio::runtime::Handle,
        session: SessionCell,
        warehouse: Arc<LanceWarehouse>,
    ) -> Self {
        let analysis = Arc::new(Semaphore::new(config.concurrency()));
        let refreshes = Arc::new(Semaphore::new(config.max_refreshes()));
        Self {
            inner: Arc::new(Inner {
                config,
                runtime,
                session,
                warehouse,
                analysis,
                refreshes,
                tables: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Refreshes the statistics of `table` when a refresh slot is free, and waits for the end.
    ///
    /// [`Self::request_refresh`] does not wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is not registered, the session is gone,
    /// or the refresh fails.
    pub async fn refresh(&self, table: &str) -> anyhow::Result<RefreshSummary> {
        let statistics = self.statistics(table)?;
        let session = upgrade_session(&self.inner.session, "file statistics refresh")?;
        // The engine never closes the semaphore, so the acquire succeeds.
        let _slot = self.inner.refreshes.acquire().await;
        statistics
            .refresh(session, &self.inner.analysis, &self.inner.config)
            .await
    }

    /// Asks the refresh task of `table` for a refresh now, and returns at once.
    ///
    /// The refresh runs in the background, in the refresh slots of the engine.
    /// The requests during one refresh give one more refresh, not one each.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is not registered.
    pub fn request_refresh(&self, table: &str) -> anyhow::Result<RefreshRequest> {
        self.with_registered(table, |registered| {
            let request = if registered.statistics.is_refreshing() {
                RefreshRequest::Queued
            } else {
                RefreshRequest::Started
            };
            registered.requests.notify_one();
            request
        })
    }

    /// The names of the registered tables, sorted.
    pub fn tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = self.inner.tables.lock().keys().cloned().collect();
        tables.sort_unstable();
        tables
    }

    /// The statistics table of `table` as a table provider, for SQL.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is not registered.
    pub fn statistics_table(&self, table: &str) -> anyhow::Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.statistics(table)?.provider()))
    }

    /// The statistics of the registered table `table`.
    fn statistics(&self, table: &str) -> anyhow::Result<Arc<TableStatistics>> {
        self.with_registered(table, |registered| Arc::clone(&registered.statistics))
    }

    /// Applies `f` to the registered table `table`, under the lock of the tables.
    fn with_registered<T>(
        &self,
        table: &str,
        f: impl FnOnce(&Registered) -> T,
    ) -> anyhow::Result<T> {
        self.inner
            .tables
            .lock()
            .get(table)
            .map(f)
            .with_context(|| format!("the table '{table}' has no statistics"))
    }

    /// Starts the task that refreshes `statistics` now, then on its interval and on each request.
    ///
    /// The task holds the statistics weakly. A drop of the engine then releases
    /// the statistics table, and the database file under its warehouse, at once,
    /// not when the runtime gets to the aborted task.
    fn spawn_refresh(
        &self,
        statistics: &Arc<TableStatistics>,
        requests: Arc<Notify>,
    ) -> JoinHandle<()> {
        let session = Arc::clone(&self.inner.session);
        let analysis = Arc::clone(&self.inner.analysis);
        let refreshes = Arc::clone(&self.inner.refreshes);
        let config = self.inner.config.clone();
        let table = statistics.registration().table.clone();
        let interval = statistics.registration().refresh_interval;
        let statistics = Arc::downgrade(statistics);
        self.inner.runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let requested = tokio::select! {
                    _ = ticker.tick() => false,
                    () = requests.notified() => true,
                };
                let Some(statistics) = statistics.upgrade() else {
                    return;
                };
                // At startup the tables register before the session is ready.
                let session = loop {
                    match upgrade_session(&session, "file statistics refresh") {
                        Ok(session) => break session,
                        Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
                    }
                };
                // Only `max_refreshes` tables refresh at a time; the others wait here.
                let slot = refreshes.acquire().await;
                let refreshed = statistics.refresh(session, &analysis, &config).await;
                drop(slot);
                // The next periodic refresh is one interval after this one.
                ticker.reset();
                match refreshed {
                    Ok(summary) if requested => tracing::info!(
                        table = table.as_str(),
                        listed = summary.listed,
                        analyzed = summary.analyzed,
                        removed = summary.removed,
                        "refreshed the file statistics on request"
                    ),
                    Ok(summary) => tracing::debug!(
                        table = table.as_str(),
                        listed = summary.listed,
                        analyzed = summary.analyzed,
                        removed = summary.removed,
                        "refreshed the file statistics"
                    ),
                    Err(error) => tracing::warn!(
                        table = table.as_str(),
                        error = format!("{error:#}"),
                        "a file statistics refresh failed; the next refresh tries again"
                    ),
                }
            }
        })
    }
}

impl ExternalTableStatisticsProvider for FileStatistics {
    /// Opens or creates the statistics table, and starts its refresh task.
    ///
    /// A table that registers again gets a new task, and keeps its rows when its
    /// statistics columns did not change.
    fn register_table(
        &self,
        registration: StatisticsRegistration,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        let engine = self.clone();
        Box::pin(async move {
            let table = registration.table.clone();
            let statistics = Arc::new(
                TableStatistics::open(Arc::clone(&engine.inner.warehouse), registration).await?,
            );
            let requests = Arc::new(Notify::new());
            let task = engine.spawn_refresh(&statistics, Arc::clone(&requests));
            let registered = Registered {
                statistics,
                requests,
                task,
            };
            // The replaced entry drops here, which stops its task.
            engine.inner.tables.lock().insert(table, registered);
            Ok(())
        })
    }

    fn drop_table(&self, table: &str) -> BoxFuture<'static, anyhow::Result<()>> {
        let removed = self.inner.tables.lock().remove(table);
        drop(removed);
        let warehouse = Arc::clone(&self.inner.warehouse);
        let table = table.to_string();
        Box::pin(async move { table::drop_statistics(&warehouse, &table).await })
    }

    fn fetch_statistics(
        &self,
        table: &str,
        objects: &[&ObjectMeta],
        schema: SchemaRef,
    ) -> BoxFuture<'static, anyhow::Result<SharedPruningStatistics>> {
        let statistics = self.statistics(table);
        // The stamps are computed now, because the future cannot hold `objects`.
        let stamps: Option<Vec<u64>> = statistics.as_ref().ok().map(|statistics| {
            let store_url = &statistics.registration().store_url;
            objects
                .iter()
                .map(|object| stamp::stamp(store_url, object))
                .collect()
        });
        Box::pin(async move {
            let statistics = statistics?;
            let stamps = stamps.unwrap_or_default();
            let read = statistics.fetch(&stamps, &schema).await?;
            Ok(Arc::new(read) as SharedPruningStatistics)
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
