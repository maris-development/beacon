//! Column statistics for each object version that Beacon can read, for file pruning.
//!
//! The pipeline has one stage for each job:
//!
//! ```text
//! discovery -> queue -> process -> sink -> store <- provider
//! ```
//!
//! | Stage | Module | Job | Gives |
//! |---|---|---|---|
//! | discovery | [`discovery`] | List each store on an interval. Queue the object versions the store does not have. Remove gone versions. | `AnalyzeTask` |
//! | queue | [`queue`] | Hold the tasks, each [`Stamp`] once, with a fixed capacity. | `AnalyzeTask` |
//! | process | `process` | Read the statistics of each task through the format of its extension. | `FileRecord` |
//! | sink | `sink` | Collect records, and store each batch in one transaction. | `Vec<FileRecord>` |
//! | store | [`store`] | Keep column statistics by stamp, in redb tables. | `StampStatistics` |
//! | provider | `provider` | Give pruning statistics for a list of objects to a table scan. | `PruningStatistics` |
//!
//! A [`Stamp`] is a stable hash of one object version: its store url, location,
//! modification time and size. A changed file has a new stamp, so it has no
//! statistics until the new version is analyzed, and the scan keeps it.

mod config;
pub mod discovery;
mod process;
mod provider;
pub mod queue;
mod sink;
mod stamp;
pub mod store;

pub use config::Config;
pub use stamp::Stamp;
pub use store::StatisticsStore;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use object_store::ObjectStore;
use redb::Database;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use discovery::{Discovery, StoreWalker};
use process::Analyzer;
use queue::AnalyzeQueue;
use sink::BatchLimits;

/// The file statistics pipeline: walkers, workers, the sink and the store.
///
/// Drop it to stop all its tasks.
pub struct FileStatistics {
    config: Config,
    runtime_handle: tokio::runtime::Handle,
    queue: Arc<AnalyzeQueue>,
    store: Arc<StatisticsStore>,
    discovery: Arc<Discovery>,
    walkers: parking_lot::Mutex<HashMap<ObjectStoreUrl, StoreWalker>>,
    /// The workers and the sink.
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for FileStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStatistics")
            .field("config", &self.config)
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl FileStatistics {
    /// Opens the store and starts the workers and the sink. No store is walked yet.
    ///
    /// # Arguments
    ///
    /// * `config` - The settings of the pipeline.
    /// * `runtime_handle` - The runtime that runs all tasks.
    /// * `session` - The session that the workers read the files through.
    /// * `database` - The redb database that holds the statistics tables.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot be opened.
    pub async fn open(
        config: Config,
        runtime_handle: tokio::runtime::Handle,
        session: SessionCell,
        database: Arc<Database>,
    ) -> anyhow::Result<Self> {
        let store = Arc::new(StatisticsStore::open(database).await?);
        let queue = Arc::new(AnalyzeQueue::new(config.queue_capacity()));
        let discovery = Arc::new(Discovery::new(Arc::clone(&queue), Arc::clone(&store)));

        let (sink_sender, records) = mpsc::channel(config.queue_capacity());
        let mut tasks = Analyzer::new(Arc::clone(&queue), sink_sender, session)
            .start(config.num_threads(), &runtime_handle);
        let limits = BatchLimits {
            size: config.batch_size(),
            age: config.batch_age(),
        };
        tasks.push(sink::spawn_sink(
            records,
            Arc::clone(&store),
            Arc::clone(&queue),
            limits,
            &runtime_handle,
        ));

        Ok(Self {
            config,
            runtime_handle,
            queue,
            store,
            discovery,
            walkers: parking_lot::Mutex::new(HashMap::new()),
            tasks,
        })
    }

    /// Starts a walker for `store`. A walker for the same url stops.
    pub fn register_store(&self, store_url: ObjectStoreUrl, store: Arc<dyn ObjectStore>) {
        let walker = StoreWalker::init(
            store_url.clone(),
            store,
            Arc::clone(&self.discovery),
            self.config.walk_interval(),
            &self.runtime_handle,
        );
        self.walkers.lock().insert(store_url, walker);
    }

    /// Starts a walk of each registered store now.
    pub fn walk_now(&self) {
        for walker in self.walkers.lock().values() {
            walker.walk_now();
        }
    }

    /// The statistics store.
    pub fn store(&self) -> &Arc<StatisticsStore> {
        &self.store
    }
}

impl Drop for FileStatistics {
    fn drop(&mut self) {
        // The walkers stop when the map drops them.
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// A late-filled, weak reference to the session.
///
/// Weak, because the session owns the runtime that owns the pipeline. A strong
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
