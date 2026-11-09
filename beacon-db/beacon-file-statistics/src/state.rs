use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::datafusion::statistics;
use crossbeam::queue::ArrayQueue;
use datafusion::common::Statistics;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::analyzer::AnalyzeTask;
use crate::stamp::Stamp;
use crate::store::StatisticsStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryState {
    Queued,
    Analyzed,
    Failed,
}

pub(crate) struct FileStatisticsState {
    state_per_store: Arc<RwLock<HashMap<ObjectStoreUrl, HashMap<Stamp, EntryState>>>>,
    entries_per_store: Arc<RwLock<HashMap<ObjectStoreUrl, HashSet<Stamp>>>>,
    queue: ArrayQueue<AnalyzeTask>,
    statistics_store: Arc<StatisticsStore>,
}

impl FileStatisticsState {
    /// Creates a new `FileStatisticsState`.
    pub fn new(max_queue_size: usize) -> Self {
        todo!()
    }

    /// Tries to enqueue an object for analysis.
    ///
    /// Returns the state of the object.
    /// Returns None if the object could not be enqueued because the queue is full.
    pub fn try_enqueue(&self, store_url: ObjectStoreUrl, object: ObjectMeta) -> Option<EntryState> {
        // Determine the stamp for the object.

        // Check if object in current entries, if already there -> return state [Analyzed]

        // check the stores for the state of the object

        todo!()
    }

    pub fn try_dequeue(&self) -> Option<AnalyzeTask> {
        self.queue.pop()
    }

    pub async fn try_finish(
        &self,
        store_url: ObjectStoreUrl,
        object: ObjectMeta,
        schema: SchemaRef,
        statistics: Statistics,
    ) -> anyhow::Result<()> {
        self.statistics_store
            .write_statistics(Stamp::new(&store_url, &object), schema, statistics)
            .await?;
        todo!()
    }
}
