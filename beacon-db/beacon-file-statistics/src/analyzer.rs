//! The background workers that read the statistics of queued files.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use beacon_datafusion_ext::format_ext::try_file_format_factory_ext;
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use object_store::ObjectMeta;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::file_format_statistics::{FileFormatStatistics, FileStatisticsOutput};
use crate::state::FileStatisticsState;
use crate::{SessionCell, upgrade_session};

/// One file to analyze.
#[derive(Debug, Clone)]
pub(crate) struct AnalyzeTask {
    /// The store that holds the object.
    pub store_url: ObjectStoreUrl,
    /// The object, with a location relative to `store_url`.
    pub object: ObjectMeta,
}

/// The outcome of one [`AnalyzeTask`].
#[derive(Debug)]
pub(crate) struct AnalyzeResult {
    /// The task that gave this result.
    pub task: AnalyzeTask,
    /// The statistics of the file, or the reason the analysis failed.
    pub outcome: anyhow::Result<FileStatisticsOutput>,
}

/// Reads the statistics of queued files with a fixed number of workers.
///
/// The workers share one queue. Each result goes to the result channel.
pub(crate) struct Analyzer {
    state: Arc<FileStatisticsState>,
    session: SessionCell,
    rt: tokio::runtime::Handle,
    num_threads: usize,
}

impl Analyzer {
    /// Creates an analyzer. Call [`Analyzer::start`] to start the workers.
    ///
    /// # Arguments
    ///
    /// * `state` - The state that holds the tasks to analyze.
    /// * `results` - The channel that gets one [`AnalyzeResult`] for each task.
    /// * `session` - The session that holds the format registry and the object stores.
    /// * `rt` - The runtime that runs the workers.
    /// * `num_threads` - The number of workers. Zero gives one worker.
    pub fn new(
        state: Arc<FileStatisticsState>,
        session: SessionCell,
        rt: tokio::runtime::Handle,
        num_threads: usize,
    ) -> Self {
        Self {
            state,
            session,
            rt,
            num_threads: num_threads.max(1),
        }
    }

    /// Starts the workers on the runtime.
    ///
    /// A worker stops when all task senders are dropped, or when the result
    /// receiver is dropped.
    ///
    /// # Returns
    ///
    /// The handle of each worker.
    pub fn start(self) -> Vec<JoinHandle<()>> {
        let analyzer = Arc::new(self);
        (0..analyzer.num_threads)
            .map(|_| analyzer.rt.spawn(Arc::clone(&analyzer).run_worker()))
            .collect()
    }

    /// Takes tasks from the queue until the queue or the result channel closes.
    async fn run_worker(self: Arc<Self>) {
        loop {
            let Some(task) = self.state.try_dequeue() else {
                // Sleep for a short duration before trying again.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            };
            let outcome = self.analyze(&task).await;
            if let Ok(output) = &outcome
                && let Err(err) = self
                    .state
                    .try_finish(
                        task.store_url.clone(),
                        task.object.clone(),
                        output.schema.clone(),
                        output.statistics.clone(),
                    )
                    .await
            {
                tracing::error!(error = %err, "failed to finish analysis for task");
            }
        }
    }

    /// Reads the statistics of one file through the format of its extension.
    async fn analyze(&self, task: &AnalyzeTask) -> anyhow::Result<FileStatisticsOutput> {
        let started = Instant::now();
        let session = upgrade_session(&self.session, "file statistics analyzer")?;
        let (format_name, format) = resolve_format(&session, task)?;
        let store = session.runtime_env().object_store(&task.store_url)?;

        let output = format
            .fetch_statistics(Arc::clone(&session), store, task.object.clone())
            .await;
        tracing::debug!(
            path = %task.object.location,
            format = format_name.as_str(),
            ok = output.is_ok(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "analyzed a file"
        );
        output
    }
}

/// The name and the analysis form of the format for the extension of `task.object`.
///
/// Only `create_for_analysis` gives real statistics. Some formats built in
/// another way give unknown statistics, so that a query never pays for them.
fn resolve_format(
    session: &SessionContext,
    task: &AnalyzeTask,
) -> anyhow::Result<(String, Arc<dyn FileFormat>)> {
    let state = session.state();
    let object = &task.object;
    let key = format_key(object)
        .ok_or_else(|| anyhow::anyhow!("no file extension on {}", object.location))?;
    let factory = try_file_format_factory_ext(&state, &key)
        .ok_or_else(|| anyhow::anyhow!("no format registered for '{key}'"))?;
    let listing = try_listing_factory_from_session(&state)
        .ok_or_else(|| anyhow::anyhow!("the session has no listing factory"))?;
    let url = ListingTableUrl::parse(format!("{}{}", task.store_url.as_str(), object.location))?;
    let format = factory.create_for_analysis(&state, &HashMap::new(), &url, &listing)?;
    Ok((factory.file_format_name(), format))
}

/// The registry key for an object: its extension, or `zarr` for a Zarr metadata file.
fn format_key(object: &ObjectMeta) -> Option<String> {
    let extension = object.location.extension()?;
    if extension == "json"
        && object
            .location
            .filename()
            .is_some_and(|name| name.starts_with("zarr"))
    {
        return Some("zarr".to_string());
    }
    Some(extension.to_string())
}
