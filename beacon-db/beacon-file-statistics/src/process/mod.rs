//! Process: the workers that read the statistics of queued object versions.

mod format;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use beacon_datafusion_ext::format_ext::try_file_format_factory_ext;
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::SessionContext;
use object_store::ObjectMeta;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::queue::{AnalyzeQueue, AnalyzeTask};
use crate::store::FileRecord;
use crate::{SessionCell, upgrade_session};
use format::{FileFormatStatistics, FileStatisticsOutput};

/// Reads the statistics of queued object versions with a fixed number of workers.
pub(crate) struct Analyzer {
    queue: Arc<AnalyzeQueue>,
    sink: Sender<FileRecord>,
    session: SessionCell,
}

impl Analyzer {
    /// Creates the process stage. Call [`Analyzer::start`] to start the workers.
    ///
    /// # Arguments
    ///
    /// * `queue` - The tasks to analyze.
    /// * `sink` - The channel that gets one [`FileRecord`] for each task.
    /// * `session` - The session that holds the format registry and the object stores.
    pub(crate) fn new(
        queue: Arc<AnalyzeQueue>,
        sink: Sender<FileRecord>,
        session: SessionCell,
    ) -> Self {
        Self {
            queue,
            sink,
            session,
        }
    }

    /// Starts `num_threads` workers on `handle`. Zero gives one worker.
    ///
    /// A worker stops when the sink receiver is dropped.
    ///
    /// # Returns
    ///
    /// The handle of each worker.
    pub(crate) fn start(
        self,
        num_threads: usize,
        handle: &tokio::runtime::Handle,
    ) -> Vec<JoinHandle<()>> {
        let analyzer = Arc::new(self);
        (0..num_threads.max(1))
            .map(|_| handle.spawn(Arc::clone(&analyzer).run_worker()))
            .collect()
    }

    /// Takes tasks from the queue until the sink closes.
    async fn run_worker(self: Arc<Self>) {
        loop {
            let task = self.queue.next().await;
            let record = match self.analyze(&task).await {
                Ok(output) => FileRecord::new(task.stamp, &output.schema, &output.statistics),
                // A failed file is stored with no columns, so it is not analyzed again until it changes.
                Err(error) => {
                    tracing::debug!(
                        path = %task.object.location,
                        %error,
                        "the analysis of a file failed; it is stored with no statistics"
                    );
                    FileRecord::failed(task.stamp)
                }
            };
            if self.sink.send(record).await.is_err() {
                return;
            }
        }
    }

    /// Reads the schema and the statistics of one file through the format of its extension.
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

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::OnceLock;

    use arrow::array::{Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
    use beacon_datafusion_ext::format_ext::{FileFormatFactoryExt, FileFormatRegistry};
    use beacon_datafusion_ext::listing_factory::ListingFactory;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionConfig;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use tokio::sync::mpsc;

    use datafusion::common::pruning::PruningStatistics;

    use super::*;
    use crate::stamp::Stamp;

    pub(crate) fn store_url() -> ObjectStoreUrl {
        ObjectStoreUrl::parse("memory://").expect("a valid store url")
    }

    /// A session with the Parquet format and `store` at [`store_url`], and the cell
    /// that points to it. The caller keeps the session alive.
    pub(crate) fn session(store: Arc<dyn ObjectStore>) -> (Arc<SessionContext>, SessionCell) {
        let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![Arc::new(ParquetFormatFactory)];
        let registry = OnceLock::new();
        assert!(registry.set(FileFormatRegistry::new(&formats)).is_ok());
        let config = SessionConfig::new()
            .with_extension(Arc::new(registry))
            .with_extension(Arc::new(ListingFactory::new(None)));
        let ctx = Arc::new(SessionContext::new_with_config(config));
        ctx.register_object_store(store_url().as_ref(), store);

        let cell: SessionCell = Arc::new(OnceLock::new());
        assert!(cell.set(Arc::downgrade(&ctx)).is_ok());
        (ctx, cell)
    }

    /// Writes a Parquet file whose `TEMP` goes from `min` to `min + 10`.
    pub(crate) async fn put_parquet(store: &dyn ObjectStore, path: &str, min: f64) -> ObjectMeta {
        let schema = Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![min, min + 10.0, min + 5.0])),
                Arc::new(Int64Array::from(vec![10, 4000, 250])),
            ],
        )
        .expect("a valid batch");
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("a writer");
        writer.write(&batch).expect("the batch is written");
        writer.close().expect("the file is closed");

        let location = Path::from(path);
        store
            .put(&location, bytes.into())
            .await
            .expect("the file is stored");
        store.head(&location).await.expect("the file exists")
    }

    fn bare_object(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    /// Starts `num_threads` workers and gives the queue and the sink receiver.
    fn start(
        cell: SessionCell,
        num_threads: usize,
    ) -> (
        Arc<AnalyzeQueue>,
        mpsc::Receiver<FileRecord>,
        Vec<JoinHandle<()>>,
    ) {
        let queue = Arc::new(AnalyzeQueue::new(64));
        let (sink, records) = mpsc::channel(64);
        let workers = Analyzer::new(Arc::clone(&queue), sink, cell)
            .start(num_threads, &tokio::runtime::Handle::current());
        (queue, records, workers)
    }

    #[tokio::test]
    async fn a_parquet_file_gives_its_statistics() {
        // Arrange
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_ctx, cell) = session(Arc::clone(&store));
        let object = put_parquet(store.as_ref(), "data/obs.parquet", 3.5).await;
        let task = AnalyzeTask::new(store_url(), object);
        let (queue, mut records, _workers) = start(cell, 1);

        // Act
        queue.enqueue(task.clone()).await;
        let record = records.recv().await.expect("one result");

        // Assert: the record reads back through the store.
        assert_eq!(record.stamp(), task.stamp);
        let stored = crate::store::tests::store().await;
        stored.put(vec![record]).await.expect("stored");
        let temp = Arc::new(Schema::new(vec![Field::new(
            "TEMP",
            DataType::Float64,
            true,
        )]));
        let statistics = stored.get(&temp, &[task.stamp]).await.expect("read");
        let column = datafusion::common::Column::from_name("TEMP");
        let value = |array: Option<arrow::array::ArrayRef>| {
            let array = array.expect("a column");
            arrow::array::AsArray::as_primitive::<arrow::datatypes::Float64Type>(&array).value(0)
        };
        assert_eq!(value(statistics.min_values(&column)), 3.5);
        assert_eq!(value(statistics.max_values(&column)), 13.5);
    }

    #[tokio::test]
    async fn a_bad_file_gives_no_columns_and_the_worker_continues() {
        // Arrange
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_ctx, cell) = session(Arc::clone(&store));
        let good = put_parquet(store.as_ref(), "data/obs.parquet", 0.0).await;
        let (queue, mut records, _workers) = start(cell, 1);

        // Act
        for object in [bare_object("data/noext"), bare_object("data/a.xyz"), good] {
            queue.enqueue(AnalyzeTask::new(store_url(), object)).await;
        }

        // Assert: one worker keeps the order of the queue.
        let mut columns = Vec::new();
        for _ in 0..3 {
            columns.push(records.recv().await.expect("a result").num_columns());
        }
        assert_eq!(columns, [0, 0, 2]);
    }

    #[tokio::test]
    async fn a_bad_file_names_the_reason() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_ctx, cell) = session(store);
        let (sink, _records) = mpsc::channel(1);
        let analyzer = Analyzer::new(Arc::new(AnalyzeQueue::new(1)), sink, cell);
        let reason = |path: &str| {
            let task = AnalyzeTask::new(store_url(), bare_object(path));
            let analyzer = &analyzer;
            async move {
                analyzer
                    .analyze(&task)
                    .await
                    .expect_err("a bad file")
                    .to_string()
            }
        };

        assert!(reason("data/noext").await.contains("no file extension"));
        assert!(
            reason("data/a.xyz")
                .await
                .contains("no format registered for 'xyz'")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_task_gives_one_result_with_many_workers() {
        // Arrange
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_ctx, cell) = session(Arc::clone(&store));
        let mut expected: Vec<Stamp> = Vec::with_capacity(20);
        let (queue, mut records, workers) = start(cell, 4);
        assert_eq!(workers.len(), 4);

        // Act
        for i in 0..20 {
            let object = put_parquet(store.as_ref(), &format!("data/{i}.parquet"), 0.0).await;
            let task = AnalyzeTask::new(store_url(), object);
            expected.push(task.stamp);
            queue.enqueue(task).await;
        }
        let mut seen = Vec::with_capacity(expected.len());
        for _ in 0..expected.len() {
            let record = records.recv().await.expect("a result");
            assert!(record.num_columns() > 0);
            seen.push(record.stamp());
        }

        // Assert
        seen.sort();
        expected.sort();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn workers_stop_when_the_sink_is_dropped() {
        // Arrange: no session, so each task fails at once.
        let (queue, records, workers) = start(Arc::new(OnceLock::new()), 2);
        drop(records);

        // Act: each worker takes one task and finds the sink closed.
        queue
            .enqueue(AnalyzeTask::new(store_url(), bare_object("a.parquet")))
            .await;
        queue
            .enqueue(AnalyzeTask::new(store_url(), bare_object("b.parquet")))
            .await;

        // Assert
        for worker in workers {
            worker.await.expect("the worker stops without a panic");
        }
    }

    #[test]
    fn zarr_metadata_files_resolve_to_the_zarr_format() {
        assert_eq!(
            format_key(&bare_object("a/zarr.json")).as_deref(),
            Some("zarr")
        );
        assert_eq!(
            format_key(&bare_object("a/other.json")).as_deref(),
            Some("json")
        );
        assert_eq!(format_key(&bare_object("a/b.nc")).as_deref(), Some("nc"));
        assert_eq!(format_key(&bare_object("a/noext")), None);
    }
}
