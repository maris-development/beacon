//! The statistics table of one external table: open, refresh and read.
//!
//! A refresh lists the files of the table and compares them with the stored
//! rows. It deletes the rows of changed and deleted files, then analyzes the
//! new and changed files and appends their rows. Rows of unchanged files stay
//! as they are, so a refresh writes only what changed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Schema, UInt64Type};
use beacon_external_table::statistics_cache::StatisticsRegistration;
use beacon_lance::io::{WriteKind, write_stream};
use beacon_lance::{CompactOptions, LanceTable, LanceTableDefinition, LanceWarehouse};
use datafusion::datasource::file_format::FileFormat;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use xxhash_rust::xxh3::xxh3_64;

use crate::analyze::{AnalyzedFile, analysis_format, analyze};
use crate::config::Config;
use crate::layout::{Layout, PATH, STAMP};
use crate::read::{StampStatistics, read};
use crate::rows::record_batch;
use crate::stamp::stamp;

/// The Lance namespace of the statistics tables, apart from the managed tables.
pub(crate) const NAMESPACE: &str = "statistics";

/// The most paths one delete names, so a predicate stays a small SQL text.
const DELETE_CHUNK: usize = 512;

/// How old a replaced version of a statistics table gets before a compaction removes it.
const CLEANUP_AGE: Duration = Duration::from_secs(600);

/// What one refresh did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefreshSummary {
    /// The number of listed files.
    pub listed: usize,
    /// The number of analyzed files: new and changed.
    pub analyzed: usize,
    /// The number of deleted rows: changed and deleted files.
    pub removed: usize,
}

/// The statistics table of one external table.
pub(crate) struct TableStatistics {
    registration: StatisticsRegistration,
    layout: Layout,
    name: String,
    uri: String,
    warehouse: Arc<LanceWarehouse>,
    /// The version that reads see. Only the refresh of this table writes, and it swaps this after each write.
    dataset: RwLock<Arc<Dataset>>,
    /// One refresh at a time.
    refreshing: tokio::sync::Mutex<()>,
}

impl TableStatistics {
    /// Opens the statistics table of `registration`, or creates it.
    ///
    /// A stored table with another layout is dropped and created again, so the
    /// next refresh analyzes each file.
    ///
    /// # Errors
    ///
    /// Returns an error when the registration gives no layout, or Lance fails.
    pub(crate) async fn open(
        warehouse: Arc<LanceWarehouse>,
        registration: StatisticsRegistration,
    ) -> anyhow::Result<Self> {
        let layout = Layout::new(&registration.schema, registration.columns.as_deref())?;
        let name = dataset_name(&registration.table);
        let uri = warehouse.table_uri(&[NAMESPACE.to_string()], &name);
        let dataset = match load(&warehouse, &uri).await {
            Ok(dataset) if layout.matches(&Schema::from(dataset.schema())) => dataset,
            Ok(_) => {
                tracing::info!(
                    table = registration.table.as_str(),
                    "the statistics columns changed; the statistics table is created again"
                );
                beacon_lance::drop_lance_table(&warehouse, &uri).await?;
                create(&warehouse, &name, &uri, &layout).await?
            }
            Err(lance::Error::DatasetNotFound { .. }) => {
                create(&warehouse, &name, &uri, &layout).await?
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            registration,
            layout,
            name,
            uri,
            warehouse,
            dataset: RwLock::new(Arc::new(dataset)),
            refreshing: tokio::sync::Mutex::new(()),
        })
    }

    /// The registration this table was opened with.
    pub(crate) fn registration(&self) -> &StatisticsRegistration {
        &self.registration
    }

    /// Lists the files of the table, and brings the statistics table up to date with them.
    ///
    /// # Errors
    ///
    /// Returns an error when the listing, a Lance write or the format fails.
    /// The rows written before the error stay, and the next refresh goes on from there.
    pub(crate) async fn refresh(
        &self,
        session: Arc<SessionContext>,
        analysis: &Semaphore,
        config: &Config,
    ) -> anyhow::Result<RefreshSummary> {
        let _refreshing = self.refreshing.lock().await;
        let state = session.state();
        let store_url = &self.registration.store_url;
        let store = state.runtime_env().object_store(store_url)?;

        let listed = self.list(&state, store.as_ref()).await?;
        let stored = self.stored_stamps().await?;
        let mut live: HashSet<&str> = HashSet::with_capacity(listed.len());
        let mut remove: Vec<String> = Vec::new();
        let mut todo: Vec<(ObjectMeta, u64)> = Vec::new();
        for object in &listed {
            let path = object.location.as_ref();
            live.insert(path);
            let version = stamp(store_url, object);
            match stored.get(path) {
                Some(stored) if *stored == version => {}
                Some(_) => {
                    remove.push(path.to_string());
                    todo.push((object.clone(), version));
                }
                None => todo.push((object.clone(), version)),
            }
        }
        remove.extend(
            stored
                .keys()
                .filter(|path| !live.contains(path.as_str()))
                .cloned(),
        );

        let summary = RefreshSummary {
            listed: listed.len(),
            analyzed: todo.len(),
            removed: remove.len(),
        };
        if remove.is_empty() && todo.is_empty() {
            return Ok(summary);
        }

        // The old rows go first: a changed file without a row reads as unknown, which keeps it.
        self.delete_paths(&remove).await?;
        if !todo.is_empty() {
            let format = analysis_format(&state, &self.registration)?;
            for chunk in todo.chunks(config.write_batch()) {
                let files = self
                    .analyze_chunk(chunk, &format, &session, &store, analysis, config)
                    .await;
                self.append(record_batch(&self.layout, &files)?).await?;
            }
        }
        self.compact_if_needed(config).await?;
        self.reload().await?;
        Ok(summary)
    }

    /// The files of the table locations, without empty files.
    ///
    /// The listing goes to the store, never to the list-files cache of the
    /// session, which can hold a list from before the last change.
    async fn list(
        &self,
        state: &datafusion::execution::SessionState,
        store: &dyn ObjectStore,
    ) -> anyhow::Result<Vec<ObjectMeta>> {
        let ignore_subdirectory = state
            .config_options()
            .execution
            .listing_table_ignore_subdirectory;
        let mut listed: Vec<ObjectMeta> = Vec::new();
        // An owned url for each listing keeps the borrows of the stream simple.
        for url in self.registration.urls.clone() {
            let prefix = url.prefix().clone();
            let objects: Vec<ObjectMeta> = if url.is_collection() {
                store.list(Some(&prefix)).try_collect().await?
            } else {
                // A location without a trailing `/` is one file, or a folder without the `/`.
                match store.head(&prefix).await {
                    Ok(object) => vec![object],
                    Err(object_store::Error::NotFound { .. }) => {
                        store.list(Some(&prefix)).try_collect().await?
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            listed.extend(objects.into_iter().filter(|object| {
                object.size > 0 && url.contains(&object.location, ignore_subdirectory)
            }));
        }
        Ok(listed)
    }

    /// Analyzes `chunk`, with at most `config.concurrency()` analyses for this chunk,
    /// and one permit of `analysis` for each running analysis.
    ///
    /// The future is boxed: a stream built from a closure over references makes
    /// the compiler fail to prove `Send` for a spawned caller. A box ends that proof here.
    fn analyze_chunk<'a>(
        &'a self,
        chunk: &'a [(ObjectMeta, u64)],
        format: &'a Arc<dyn FileFormat>,
        session: &'a Arc<SessionContext>,
        store: &'a Arc<dyn ObjectStore>,
        analysis: &'a Semaphore,
        config: &'a Config,
    ) -> BoxFuture<'a, Vec<AnalyzedFile>> {
        let type_widening = self.registration.type_widening.as_ref();
        let layout = &self.layout;
        // Each future owns its inputs, so no future borrows the item of the iterator.
        let analyses: Vec<BoxFuture<'a, AnalyzedFile>> = chunk
            .iter()
            .map(|(object, version)| {
                let (format, session, store) =
                    (Arc::clone(format), Arc::clone(session), Arc::clone(store));
                let (object, version) = (object.clone(), *version);
                async move {
                    // The semaphore stays open for the life of the engine, so the acquire succeeds.
                    let _permit = analysis.acquire().await;
                    analyze(
                        &format,
                        session,
                        store,
                        object,
                        version,
                        layout,
                        type_widening,
                    )
                    .await
                }
                .boxed()
            })
            .collect();
        futures::stream::iter(analyses)
            .buffer_unordered(config.concurrency())
            .collect()
            .boxed()
    }

    /// The stamp of each stored path.
    async fn stored_stamps(&self) -> anyhow::Result<HashMap<String, u64>> {
        let dataset = self.snapshot();
        let mut scanner = dataset.scan();
        scanner.project(&[PATH, STAMP])?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let mut stored = HashMap::new();
        for batch in &batches {
            let (Some(paths), Some(stamps)) =
                (batch.column_by_name(PATH), batch.column_by_name(STAMP))
            else {
                continue;
            };
            let paths = paths.as_string::<i32>();
            let stamps = stamps.as_primitive::<UInt64Type>();
            for (path, stamp) in paths.iter().zip(stamps.values()) {
                if let Some(path) = path {
                    stored.insert(path.to_string(), *stamp);
                }
            }
        }
        Ok(stored)
    }

    /// Deletes the rows of `paths`, in chunks of [`DELETE_CHUNK`].
    async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()> {
        for chunk in paths.chunks(DELETE_CHUNK) {
            // A quote in a path doubles, as SQL needs.
            let quoted: Vec<String> = chunk
                .iter()
                .map(|path| format!("'{}'", path.replace('\'', "''")))
                .collect();
            let predicate = format!("{PATH} IN ({})", quoted.join(", "));
            beacon_lance::delete_rows(&self.warehouse, &self.uri, Some(&predicate)).await?;
        }
        Ok(())
    }

    /// Appends `batch` to the statistics table.
    async fn append(&self, batch: RecordBatch) -> anyhow::Result<()> {
        let schema = batch.schema();
        let stream = RecordBatchStreamAdapter::new(schema, futures::stream::iter([Ok(batch)]));
        let lock = self.warehouse.lock(&self.uri);
        let _guard = lock.lock().await;
        write_stream(
            &self.uri,
            self.warehouse.session(),
            Box::pin(stream),
            WriteKind::Append,
        )
        .await?;
        Ok(())
    }

    /// Merges the fragments when the table has more than `config.compaction_fragments()`.
    async fn compact_if_needed(&self, config: &Config) -> anyhow::Result<()> {
        let fragments = load(&self.warehouse, &self.uri).await?.count_fragments();
        if fragments <= config.compaction_fragments() {
            return Ok(());
        }
        let options = CompactOptions {
            target_rows_per_fragment: None,
            cleanup_older_than: Some(CLEANUP_AGE),
        };
        beacon_lance::compact_table(&self.warehouse, &self.uri, &options).await?;
        Ok(())
    }

    /// Loads the latest version, so the next reads see the last writes.
    async fn reload(&self) -> anyhow::Result<()> {
        let dataset = load(&self.warehouse, &self.uri).await?;
        *self.dataset.write() = Arc::new(dataset);
        Ok(())
    }

    /// Whether a refresh of this table runs now.
    pub(crate) fn is_refreshing(&self) -> bool {
        self.refreshing.try_lock().is_err()
    }

    /// The version that reads see now.
    pub(crate) fn snapshot(&self) -> Arc<Dataset> {
        Arc::clone(&self.dataset.read())
    }

    /// The pruning statistics of `objects` for the columns of `schema`.
    ///
    /// # Errors
    ///
    /// Returns an error when the scan fails.
    pub(crate) async fn fetch(
        &self,
        stamps: &[u64],
        schema: &Schema,
    ) -> anyhow::Result<StampStatistics> {
        read(&self.snapshot(), &self.layout, stamps, schema).await
    }

    /// The statistics table as a table provider, for SQL.
    ///
    /// The provider opens the latest version on each scan, so it does not get old.
    pub(crate) fn provider(&self) -> LanceTable {
        let definition =
            LanceTableDefinition::new(&self.name, vec![NAMESPACE.to_string()], &self.uri);
        let schema = Arc::new(Schema::from(self.snapshot().schema()));
        LanceTable::new(definition, schema, Arc::clone(&self.warehouse))
    }
}

/// Deletes the statistics table of `table`, if there is one.
///
/// # Errors
///
/// Returns an error when Lance cannot delete the files.
pub(crate) async fn drop_statistics(warehouse: &LanceWarehouse, table: &str) -> anyhow::Result<()> {
    let uri = warehouse.table_uri(&[NAMESPACE.to_string()], &dataset_name(table));
    beacon_lance::drop_lance_table(warehouse, &uri)
        .await
        .with_context(|| format!("the statistics of '{table}' were not deleted"))
}

/// The Lance name of the statistics table of `table`.
///
/// The name keeps only `[A-Za-z0-9_]`, and the hash of the full name keeps two
/// names apart that differ only in other characters. A Lance drop fails for a
/// name with a space.
fn dataset_name(table: &str) -> String {
    let readable: String = table
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{readable}-{:016x}", xxh3_64(table.as_bytes()))
}

/// The latest version of the dataset at `uri`.
async fn load(warehouse: &LanceWarehouse, uri: &str) -> lance::Result<Dataset> {
    DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
}

/// Creates the empty statistics table at `uri`, and loads it.
async fn create(
    warehouse: &Arc<LanceWarehouse>,
    name: &str,
    uri: &str,
    layout: &Layout,
) -> anyhow::Result<Dataset> {
    beacon_lance::create_lance_table(
        Arc::clone(warehouse),
        &[NAMESPACE.to_string()],
        name,
        &layout.schema,
    )
    .await?;
    Ok(load(warehouse, uri).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_name_is_safe_and_unique() {
        let name = dataset_name("my table.v2");

        assert!(name.starts_with("my_table_v2-"));
        assert!(!name.contains(' '));
        assert_ne!(name, dataset_name("my_table_v2"));
    }
}
