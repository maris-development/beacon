//! Column statistics keyed by stamp, in three redb tables.
//!
//! | Method | Job |
//! |---|---|
//! | [`StatisticsStore::put`] | Store the statistics of analyzed files. |
//! | [`StatisticsStore::contains`] | Tell which stamps the store has. |
//! | [`StatisticsStore::get`] | Give pruning statistics for a list of stamps. |
//! | [`StatisticsStore::retain`] | Remove the stamps of a store that a walk did not see. |
//!
//! A stamp identifies one object version, so its statistics never change. A
//! removal only frees space, and a stamp that the store does not have has no
//! statistics.

mod error;
mod read;
mod record;
mod stat;
mod tables;

pub use error::{Result, StoreError};
pub use read::StampStatistics;
pub use record::FileRecord;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::execution::object_store::ObjectStoreUrl;
use parking_lot::Mutex;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, WriteTransaction,
};

use crate::stamp::Stamp;
use tables::{
    COLUMNS, ColumnId, FILES, STATS, decode_column_id, decode_column_ids, stamp_of, stat_key,
};

/// The longest time a write stays in memory before a write syncs it to disk.
const SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Column statistics for each analyzed object version.
///
/// Most writes do not sync to disk: a write syncs only when the last sync is
/// [`SYNC_INTERVAL`] old. A crash loses at most that much, and the next walk
/// queues those files again.
///
/// It is cheap to clone: each clone shares the database. Each method runs its
/// redb work on a blocking thread.
#[derive(Clone)]
pub struct StatisticsStore {
    database: Arc<Database>,
    last_sync: Arc<Mutex<Instant>>,
}

impl std::fmt::Debug for StatisticsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatisticsStore").finish_non_exhaustive()
    }
}

impl StatisticsStore {
    /// Opens the store in `database`, and creates its tables when they are missing.
    ///
    /// # Errors
    ///
    /// Returns an error when the tables cannot be created.
    pub async fn open(database: Arc<Database>) -> Result<Self> {
        let store = Self {
            database,
            last_sync: Arc::new(Mutex::new(Instant::now())),
        };
        store
            .blocking(|store| {
                // A default transaction syncs, so the tables exist on disk from here on.
                let write = store.database.begin_write()?;
                for table in [COLUMNS, FILES, STATS] {
                    write.open_table(table)?;
                }
                write.commit()?;
                Ok(())
            })
            .await?;
        Ok(store)
    }

    /// Stores `records` in one write transaction.
    ///
    /// A record for a stamp that the store has already overwrites the same values.
    ///
    /// # Errors
    ///
    /// Returns an error when a value does not encode, or the transaction fails.
    pub async fn put(&self, records: Vec<FileRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.blocking(move |store| {
            let write = store.begin_write()?;
            {
                let mut columns = write.open_table(COLUMNS)?;
                let mut files = write.open_table(FILES)?;
                let mut stats = write.open_table(STATS)?;

                let mut ids: HashMap<&str, ColumnId> = HashMap::new();
                let mut next_id = ColumnId::try_from(columns.len()?)
                    .map_err(|_| StoreError::Codec("more than u32::MAX column names".into()))?;
                let mut rows: Vec<([u8; 20], Vec<u8>)> = Vec::new();
                for record in &records {
                    let mut column_ids = Vec::with_capacity(record.columns.len() * 4);
                    for (name, stat) in &record.columns {
                        let id = match ids.get(name.as_str()) {
                            Some(id) => *id,
                            None => {
                                let stored = columns
                                    .get(name.as_bytes())?
                                    .map(|value| decode_column_id(value.value()))
                                    .transpose()?;
                                let id = match stored {
                                    Some(id) => id,
                                    None => {
                                        let id = next_id;
                                        next_id += 1;
                                        columns
                                            .insert(name.as_bytes(), id.to_be_bytes().as_slice())?;
                                        id
                                    }
                                };
                                ids.insert(name.as_str(), id);
                                id
                            }
                        };
                        column_ids.extend_from_slice(&id.to_be_bytes());
                        rows.push((stat_key(id, record.stamp), bincode::serialize(stat)?));
                    }
                    files.insert(record.stamp.to_bytes().as_slice(), column_ids.as_slice())?;
                }
                // Inserts in key order touch each B-tree page once.
                rows.sort_unstable_by_key(|(key, _)| *key);
                for (key, value) in &rows {
                    stats.insert(key.as_slice(), value.as_slice())?;
                }
            }
            write.commit()?;
            Ok(())
        })
        .await
    }

    /// Whether the store has each stamp, in the order of `stamps`.
    ///
    /// # Errors
    ///
    /// Returns an error when the read fails.
    pub async fn contains(&self, stamps: &[Stamp]) -> Result<Vec<bool>> {
        if stamps.is_empty() {
            return Ok(Vec::new());
        }
        let stamps = stamps.to_vec();
        self.blocking(move |store| {
            let read = store.database.begin_read()?;
            let files = read.open_table(FILES)?;
            stamps
                .iter()
                .map(|stamp| Ok(files.get(stamp.to_bytes().as_slice())?.is_some()))
                .collect()
        })
        .await
    }

    /// Removes each stamp of the store at `store_url` that is not in `live`.
    ///
    /// # Returns
    ///
    /// The number of removed stamps.
    ///
    /// # Errors
    ///
    /// Returns an error when a value does not decode, or the transaction fails.
    pub async fn retain(&self, store_url: &ObjectStoreUrl, live: HashSet<Stamp>) -> Result<usize> {
        let (start, end) = Stamp::store_range(store_url);
        self.blocking(move |store| {
            let write = store.begin_write()?;
            let removed = {
                let mut files = write.open_table(FILES)?;
                let mut stats = write.open_table(STATS)?;
                let mut gone = Vec::new();
                for entry in files.range(start.to_bytes().as_slice()..=end.to_bytes().as_slice())? {
                    let stamp = stamp_of(entry?.0.value())?;
                    if !live.contains(&stamp) {
                        gone.push(stamp);
                    }
                }
                for stamp in &gone {
                    let column_ids = match files.remove(stamp.to_bytes().as_slice())? {
                        Some(value) => decode_column_ids(value.value())?,
                        None => continue,
                    };
                    for column_id in column_ids {
                        stats.remove(stat_key(column_id, *stamp).as_slice())?;
                    }
                }
                gone.len()
            };
            // Without a removal there is nothing to commit; the dropped transaction aborts.
            if removed > 0 {
                write.commit()?;
            }
            Ok(removed)
        })
        .await
    }

    /// A write transaction that syncs to disk only when the last sync is [`SYNC_INTERVAL`] old.
    fn begin_write(&self) -> Result<WriteTransaction> {
        let mut write = self.database.begin_write()?;
        let mut last_sync = self.last_sync.lock();
        if last_sync.elapsed() < SYNC_INTERVAL {
            write.set_durability(Durability::None)?;
        } else {
            *last_sync = Instant::now();
        }
        Ok(write)
    }

    /// Runs `work` on a blocking thread, because each redb call blocks.
    async fn blocking<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&StatisticsStore) -> Result<T> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || work(&store)).await?
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::datatypes::DataType;
    use redb::backends::InMemoryBackend;

    use super::*;
    use stat::{ColumnStat, StatScalar};

    /// An empty redb database in memory.
    pub(crate) fn database() -> Arc<Database> {
        Arc::new(
            Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .expect("an in-memory database"),
        )
    }

    /// An empty store in memory.
    pub(crate) async fn store() -> StatisticsStore {
        StatisticsStore::open(database())
            .await
            .expect("the store opens")
    }

    pub(crate) fn url(name: &str) -> ObjectStoreUrl {
        ObjectStoreUrl::parse(format!("{name}://")).expect("a valid store url")
    }

    /// The stamp of file `file` in store `memory`.
    pub(crate) fn stamp(file: u64) -> Stamp {
        let (start, _) = Stamp::store_range(&url("memory"));
        let mut bytes = start.to_bytes();
        bytes[8..].copy_from_slice(&file.to_be_bytes());
        Stamp::from_bytes(bytes)
    }

    pub(crate) fn stat(min: f64) -> ColumnStat {
        ColumnStat {
            min: StatScalar::F64(min),
            max: StatScalar::F64(min + 1.0),
            null_count: Some(0),
            row_count: Some(5),
            data_type: DataType::Float64,
        }
    }

    /// A record with one `TEMP` range for file `file` in store `memory`.
    pub(crate) fn record(file: u64, min: f64) -> FileRecord {
        FileRecord {
            stamp: stamp(file),
            columns: vec![("TEMP".to_string(), stat(min))],
        }
    }

    /// The number of rows in `table`.
    async fn rows(store: &StatisticsStore, table: tables::ByteTable) -> u64 {
        store
            .blocking(move |store| Ok(store.database.begin_read()?.open_table(table)?.len()?))
            .await
            .expect("read")
    }

    #[tokio::test]
    async fn a_put_is_seen_by_contains() {
        let store = store().await;

        store
            .put(vec![record(1, 0.0), FileRecord::failed(stamp(2))])
            .await
            .expect("stored");

        assert_eq!(
            store
                .contains(&[stamp(1), stamp(2), stamp(3)])
                .await
                .expect("read"),
            [true, true, false],
            "a failed analysis counts as stored"
        );
    }

    #[tokio::test]
    async fn retain_removes_only_the_other_stamps_of_its_store() {
        // Arrange
        let store = store().await;
        let other = Stamp::from_parts(0, 9);
        store
            .put(vec![
                record(1, 0.0),
                record(2, 0.0),
                FileRecord::failed(other),
            ])
            .await
            .expect("stored");

        // Act
        let removed = store
            .retain(&url("memory"), HashSet::from([stamp(1)]))
            .await
            .expect("retained");

        // Assert
        assert_eq!(removed, 1);
        assert_eq!(
            store
                .contains(&[stamp(1), stamp(2), other])
                .await
                .expect("read"),
            [true, false, true]
        );
        assert_eq!(
            rows(&store, STATS).await,
            1,
            "the stats rows of the removed stamp are gone"
        );
    }

    #[tokio::test]
    async fn a_column_name_keeps_its_id_across_puts() {
        let store = store().await;
        store.put(vec![record(1, 0.0)]).await.expect("stored");
        let mut second = record(2, 0.0);
        second.columns.push(("PSAL".to_string(), stat(1.0)));

        store.put(vec![second]).await.expect("stored");

        assert_eq!(rows(&store, COLUMNS).await, 2);
    }

    #[tokio::test]
    async fn a_write_syncs_only_when_the_last_sync_is_old() {
        // Arrange
        let store = store().await;
        let old = Instant::now()
            .checked_sub(SYNC_INTERVAL + Duration::from_secs(1))
            .expect("the clock runs longer than the interval");
        *store.last_sync.lock() = old;

        // Act
        store.put(vec![record(1, 0.0)]).await.expect("stored");
        let synced = *store.last_sync.lock();
        store.put(vec![record(2, 0.0)]).await.expect("stored");

        // Assert
        assert!(synced > old, "an old sync makes the write sync");
        assert_eq!(
            *store.last_sync.lock(),
            synced,
            "a write soon after does not sync"
        );
    }
}
